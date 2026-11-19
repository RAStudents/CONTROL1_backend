//! Token-scoped export files: bounded CSV buffering, durable writes and read-back verification.

use crate::domain::{
    csv::CsvLayout, exports::GenerationError, exports::Preset, limits::MAX_EXPORT_BYTES,
};
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug)]
pub(crate) struct FileData {
    pub(crate) size: i64,
    pub(crate) digest: String,
    pub(crate) records: i64,
}

pub(crate) struct ExportFile<'path> {
    path: &'path Path,
    file: tokio::fs::File,
    // The encoder leaves this slot while its drained chunk is written to disk.
    csv: Option<csv::Writer<Vec<u8>>>,
    preset: Preset,
    digest: Sha256,
    size: i64,
    records: i64,
}

impl<'path> ExportFile<'path> {
    pub(crate) async fn create(
        path: &'path Path,
        layout: &CsvLayout,
    ) -> Result<Self, GenerationError> {
        tokio::fs::create_dir_all(path.parent().ok_or(GenerationError::Storage)?)
            .await
            .map_err(|_| GenerationError::Storage)?;
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .await
            .map_err(|_| GenerationError::Storage)?;
        let csv = csv_encoder(layout.preset, Vec::new());
        let mut output = Self {
            path,
            file,
            csv: Some(csv),
            preset: layout.preset,
            digest: Sha256::new(),
            size: 0,
            records: 0,
        };

        if matches!(layout.preset, Preset::ExcelSemicolonBom) {
            output.write_bytes(&[0xef, 0xbb, 0xbf]).await?;
        }
        output
            .csv
            .as_mut()
            .ok_or(GenerationError::Worker)?
            .write_record(layout.columns())
            .map_err(|_| GenerationError::Worker)?;

        Ok(output)
    }

    pub(crate) fn records(&self) -> i64 {
        self.records
    }

    pub(crate) fn write_record(&mut self, cells: &[String]) -> Result<(), GenerationError> {
        self.csv
            .as_mut()
            .ok_or(GenerationError::Worker)?
            .write_record(cells)
            .map_err(|_| GenerationError::Worker)?;
        self.records += 1;
        Ok(())
    }

    pub(crate) async fn flush(&mut self) -> Result<(), GenerationError> {
        let bytes = self.drain_csv().await?;
        self.csv = Some(csv_encoder(self.preset, bytes));
        Ok(())
    }

    async fn drain_csv(&mut self) -> Result<Vec<u8>, GenerationError> {
        let mut csv = self.csv.take().ok_or(GenerationError::Worker)?;
        csv.flush().map_err(|_| GenerationError::Storage)?;
        let mut bytes = csv.into_inner().map_err(|_| GenerationError::Storage)?;
        self.write_bytes(&bytes).await?;
        bytes.clear();
        Ok(bytes)
    }

    async fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), GenerationError> {
        self.size += bytes.len() as i64;
        if self.size > MAX_EXPORT_BYTES {
            return Err(GenerationError::Limit);
        }

        self.digest.update(bytes);
        self.file
            .write_all(bytes)
            .await
            .map_err(|_| GenerationError::Storage)
    }

    pub(crate) async fn finish(mut self) -> Result<FileData, GenerationError> {
        drop(self.drain_csv().await?);
        self.file
            .flush()
            .await
            .map_err(|_| GenerationError::Storage)?;
        self.file
            .sync_all()
            .await
            .map_err(|_| GenerationError::Storage)?;
        let stored_size = self
            .file
            .metadata()
            .await
            .map_err(|_| GenerationError::Storage)?
            .len();
        if stored_size != self.size as u64 {
            return Err(GenerationError::Storage);
        }
        drop(self.file);

        let expected = self.digest.finalize();
        verify_digest(self.path, &expected).await?;
        Ok(FileData {
            size: self.size,
            digest: hex::encode(expected),
            records: self.records,
        })
    }
}

fn csv_encoder(preset: Preset, bytes: Vec<u8>) -> csv::Writer<Vec<u8>> {
    csv::WriterBuilder::new()
        .delimiter(preset.delimiter())
        .terminator(csv::Terminator::CRLF)
        .from_writer(bytes)
}

async fn verify_digest(path: &Path, expected: &[u8]) -> Result<(), GenerationError> {
    let mut stored = tokio::fs::File::open(path)
        .await
        .map_err(|_| GenerationError::Storage)?;
    let mut digest = Sha256::new();
    let mut block = vec![0; 64 * 1024];
    loop {
        let count = stored
            .read(&mut block)
            .await
            .map_err(|_| GenerationError::Storage)?;
        if count == 0 {
            break;
        }
        digest.update(&block[..count]);
    }

    if &digest.finalize()[..] != expected {
        return Err(GenerationError::Storage);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    fn output_path() -> PathBuf {
        std::env::temp_dir().join(format!("control-export-{}.csv", uuid::Uuid::new_v4()))
    }

    async fn assert_chunked_csv(preset: Preset) {
        let path = output_path();
        let layout = CsvLayout {
            preset,
            timezone: chrono_tz::UTC,
            include_media: true,
        };
        let mut output = ExportFile::create(&path, &layout).await.unwrap();
        let event = json!({"id": " =1", "occurred_at": "2025-01-01T00:00:00Z",
                          "event_type_name": "\"quoted\", comma; and\r\nnewline"});
        let cells = layout.record(&event).unwrap();
        for _ in 0..256 {
            output.write_record(&cells).unwrap();
        }
        output.flush().await.unwrap();
        for _ in 0..256 {
            output.write_record(&cells).unwrap();
        }
        output.flush().await.unwrap();
        output.write_record(&cells).unwrap();
        let metadata = output.finish().await.unwrap();
        let bytes = tokio::fs::read(&path).await.unwrap();
        assert_eq!(metadata.records, 513);
        assert_eq!(metadata.size, bytes.len() as i64);
        assert_eq!(metadata.digest, hex::encode(Sha256::digest(&bytes)));
        assert_eq!(
            bytes.starts_with(&[0xef, 0xbb, 0xbf]),
            matches!(preset, Preset::ExcelSemicolonBom)
        );
        assert!(bytes.ends_with(b"\r\n"));
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(preset.delimiter())
            .from_reader(bytes.as_slice());
        assert_eq!(
            reader.headers().unwrap().iter().collect::<Vec<_>>(),
            layout.columns()
        );
        let records = reader.records().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 513);
        for record in records {
            assert_eq!(record.iter().collect::<Vec<_>>(), cells);
        }
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn excel_chunks_preserve_bom_quoting_crlf_and_verified_metadata() {
        assert_chunked_csv(Preset::ExcelSemicolonBom).await;
    }

    #[tokio::test]
    async fn standard_chunks_preserve_quoting_crlf_and_verified_metadata() {
        assert_chunked_csv(Preset::StandardCommaUtf8).await;
    }

    #[tokio::test]
    async fn empty_export_still_contains_one_header_and_correct_size() {
        let path = output_path();
        let layout = CsvLayout {
            preset: Preset::StandardCommaUtf8,
            timezone: chrono_tz::UTC,
            include_media: false,
        };
        let metadata = ExportFile::create(&path, &layout)
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        let bytes = tokio::fs::read(&path).await.unwrap();
        assert_eq!(metadata.records, 0);
        assert_eq!(metadata.size, bytes.len() as i64);
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            layout.columns().join(",") + "\r\n"
        );
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn existing_token_file_is_never_overwritten() {
        let path = output_path();
        tokio::fs::write(&path, b"existing export").await.unwrap();
        let layout = CsvLayout {
            preset: Preset::StandardCommaUtf8,
            timezone: chrono_tz::UTC,
            include_media: false,
        };
        assert!(matches!(
            ExportFile::create(&path, &layout).await,
            Err(GenerationError::Storage)
        ));
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"existing export");
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn verification_rejects_corruption_even_when_file_size_matches() {
        let path = output_path();
        tokio::fs::write(&path, b"corrupt").await.unwrap();
        assert_eq!(
            verify_digest(&path, &Sha256::digest(b"correct")).await,
            Err(GenerationError::Storage)
        );
        tokio::fs::remove_file(path).await.unwrap();
    }
}
