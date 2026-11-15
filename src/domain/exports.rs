use serde::{Deserialize, Serialize};
#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum Preset {
    ExcelSemicolonBom,
    StandardCommaUtf8,
}

impl Preset {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ExcelSemicolonBom => "EXCEL_SEMICOLON_BOM",
            Self::StandardCommaUtf8 => "STANDARD_COMMA_UTF8",
        }
    }

    pub(crate) const fn delimiter(self) -> u8 {
        match self {
            Self::ExcelSemicolonBom => b';',
            Self::StandardCommaUtf8 => b',',
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GenerationError {
    Storage,
    Worker,
    Limit,
}

impl GenerationError {
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::Storage => "EXPORT_STORAGE_FAILED",
            Self::Worker => "EXPORT_WORKER_FAILED",
            Self::Limit => "EXPORT_LIMIT_EXCEEDED",
        }
    }
}

impl std::fmt::Display for GenerationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for GenerationError {}
