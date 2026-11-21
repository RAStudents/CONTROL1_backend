use super::session::Session;
use super::{close, frame, terminal};
use crate::infrastructure::support::Error;
use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

impl Session {
    pub(super) async fn apply_access(&mut self, answer: Result<Option<DateTime<Utc>>, Error>) {
        self.access = None;
        match answer {
            Ok(grant) => self.allow_access(grant).await,
            Err(error) if matches!(error.status.as_u16(), 401 | 403) => self.deny_access(&error),
            Err(_) => self.pause_access(),
        }
    }

    async fn allow_access(&mut self, grant: Option<DateTime<Utc>>) {
        if self.uncertain_since.is_some() {
            // A reconnect establishes a fresh generation after authorization uncertainty.
            self.stop(close(1013));
            return;
        }
        self.authorized = true;
        self.retry_secs = 1;
        let now = Utc::now();
        let token_seconds = (self.identity.exp as i64 - now.timestamp()).max(0) as u64;
        let until_grant = grant
            .map(|g| (g - now).to_std().unwrap_or_default())
            .unwrap_or(Duration::from_secs(60));
        self.next_check =
            Instant::now() + Duration::from_secs(token_seconds.min(60)).min(until_grant);
        if self.dispatcher.is_none() {
            self.attach_dispatcher().await;
        }
    }

    fn deny_access(&mut self, error: &Error) {
        if error.status.as_u16() == 403 {
            crate::application::gateway::audit_denial(
                &self.app,
                self.company,
                self.identity.sub,
                error.code,
            );
        }
        self.stop(terminal(error, self.company));
    }

    fn pause_access(&mut self) {
        self.authorized = false;
        self.query = None;
        self.delivery.clear();
        let now = Instant::now();
        let since = *self.uncertain_since.get_or_insert(now);
        let elapsed = now.duration_since(since);
        crate::infrastructure::metrics::set(
            "ws_authorization_unavailable_seconds",
            Some(self.company),
            elapsed.as_secs_f64(),
        );
        if elapsed >= Duration::from_secs(30) {
            self.stop(close(1013));
        } else {
            let jitter = u64::from(Uuid::new_v4().as_bytes()[0]);
            let delay = Duration::from_millis(self.retry_secs * 1000 + jitter);
            self.next_check = (now + delay).min(since + Duration::from_secs(30));
            self.delivery.unavailable = Some(frame(
                "ACCESS_CHECK_UNAVAILABLE",
                self.generation.id,
                serde_json::json!({"retry_after_ms": delay.as_millis()}),
            ));
            self.retry_secs = (self.retry_secs * 2).min(8);
        }
        tracing::warn!(company = %self.company, elapsed_ms = elapsed.as_millis(), "websocket authorization unavailable");
    }
}
