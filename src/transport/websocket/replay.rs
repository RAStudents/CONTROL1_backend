use super::session::{Session, WriterStatus};
use super::{Frame, Signal, bounds, close, frame, next_sequence, prepare_frames, rows, stream};
use crate::{
    application::gateway::DbAnswer, domain::replay::Generation, infrastructure::support::Error,
};

impl Session {
    pub(super) async fn attach_dispatcher(&mut self) {
        let subscription = stream::attach(&self.app, self.company).await;
        let signal = *subscription.state.borrow();
        self.dispatcher = Some(subscription);
        self.apply_signal(signal);
    }

    pub(super) fn apply_signal(&mut self, signal: Signal) {
        self.signal = signal;
        match signal {
            Signal::Unavailable => self.stop(close(1013)),
            Signal::Resync(reason, head) => {
                self.delivery.resync(&mut self.generation, reason, head);
                self.query = None;
            }
            _ => {}
        }
    }

    pub(super) fn schedule_replay(&mut self, writer: WriterStatus) {
        if !self.authorized
            || self.closing
            || self.generation.blocked
            || self.query.is_some()
            || !matches!(self.signal, Signal::Head(_))
        {
            return;
        }
        if self.generation.base.is_none() && writer == WriterStatus::Idle {
            self.query = Some(bounds(&self.app, self.company, self.generation.id));
            return;
        }
        if let Some(target) = self.generation.target
            && self.delivery.fifo.is_empty()
        {
            self.schedule_chunk(target);
        }
    }

    fn schedule_chunk(&mut self, target: i64) {
        let end = match (self.generation.replay_complete, self.signal) {
            (true, Signal::Head(head)) => head,
            _ => target,
        };
        if self.generation.enqueued < end {
            let Some(start) = next_sequence(self.generation.enqueued) else {
                self.stop(close(1011));
                return;
            };
            let live = self
                .generation
                .replay_complete
                .then(|| self.dispatcher.clone())
                .flatten();
            self.query = Some(rows(
                &self.app,
                self.company,
                self.generation.id,
                start..=end,
                live,
            ));
        } else if !self.generation.replay_complete {
            self.generation.replay_complete = true;
            let _ = self.delivery.enqueue(frame(
                "REPLAY_COMPLETED",
                self.generation.id,
                serde_json::json!({"through_sequence": target}),
            ));
        }
    }

    pub(super) async fn request_replay(&mut self, id: i64, after: i64) {
        if !Generation::valid(id, after)
            || (id == self.generation.id && self.generation.requested != after)
        {
            let reason = if id == self.generation.id {
                "CONFLICTING_REPLAY_ID"
            } else {
                "INVALID_REPLAY_ID"
            };
            let head = match self.signal {
                Signal::Head(h) => h,
                _ => self.generation.confirmed,
            };
            self.delivery.resync(&mut self.generation, reason, head);
            self.query = None;
            return;
        }
        if id <= self.generation.id {
            return;
        }
        self.delivery.clear();
        self.query = None;
        self.generation = Generation::new(id, after);
        if let Signal::Resync(reason, head) = self.signal {
            if reason == "STREAM_INTEGRITY_VIOLATION" {
                self.delivery.resync(&mut self.generation, reason, head);
            } else {
                self.attach_dispatcher().await;
            }
        }
    }

    pub(super) fn apply_query(&mut self, answer: DbAnswer) {
        self.query = None;
        match answer {
            DbAnswer::Bounds(id, result) if id == self.generation.id => self.apply_bounds(&result),
            DbAnswer::Rows(id, start, end, result) if id == self.generation.id => {
                let frames = result
                    .map_err(|_| "REPLAY_DB_ERROR")
                    .and_then(|rows| prepare_frames(&self.app, id, start, end, rows));
                self.apply_frames(frames, end);
            }
            _ => {}
        }
    }

    fn apply_bounds(&mut self, result: &Result<(i64, Option<i64>), Error>) {
        let (head, minimum) = match result {
            Ok(bounds) => *bounds,
            Err(_) => {
                let head = self.generation.confirmed;
                self.delivery
                    .resync(&mut self.generation, "REPLAY_DB_ERROR", head);
                return;
            }
        };
        let after = self.generation.requested;
        let from = (after < head).then(|| next_sequence(after)).flatten();
        if after > head {
            self.delivery
                .resync(&mut self.generation, "INVALID_FUTURE_SEQUENCE", head);
        } else if after < head && minimum.zip(from).is_none_or(|(min, start)| start < min) {
            self.delivery
                .resync(&mut self.generation, "OUTBOX_RETENTION_EXCEEDED", head);
        } else {
            self.generation.base = Some(after);
            self.generation.confirmed = after;
            self.generation.target = Some(head);
            let _ = self.delivery.enqueue(frame("SUBSCRIBED", self.generation.id,
                serde_json::json!({"replay_from_sequence": from, "replay_until_sequence": head, "replay_empty": after == head})));
        }
    }

    fn apply_frames(&mut self, frames: Result<Vec<Frame>, &'static str>, head: i64) {
        let frames = match frames {
            Ok(frames) => frames,
            Err(reason) => {
                tracing::warn!(company = %self.company, reason, "websocket replay interrupted");
                self.delivery.resync(&mut self.generation, reason, head);
                return;
            }
        };
        for item in frames {
            let Some(sequence) = item.sequence else {
                self.stop(close(1011));
                break;
            };
            self.generation.enqueued = sequence;
            if !self.delivery.enqueue(item) {
                self.delivery
                    .resync(&mut self.generation, "SLOW_CONSUMER", head);
                break;
            }
        }
    }
}
