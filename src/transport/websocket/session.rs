use super::{
    Delivery, DeliveryAccess, Frame, Input, MAX_SEQ, Query, Signal, check, close, frame, stream,
};
use crate::{
    application::gateway::Check,
    domain::{access::Identity, replay::Generation},
    state::App,
};
use axum::extract::ws::Message;
use std::{future::pending, time::Duration};
use tokio::time::Instant;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WriterStatus {
    Idle,
    Busy,
}

pub(super) struct Session {
    pub(super) app: App,
    pub(super) company: Uuid,
    pub(super) identity: Identity,
    pub(super) generation: Generation,
    pub(super) delivery: Delivery,
    pub(super) query: Option<Query>,
    pub(super) access: Option<Check>,
    pub(super) next_check: Instant,
    pub(super) authorized: bool,
    pub(super) closing: bool,
    pub(super) uncertain_since: Option<Instant>,
    pub(super) retry_secs: u64,
    pub(super) last_heartbeat: Instant,
    pub(super) heartbeat_deadline: Option<Instant>,
    pub(super) dispatcher: Option<stream::Subscription>,
    pub(super) signal: Signal,
}

impl Session {
    pub(super) fn new(app: App, company: Uuid, identity: Identity, generation: Generation) -> Self {
        let access = Some(check(&app, company, &identity));
        Self {
            app,
            company,
            identity,
            generation,
            access,
            delivery: Delivery::default(),
            query: None,
            next_check: Instant::now() + Duration::from_secs(60),
            authorized: false,
            closing: false,
            uncertain_since: None,
            retry_secs: 1,
            last_heartbeat: Instant::now(),
            heartbeat_deadline: None,
            dispatcher: None,
            signal: Signal::Starting,
        }
    }

    pub(super) fn stop(&mut self, reason: Frame) {
        self.delivery.stop(reason);
        self.closing = true;
        self.query = None;
    }

    pub(super) fn delivery_access(&self) -> DeliveryAccess {
        if self.authorized && !self.closing {
            DeliveryAccess::Authorized
        } else {
            DeliveryAccess::Paused
        }
    }

    pub(super) fn refresh_access(&mut self) {
        self.authorized = false;
        self.access = Some(check(&self.app, self.company, &self.identity));
    }

    pub(super) fn write_completed(&mut self, item: &Frame) {
        if item.heartbeat {
            self.heartbeat_deadline = None;
        }
        let Some(sequence) = item.sequence else {
            return;
        };
        self.delivery.socket_written = Some(
            self.delivery
                .socket_written
                .map_or(sequence, |prev| prev.max(sequence)),
        );
        if item.generation != self.generation.id {
            crate::infrastructure::metrics::increment("ws_stale_generation_writes_total");
            tracing::debug!(company = %self.company, sequence, "obsolete replay write completed");
            return;
        }
        if !self.generation.record(sequence) {
            self.stop(close(1011));
        }
    }

    pub(super) async fn incoming(&mut self, message: Message) -> bool {
        match message {
            Message::Text(text) => self.handle_input(super::parse(&text)).await,
            Message::Close(_) => return false,
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Binary(_) => self.stop(close(1002)),
        }
        true
    }

    async fn handle_input(&mut self, input: Option<Input>) {
        match input {
            Some(Input::Heartbeat {
                client_confirmed_seq,
            }) => {
                if self.confirm_ack(client_confirmed_seq) {
                    self.heartbeat();
                }
            }
            Some(Input::Ack {
                client_confirmed_seq,
            }) => {
                self.confirm_ack(client_confirmed_seq);
            }
            Some(Input::ReplayRequest {
                replay_id,
                after_sequence,
            }) => {
                self.request_replay(replay_id, after_sequence).await;
            }
            _ => self.stop(close(1002)),
        }
    }

    fn confirm_ack(&mut self, sequence: i64) -> bool {
        if (0..=MAX_SEQ).contains(&sequence) && self.generation.accept_ack(sequence) {
            return true;
        }
        crate::infrastructure::metrics::increment("ws_invalid_ack_total");
        tracing::warn!(company = %self.company, "INVALID_CLIENT_ACK");
        self.stop(close(1002));
        false
    }

    fn heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
        self.heartbeat_deadline
            .get_or_insert(self.last_heartbeat + Duration::from_secs(5));
        if let Some(base) = self.generation.base {
            self.delivery.heartbeat = Some(frame(
                "HEARTBEAT_ACK",
                self.generation.id,
                serde_json::json!({"replay_base_seq": base,
                    "client_confirmed_seq": self.generation.confirmed,
                    "last_replay_written_seq": self.generation.written,
                    "last_socket_written_seq": self.delivery.socket_written}),
            ));
        }
    }
}

pub(super) async fn dispatcher_signal(dispatcher: &mut Option<stream::Subscription>) -> Signal {
    let Some(subscription) = dispatcher.as_mut() else {
        return pending().await;
    };
    if subscription.state.changed().await.is_err() {
        Signal::Unavailable
    } else {
        *subscription.state.borrow_and_update()
    }
}
