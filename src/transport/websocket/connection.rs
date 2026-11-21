use super::session::{Session, WriterStatus, dispatcher_signal};
use super::{
    Generation, Input, MAX_SEQ, Result, Sink, WRITE_TIMEOUT, Write, close, events, frame, parse,
    send, terminal,
};
use crate::state::App;
use axum::{
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::{IntoResponse, Response},
};
use futures_util::{StreamExt, future::BoxFuture};
use serde_json::json;
use std::{future::pending, time::Duration};
use tokio::time::{Instant, timeout};
use uuid::Uuid;

async fn poll<T>(future: &mut Option<BoxFuture<'static, T>>) -> T {
    match future {
        Some(f) => f.await,
        None => pending().await,
    }
}

pub(crate) async fn upgrade(
    State(app): State<App>,
    Path(id): Path<String>,
    ws: WebSocketUpgrade,
) -> Result<Response> {
    let company = events::company(&id)?;
    Ok(ws
        .max_message_size(32 * 1024)
        .max_frame_size(32 * 1024)
        .on_upgrade(move |socket| actor(app, company, socket))
        .into_response())
}

struct AuthenticatedSocket {
    sink: Sink,
    read: futures_util::stream::SplitStream<WebSocket>,
    identity: crate::domain::access::Identity,
    generation: Generation,
}

async fn authenticate(
    app: &App,
    company: Uuid,
    mut socket: WebSocket,
) -> Option<AuthenticatedSocket> {
    let first = timeout(Duration::from_secs(5), socket.next()).await;
    let Some(Input::Auth {
        access_token,
        replay_id,
        after_sequence,
    }) = (match first {
        Ok(Some(Ok(Message::Text(text)))) => parse(&text),
        _ => None,
    })
    else {
        let _ = timeout(WRITE_TIMEOUT, socket.send(close(1002).message)).await;
        return None;
    };
    let (sink, read) = socket.split();
    let who = match crate::infrastructure::tokens::decode(app, &access_token) {
        Ok(who) => who,
        Err(e) => {
            drop(send(sink, terminal(&e, company)).await);
            return None;
        }
    };
    if !Generation::valid(replay_id, after_sequence) {
        let reason = if !(1..=MAX_SEQ).contains(&replay_id) {
            "INVALID_REPLAY_ID"
        } else {
            "SEQUENCE_OUT_OF_RANGE"
        };
        drop(
            send(
                sink,
                frame(
                    "RESYNC_REQUIRED",
                    1,
                    json!({"reason":reason,"current_sequence":0}),
                ),
            )
            .await,
        );
        return None;
    }

    Some(AuthenticatedSocket {
        sink,
        read,
        identity: who,
        generation: Generation::new(replay_id, after_sequence),
    })
}

async fn actor(app: App, company: Uuid, socket: WebSocket) {
    let Some(authenticated) = authenticate(&app, company, socket).await else {
        return;
    };
    let mut guard = app.access_changes.subscribe();
    let mut read = authenticated.read;
    let mut session = Session::new(
        app,
        company,
        authenticated.identity,
        authenticated.generation,
    );
    let mut available_sink = Some(authenticated.sink);
    let mut write: Option<Write> = None;

    loop {
        session.schedule_replay(if write.is_none() {
            WriterStatus::Idle
        } else {
            WriterStatus::Busy
        });
        if let Some(sink) = available_sink.take() {
            if let Some(item) = session.delivery.take(session.delivery_access()) {
                write = Some(Box::pin(send(sink, item)));
            } else {
                available_sink = Some(sink);
            }
        }

        tokio::select! {
            biased;
            _ = guard.recv(), if !session.closing => {
                session.query = None;
                session.refresh_access();
            }
            _ = tokio::time::sleep_until(session.next_check),
                if !session.closing && session.access.is_none() => session.refresh_access(),
            answer = poll(&mut session.access), if !session.closing => {
                session.apply_access(answer).await;
            }
            signal = dispatcher_signal(&mut session.dispatcher), if !session.closing => {
                session.apply_signal(signal);
            }
            _ = tokio::time::sleep_until(session.uncertain_since.unwrap_or_else(Instant::now) + Duration::from_secs(30)),
                if !session.closing && session.uncertain_since.is_some() => {
                session.stop(close(1013));
                session.access = None;
            }
            _ = tokio::time::sleep_until(session.last_heartbeat + Duration::from_secs(65)),
                if !session.closing => session.stop(close(1008)),
            _ = tokio::time::sleep_until(session.heartbeat_deadline.unwrap_or_else(Instant::now)),
                if session.heartbeat_deadline.is_some() && !session.closing => {
                // A blocked sink cannot meet the control deadline: drop the connection.
                tracing::warn!(%company, "HEARTBEAT_ACK_DEADLINE_EXCEEDED");
                return;
            }
            (sink, item, success) = poll(&mut write) => {
                write = None;
                if !success || item.closes_with.is_some() {
                    return;
                }
                session.write_completed(&item);
                available_sink = Some(sink);
            }
            incoming = read.next(), if !session.closing => {
                let Some(Ok(incoming)) = incoming else {
                    return;
                };
                if !session.incoming(incoming).await {
                    return;
                }
            }
            answer = poll(&mut session.query), if session.authorized && !session.closing => {
                session.apply_query(answer);
            }
        }
    }
}
