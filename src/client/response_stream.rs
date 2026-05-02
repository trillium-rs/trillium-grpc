use crate::{Codec, Encoding, Status, client::read_grpc_status, frame::reader::MessageStream};
use async_channel::{Receiver, Sender, bounded};
use futures_lite::{Stream, StreamExt};
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};
use trillium_server_common::Runtime;

/// Stream of decoded response messages from a server-streaming or bidi gRPC
/// call. Backed by a task spawned on the underlying client's runtime: the
/// task owns the `trillium_client::Conn`, reads framed messages from the
/// response body, and yields a final error item if the trailing
/// `grpc-status` is non-Ok.
///
/// Dropping the stream closes the channel, which the task observes on its
/// next send and bails out — no leaked work.
///
/// `Receiver` is not `Unpin`, so it lives in a `Pin<Box<_>>` to keep
/// `ResponseStream` itself `Unpin` for ergonomic `.next()` consumption.
pub struct ResponseStream<C, T> {
    rx: Pin<Box<Receiver<Result<T, Status>>>>,
    _marker: PhantomData<fn() -> C>,
}

impl<C, T> ResponseStream<C, T>
where
    C: Codec<T>,
    T: Send + 'static,
{
    pub(crate) fn spawn(
        client: &trillium_client::Client,
        conn: trillium_client::Conn,
        encoding: Encoding,
        deadline: Option<Instant>,
    ) -> Self {
        let (tx, rx) = bounded(8);
        let runtime = client.connector().runtime();
        let _detach = runtime
            .clone()
            .spawn(read_loop::<C, T>(conn, tx, encoding, runtime, deadline));
        Self { rx: Box::pin(rx), _marker: PhantomData }
    }

    /// Trailers-only stream: yields just the given error (or nothing if Ok)
    /// then ends. Used when the server returns HEADERS+END_STREAM with
    /// `grpc-status` already set, no body to read.
    pub(crate) fn trailers_only(result: Result<(), Status>) -> Self {
        let (tx, rx) = bounded(1);
        if let Err(status) = result {
            let _ = tx.try_send(Err(status));
        }
        Self { rx: Box::pin(rx), _marker: PhantomData }
    }
}

async fn read_loop<C, T>(
    mut conn: trillium_client::Conn,
    tx: Sender<Result<T, Status>>,
    encoding: Encoding,
    runtime: Runtime,
    deadline: Option<Instant>,
) where
    C: Codec<T>,
    T: Send + 'static,
{
    {
        let body = conn.response_body();
        let mut messages = MessageStream::<C, T, _>::new(body).with_encoding(encoding);
        loop {
            let next = match next_with_deadline(&runtime, deadline, messages.next()).await {
                Ok(opt) => opt,
                Err(status) => {
                    let _ = tx.send(Err(status)).await;
                    return;
                }
            };
            let Some(item) = next else { break };
            let stop = item.is_err();
            if tx.send(item).await.is_err() {
                return;
            }
            if stop {
                return;
            }
        }
    }
    if let Err(status) = read_grpc_status(&conn) {
        let _ = tx.send(Err(status)).await;
    }
}

/// Race a `Stream::next()` future (yielding `Option<Item>`) against the
/// deadline. `Ok(opt)` is the message-arrived path; `Err` is the deadline
/// firing first.
async fn next_with_deadline<F, T>(
    runtime: &Runtime,
    deadline: Option<Instant>,
    fut: F,
) -> Result<Option<T>, Status>
where
    F: std::future::Future<Output = Option<T>>,
{
    let Some(deadline) = deadline else {
        return Ok(fut.await);
    };
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        return Err(Status::deadline_exceeded("deadline elapsed"));
    };
    let runtime = runtime.clone();
    let timer = async move {
        runtime.delay(remaining).await;
        Err(Status::deadline_exceeded("deadline elapsed"))
    };
    futures_lite::future::or(async move { Ok(fut.await) }, timer).await
}

/// Race a `Result<T, Status>`-yielding future against a deadline. On
/// expiry returns `Err(DEADLINE_EXCEEDED)` directly. Used by the dispatch
/// trait methods for unary / streaming setup; [`read_loop`] does its
/// racing inline because its inner future yields `Option`.
pub(crate) async fn race_against_deadline<T, F>(
    runtime: &Runtime,
    deadline: Instant,
    fut: F,
) -> Result<T, Status>
where
    F: std::future::Future<Output = Result<T, Status>>,
{
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        return Err(Status::deadline_exceeded("deadline elapsed"));
    };
    let runtime = runtime.clone();
    let timer = async move {
        runtime.delay(remaining).await;
        Err(Status::deadline_exceeded("deadline elapsed"))
    };
    futures_lite::future::or(fut, timer).await
}

impl<C, T> Stream for ResponseStream<C, T>
where
    C: Codec<T>,
    T: Send + 'static,
{
    type Item = Result<T, Status>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().rx.as_mut().poll_next(cx)
    }
}
