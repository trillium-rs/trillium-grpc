//! Bidirectional-streaming: the prologue + responder split that straddles the
//! run→upgrade seam.
//!
//! Bidi is the one shape that can't run entirely in `Handler::run`: read-while-
//! write needs the response head already flushed, which only happens once `run`
//! returns and `Handler::upgrade` is called. But the response's *initial*
//! metadata (and the content-type) must be committed in `run`, before the
//! flush — and that decision may depend on reading the first request message
//! (the conformance suite, for instance, carries the response-header definition
//! in request 1). So bidi is two functions joined by an **object**, never a
//! suspended future:
//!
//! 1. **The prologue** — the service trait's bidi method. It runs to completion
//!    in `run`, holding a [`GrpcServerConn`]: it may read request messages (via
//!    [`GrpcServerConn::requests`]) to decide response headers, sets that initial
//!    metadata straight through to the `Conn`, and returns a
//!    [`BidiResponder`]. Returning `Err(Status)` rejects before the flush
//!    (trailers-only, no upgrade).
//! 2. **The responder** — [`BidiResponder::respond`], a *fresh* future built and
//!    driven in `upgrade` over a [`Channel`]. It owns the read-while-write loop.
//!    Trailing metadata is written through the channel
//!    ([`Channel::response_trailers_mut`], seeded with whatever the prologue
//!    set) and emitted with `grpc-status` when it returns `Ok(())` or
//!    `Err(Status)`.
//!
//! What actually crosses the seam is a `BidiUpgrade` in the `Conn`'s state: a
//! type-erased `BidiDriver` (the boxed responder plus the codec fn-pointers,
//! encodings, and prologue-set trailers it needs). The user's loop future is
//! *not* suspended across the seam — it's created fresh in `upgrade` — so it
//! never has to be moved while holding borrows into the channel. The request
//! body's receive state is retained from `Conn` onto `Upgrade` by trillium-http,
//! so the responder's `Channel` continues reading at the next frame after
//! whatever the prologue consumed.
//!
//! [`GrpcServerConn`]: crate::server::GrpcServerConn
//! [`GrpcServerConn::requests`]: crate::server::GrpcServerConn::requests

use crate::{
    Encoding, Status,
    server::{dispatch::Cancellation, streaming::Channel},
};
use bytes::Bytes;
use std::{future::Future, pin::Pin, time::Instant};
use sync_wrapper::SyncWrapper;
use trillium::{Headers, Upgrade};

/// The user-driven half of a bidirectional-streaming RPC: the read-while-write
/// loop, created by the service method (the prologue) and run after the response
/// head is flushed.
///
/// Implement this on a type that carries whatever the prologue computed (any
/// requests it already read, the chosen response shape, …). [`respond`] is given
/// a [`Channel`] to interleave `recv` and `send`; trailing metadata (including
/// `grpc-status-details-bin` error details) is written through
/// [`Channel::response_trailers_mut`] and emitted with `grpc-status` from the
/// returned `Ok(())` / `Err(Status)`.
///
/// [`respond`]: BidiResponder::respond
pub trait BidiResponder<Req, Resp>: Send + 'static {
    /// Drive the bidirectional loop to completion. `Ok(())` ends the RPC with
    /// `grpc-status: 0`; `Err` ends it with that status.
    fn respond(
        self,
        channel: Channel<'_, Req, Resp>,
    ) -> impl Future<Output = Result<(), Status>> + Send;
}

/// Object-safe bridge over [`BidiResponder`]: boxes the (lifetime-carrying)
/// response future so the responder can be type-erased into a [`BidiDriver`].
trait ErasedResponder<Req, Resp>: Send {
    fn respond_boxed<'a>(
        self: Box<Self>,
        channel: Channel<'a, Req, Resp>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Status>> + Send + 'a>>
    where
        Req: 'a,
        Resp: 'a;
}

impl<Req, Resp, R> ErasedResponder<Req, Resp> for R
where
    R: BidiResponder<Req, Resp>,
{
    fn respond_boxed<'a>(
        self: Box<Self>,
        channel: Channel<'a, Req, Resp>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Status>> + Send + 'a>>
    where
        Req: 'a,
        Resp: 'a,
    {
        Box::pin(async move { (*self).respond(channel).await })
    }
}

/// A fully type-erased bidi loop, ready to drive over an [`Upgrade`]. Erasing
/// `Req`/`Resp` lets a single [`BidiUpgrade`] state key serve every bidi method
/// in a service.
trait BidiDriver: Send {
    fn drive(self: Box<Self>, upgrade: Upgrade) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

/// Everything the upgrade phase needs to run one bidi RPC: the boxed responder,
/// the trailing metadata the prologue accumulated, the codec fn-pointers, the
/// negotiated encodings, and the deadline carried from the request.
struct BidiState<Req, Resp> {
    responder: Box<dyn ErasedResponder<Req, Resp>>,
    base_trailers: Headers,
    decode: fn(&[u8]) -> Result<Req, Status>,
    encode: fn(&Resp) -> Result<Bytes, Status>,
    request_encoding: Encoding,
    response_encoding: Encoding,
    deadline: Option<Instant>,
}

impl<Req, Resp> BidiDriver for BidiState<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    fn drive(self: Box<Self>, mut upgrade: Upgrade) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            let BidiState {
                responder,
                mut base_trailers,
                decode,
                encode,
                request_encoding,
                response_encoding,
                deadline,
            } = *self;

            // The responder writes trailing metadata through the channel into
            // `base_trailers` (seeded by the prologue); we add `grpc-status`
            // after it returns.
            let cancellation = Cancellation::for_upgrade(&upgrade, deadline);
            let result = cancellation
                .race(async {
                    let channel = Channel::new(
                        &mut upgrade,
                        &mut base_trailers,
                        decode,
                        encode,
                        request_encoding,
                        response_encoding,
                    );
                    responder.respond_boxed(channel).await
                })
                .await;

            match result {
                Ok(()) => Status::ok().write_into(&mut base_trailers),
                Err(status) => status.write_into(&mut base_trailers),
            }

            if let Err(e) = upgrade.send_trailers(base_trailers).await {
                log::warn!("trillium-grpc: send_trailers failed: {e}");
            }
        })
    }
}

/// The bidi handoff stashed in the `Conn`'s state at the run→upgrade seam.
/// [`SyncWrapper`] satisfies the state set's `Sync` bound without requiring the
/// responder itself to be `Sync` (it never is concurrently shared).
pub(crate) struct BidiUpgrade(SyncWrapper<Box<dyn BidiDriver>>);

impl BidiUpgrade {
    /// Box the responder together with its codec/encoding context into a
    /// type-erased driver, ready to stash in `Conn` state. Called from the
    /// run-phase bidi dispatch once the prologue returns a responder.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new<Req, Resp, R>(
        responder: R,
        base_trailers: Headers,
        decode: fn(&[u8]) -> Result<Req, Status>,
        encode: fn(&Resp) -> Result<Bytes, Status>,
        request_encoding: Encoding,
        response_encoding: Encoding,
        deadline: Option<Instant>,
    ) -> Self
    where
        R: BidiResponder<Req, Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
    {
        let state = BidiState {
            responder: Box::new(responder),
            base_trailers,
            decode,
            encode,
            request_encoding,
            response_encoding,
            deadline,
        };
        Self(SyncWrapper::new(Box::new(state)))
    }
}

/// Whether this upgrade was marked for bidi by the run-phase dispatch. Generated
/// `Handler::has_upgrade` delegates here.
pub fn has_bidi_upgrade(upgrade: &Upgrade) -> bool {
    upgrade.state().get::<BidiUpgrade>().is_some()
}

/// Drive a bidi RPC to completion over its upgraded transport: build the
/// [`Channel`], run the responder loop, and write the terminating `grpc-status`
/// trailers. Generated `Handler::upgrade` delegates here. A no-op if the upgrade
/// wasn't ours (shouldn't happen given [`has_bidi_upgrade`] gates it).
pub async fn drive_bidi_upgrade(mut upgrade: Upgrade) {
    let Some(state) = upgrade.state_mut().take::<BidiUpgrade>() else {
        return;
    };
    state.0.into_inner().drive(upgrade).await;
}
