//! Our implementation of `connectrpc.conformance.v1.ConformanceService`.
//!
//! The semantics mirror the Go gRPC reference server
//! (`internal/app/grpcserver/impl.go` in the conformance repo): every method
//! echoes back a [`ConformancePayload`] describing what the server observed —
//! the request headers, the deadline, and each request message wrapped in a
//! `google.protobuf.Any` — and otherwise does exactly what the request's
//! response-definition asks: emit the given data, set the given response
//! headers/trailers, wait the given delay, or raise the given error.
//!
//! Errors carry a `grpc-status-details-bin` trailer (a serialized
//! `google.rpc.Status`) so the conformance client can reconstruct the error
//! details, including the [`RequestInfo`] the reference server appends.
//!
//! Bidi uses the prologue + [`BidiResponder`] shape: the prologue reads the
//! first request to learn the response definition (so it can set response
//! headers/trailers before the head flushes) and echo the request headers /
//! deadline, then the responder drives the read-while-write loop — mirroring the
//! Go reference's `BidiStream`.

use crate::pb::{
    self, BidiStreamResponse, ClientStreamResponse, ConformancePayload, IdempotentUnaryResponse,
    ServerStreamResponse, UnaryResponse, UnimplementedRequest, UnimplementedResponse,
    conformance_payload::RequestInfo, unary_response_definition::Response,
};
// gRPC `-bin` header values use base64 *without* padding.
use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD as BASE64};
use prost::Message;
use prost_types::Any;
use std::{collections::VecDeque, time::Duration, time::Instant};
use tokio::time::sleep;
use trillium::Headers;
use trillium_grpc::{BidiResponder, Channel, Code, GrpcServerConn, Status, Stream};

/// The unit type implementing the conformance service.
pub struct ConformanceServiceImpl;

const TYPE_PREFIX: &str = "type.googleapis.com/connectrpc.conformance.v1.";

/// A minimal `google.rpc.Status`, serialized into `grpc-status-details-bin`.
#[derive(Clone, PartialEq, ::prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<Any>,
}

/// Wrap a conformance message in an `Any` with its canonical type URL.
fn to_any(short_name: &str, msg: &impl Message) -> Any {
    Any {
        type_url: format!("{TYPE_PREFIX}{short_name}"),
        value: msg.encode_to_vec(),
    }
}

/// Append proto `Header`s onto a trillium `Headers` bag (one entry per value).
fn apply_headers(dest: &mut Headers, headers: &[pb::Header]) {
    for h in headers {
        for v in &h.value {
            dest.append(h.name.clone(), v.clone());
        }
    }
}

/// Echo the observed request headers back as proto `Header`s. `checkHeaders`
/// in the runner only asserts that expected headers are *present*, so echoing
/// transport headers (content-type, grpc-*, …) as extras is harmless.
fn echo_headers(headers: &Headers) -> Vec<pb::Header> {
    headers
        .iter()
        .filter_map(|(name, values)| {
            let value: Vec<String> = values
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            (!value.is_empty()).then(|| pb::Header {
                name: name.to_string(),
                value,
            })
        })
        .collect()
}

/// Remaining time until the request deadline, in milliseconds — matching the
/// reference server's `time.Until(deadline)`.
fn timeout_ms(deadline: Option<Instant>) -> Option<i64> {
    deadline.map(|d| {
        let now = Instant::now();
        if d > now {
            (d - now).as_millis() as i64
        } else {
            0
        }
    })
}

/// Build the `RequestInfo` echoed in a conformance payload.
fn request_info(
    headers: Vec<pb::Header>,
    timeout_ms: Option<i64>,
    requests: Vec<Any>,
) -> RequestInfo {
    RequestInfo {
        request_headers: headers,
        timeout_ms,
        requests,
        connect_get_info: None,
    }
}

/// Convert a conformance `Error` into a [`Status`], optionally appending a
/// `RequestInfo` to its details, and (when `trailers` is provided) writing the
/// full `google.rpc.Status` into a `grpc-status-details-bin` trailer.
fn status_from_error(
    err: &pb::Error,
    request_info_any: Option<Any>,
    trailers: Option<&mut Headers>,
) -> Status {
    let code = Code::from_u8(err.code as u8).unwrap_or(Code::Unknown);
    let message = err.message.clone().unwrap_or_default();

    let mut details = err.details.clone();
    if let Some(any) = request_info_any {
        details.push(any);
    }

    if let Some(trailers) = trailers {
        let rpc_status = RpcStatus {
            code: err.code,
            message: message.clone(),
            details,
        };
        trailers.insert(
            "grpc-status-details-bin",
            BASE64.encode(rpc_status.encode_to_vec()),
        );
    }

    Status::new(code, message)
}

/// Shared unary / client-streaming response logic: set headers + trailers,
/// honor the response delay, then either echo a payload or raise the error.
async fn respond_single(
    grpc: &mut GrpcServerConn,
    def: Option<pb::UnaryResponseDefinition>,
    requests: Vec<Any>,
) -> Result<ConformancePayload, Status> {
    let info = request_info(
        echo_headers(grpc.received_headers()),
        timeout_ms(grpc.deadline()),
        requests,
    );

    let Some(def) = def else {
        // No response definition: echo only the request info.
        return Ok(ConformancePayload {
            data: Vec::new(),
            request_info: Some(info),
        });
    };

    apply_headers(grpc.response_headers_mut(), &def.response_headers);
    apply_headers(grpc.response_trailers_mut(), &def.response_trailers);

    if def.response_delay_ms > 0 {
        sleep(Duration::from_millis(def.response_delay_ms as u64)).await;
    }

    match def.response {
        Some(Response::Error(err)) => {
            let info_any = to_any("ConformancePayload.RequestInfo", &info);
            Err(status_from_error(
                &err,
                Some(info_any),
                Some(grpc.response_trailers_mut()),
            ))
        }
        Some(Response::ResponseData(data)) => Ok(ConformancePayload {
            data,
            request_info: Some(info),
        }),
        None => Ok(ConformancePayload {
            data: Vec::new(),
            request_info: Some(info),
        }),
    }
}

/// Build the lazily-pulled server-streaming response: emit each prepared
/// response (sleeping `delay` before each), then a terminal error if any.
fn response_stream(
    responses: VecDeque<ServerStreamResponse>,
    terminal: Option<Status>,
    delay: Duration,
) -> impl Stream<Item = Result<ServerStreamResponse, Status>> + Send + use<> {
    futures_lite::stream::unfold(
        (responses, terminal, delay),
        |(mut responses, mut terminal, delay)| async move {
            if let Some(resp) = responses.pop_front() {
                if !delay.is_zero() {
                    sleep(delay).await;
                }
                Some((Ok(resp), (responses, terminal, delay)))
            } else {
                terminal
                    .take()
                    .map(|status| (Err(status), (responses, terminal, delay)))
            }
        },
    )
}

/// The bidi loop, carrying what the prologue read and decided: the echoed
/// request headers / timeout, the response definition, the duplex mode, and the
/// first request (already consumed in the run phase to learn the definition).
struct BidiResponderImpl {
    request_headers: Vec<pb::Header>,
    timeout_ms: Option<i64>,
    def: Option<pb::StreamResponseDefinition>,
    full_duplex: bool,
    first: Option<pb::BidiStreamRequest>,
}

impl BidiResponder<pb::BidiStreamRequest, BidiStreamResponse> for BidiResponderImpl {
    async fn respond(
        self,
        mut channel: Channel<'_, pb::BidiStreamRequest, BidiStreamResponse>,
    ) -> Result<(), Status> {
        let BidiResponderImpl {
            request_headers,
            timeout_ms,
            def,
            full_duplex,
            mut first,
        } = self;
        let mut requests: Vec<Any> = Vec::new();
        let mut resp_num = 0usize;

        // Drive the receive loop, replaying the prologue-consumed first request
        // before reading the rest off the channel.
        loop {
            let req = match first.take() {
                Some(req) => Ok(req),
                None => match channel.recv().await {
                    None => break,
                    Some(result) => result,
                },
            };
            requests.push(to_any("BidiStreamRequest", &req?));

            if full_duplex {
                let Some(def) = def.as_ref() else { break };
                if resp_num >= def.response_data.len() {
                    break;
                }
                // The first full-duplex response carries the full request info;
                // later ones echo only the requests seen since the previous one.
                let info = if resp_num == 0 {
                    request_info(
                        request_headers.clone(),
                        timeout_ms,
                        std::mem::take(&mut requests),
                    )
                } else {
                    request_info(Vec::new(), None, std::mem::take(&mut requests))
                };
                if def.response_delay_ms > 0 {
                    sleep(Duration::from_millis(def.response_delay_ms as u64)).await;
                }
                channel
                    .send(BidiStreamResponse {
                        payload: Some(ConformancePayload {
                            data: def.response_data[resp_num].clone(),
                            request_info: Some(info),
                        }),
                    })
                    .await?;
                resp_num += 1;
            }
        }

        // Flush any responses still owed: the whole set for half-duplex, or the
        // surplus for full-duplex where responses outnumbered requests.
        if let Some(def) = def {
            while resp_num < def.response_data.len() {
                let info = (resp_num == 0).then(|| {
                    request_info(
                        request_headers.clone(),
                        timeout_ms,
                        std::mem::take(&mut requests),
                    )
                });
                if def.response_delay_ms > 0 {
                    sleep(Duration::from_millis(def.response_delay_ms as u64)).await;
                }
                channel
                    .send(BidiStreamResponse {
                        payload: Some(ConformancePayload {
                            data: def.response_data[resp_num].clone(),
                            request_info: info,
                        }),
                    })
                    .await?;
                resp_num += 1;
            }

            if let Some(err) = def.error {
                // When no responses preceded the error, append the request info
                // to the error details (matching the reference server).
                let info_any = (resp_num == 0).then(|| {
                    to_any(
                        "ConformancePayload.RequestInfo",
                        &request_info(request_headers, timeout_ms, requests),
                    )
                });
                return Err(status_from_error(
                    &err,
                    info_any,
                    Some(channel.response_trailers_mut()),
                ));
            }
        }

        Ok(())
    }
}

impl pb::ConformanceService for ConformanceServiceImpl {
    async fn unary(
        &self,
        grpc: &mut GrpcServerConn,
        request: pb::UnaryRequest,
    ) -> Result<UnaryResponse, Status> {
        let any = to_any("UnaryRequest", &request);
        let payload = respond_single(grpc, request.response_definition, vec![any]).await?;
        Ok(UnaryResponse {
            payload: Some(payload),
        })
    }

    async fn idempotent_unary(
        &self,
        grpc: &mut GrpcServerConn,
        request: pb::IdempotentUnaryRequest,
    ) -> Result<IdempotentUnaryResponse, Status> {
        let any = to_any("IdempotentUnaryRequest", &request);
        let payload = respond_single(grpc, request.response_definition, vec![any]).await?;
        Ok(IdempotentUnaryResponse {
            payload: Some(payload),
        })
    }

    async fn client_stream(
        &self,
        grpc: &mut GrpcServerConn,
    ) -> Result<ClientStreamResponse, Status> {
        let mut def = None;
        let mut requests = Vec::new();
        let mut first = true;
        let mut stream = grpc.requests::<pb::ClientStreamRequest>();
        while let Some(msg) = stream.recv().await? {
            if first {
                def = msg.response_definition.clone();
                first = false;
            }
            requests.push(to_any("ClientStreamRequest", &msg));
        }
        drop(stream);

        let payload = respond_single(grpc, def, requests).await?;
        Ok(ClientStreamResponse {
            payload: Some(payload),
        })
    }

    async fn server_stream(
        &self,
        grpc: &mut GrpcServerConn,
        request: pb::ServerStreamRequest,
    ) -> Result<impl Stream<Item = Result<ServerStreamResponse, Status>> + Send + use<>, Status>
    {
        let any = to_any("ServerStreamRequest", &request);
        let info = request_info(
            echo_headers(grpc.received_headers()),
            timeout_ms(grpc.deadline()),
            vec![any],
        );

        let Some(def) = request.response_definition else {
            return Ok(response_stream(VecDeque::new(), None, Duration::ZERO));
        };

        apply_headers(grpc.response_headers_mut(), &def.response_headers);
        apply_headers(grpc.response_trailers_mut(), &def.response_trailers);

        let mut responses = VecDeque::new();
        for (i, data) in def.response_data.into_iter().enumerate() {
            responses.push_back(ServerStreamResponse {
                payload: Some(ConformancePayload {
                    data,
                    // Only the first response carries request info; nothing in
                    // it changes across a server stream.
                    request_info: (i == 0).then(|| info.clone()),
                }),
            });
        }

        let terminal = def.error.map(|err| {
            // When no responses precede the error, the reference server appends
            // the RequestInfo to the error details.
            let info_any = responses
                .is_empty()
                .then(|| to_any("ConformancePayload.RequestInfo", &info));
            status_from_error(&err, info_any, Some(grpc.response_trailers_mut()))
        });

        let delay = Duration::from_millis(def.response_delay_ms as u64);
        Ok(response_stream(responses, terminal, delay))
    }

    async fn bidi_stream(
        &self,
        grpc: &mut GrpcServerConn,
    ) -> Result<impl BidiResponder<pb::BidiStreamRequest, BidiStreamResponse> + use<>, Status> {
        // The first request carries the response definition; read it here (in
        // the run phase) so response headers/trailers can be committed before
        // the head flushes. Capture the request headers / deadline now, too —
        // they're echoed into the first response's request info.
        let request_headers = echo_headers(grpc.received_headers());
        let timeout_ms = timeout_ms(grpc.deadline());

        let first = grpc.requests::<pb::BidiStreamRequest>().recv().await?;
        let (def, full_duplex) = match &first {
            Some(req) => (req.response_definition.clone(), req.full_duplex),
            None => (None, false),
        };

        if let Some(def) = def.as_ref() {
            apply_headers(grpc.response_headers_mut(), &def.response_headers);
            apply_headers(grpc.response_trailers_mut(), &def.response_trailers);
        }

        Ok(BidiResponderImpl {
            request_headers,
            timeout_ms,
            def,
            full_duplex,
            first,
        })
    }

    async fn unimplemented(
        &self,
        _grpc: &mut GrpcServerConn,
        _request: UnimplementedRequest,
    ) -> Result<UnimplementedResponse, Status> {
        Err(Status::unimplemented(
            "connectrpc.conformance.v1.ConformanceService.Unimplemented is not implemented",
        ))
    }
}
