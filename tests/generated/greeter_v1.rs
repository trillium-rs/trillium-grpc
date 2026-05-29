use std::sync::Arc;
use trillium::{Conn, Handler, Method, Upgrade};
use trillium_grpc::{
    BidiConn, BidiResponder, GrpcServerConn, Prost, Server, ServiceClient, Status,
    Stream, StreamingConn, UnaryConn, prepare_grpc_conn,
};
#[derive(Clone, PartialEq, Eq, Hash, ::trillium_grpc::prost::Message)]
#[prost(prost_path = "::trillium_grpc::prost")]
pub struct HelloRequest {
    #[prost(string, tag = "1")]
    pub name: ::trillium_grpc::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, Eq, Hash, ::trillium_grpc::prost::Message)]
#[prost(prost_path = "::trillium_grpc::prost")]
pub struct HelloReply {
    #[prost(string, tag = "1")]
    pub message: ::trillium_grpc::prost::alloc::string::String,
}
pub trait Greeter: Send + Sync + 'static {
    fn say_hello(
        &self,
        conn: &mut GrpcServerConn,
        request: HelloRequest,
    ) -> impl Future<Output = Result<HelloReply, Status>> + Send;
    fn say_hello_stream(
        &self,
        conn: &mut GrpcServerConn,
        request: HelloRequest,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<HelloReply, Status>> + Send + use<Self>,
            Status,
        >,
    > + Send;
    fn say_hello_many(
        &self,
        conn: &mut GrpcServerConn,
    ) -> impl Future<Output = Result<HelloReply, Status>> + Send;
    fn say_hello_chat(
        &self,
        conn: &mut GrpcServerConn,
    ) -> impl Future<
        Output = Result<impl BidiResponder<HelloRequest, HelloReply> + use<Self>, Status>,
    > + Send;
}
pub struct GreeterServer<T>(Arc<T>);
impl<T> GreeterServer<T> {
    pub fn new(inner: T) -> Self {
        Self(Arc::new(inner))
    }
}
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy)]
enum GreeterDispatch {
    SayHello,
    SayHelloStream,
    SayHelloMany,
    SayHelloChat,
}
impl<T: Greeter> Handler for GreeterServer<T> {
    async fn run(&self, conn: Conn) -> Conn {
        const PREFIX: &str = "/greeter.v1.Greeter";
        let Some(method) = conn.path().strip_prefix(PREFIX) else {
            return conn;
        };
        if conn.method() != Method::Post {
            return conn;
        }
        let dispatch = match method {
            "/SayHello" => GreeterDispatch::SayHello,
            "/SayHelloStream" => GreeterDispatch::SayHelloStream,
            "/SayHelloMany" => GreeterDispatch::SayHelloMany,
            "/SayHelloChat" => GreeterDispatch::SayHelloChat,
            _ => return conn,
        };
        let conn = match prepare_grpc_conn(conn, "proto") {
            Ok(c) => c,
            Err(c) => return c,
        };
        let inner = Arc::clone(&self.0);
        match dispatch {
            GreeterDispatch::SayHello => {
                Prost::unary(
                        conn,
                        async move |grpc, req| inner.say_hello(grpc, req).await,
                    )
                    .await
            }
            GreeterDispatch::SayHelloStream => {
                Prost::server_streaming(
                        conn,
                        async move |grpc, req| inner.say_hello_stream(grpc, req).await,
                    )
                    .await
            }
            GreeterDispatch::SayHelloMany => {
                Prost::client_streaming(
                        conn,
                        async move |grpc| inner.say_hello_many(grpc).await,
                    )
                    .await
            }
            GreeterDispatch::SayHelloChat => {
                Prost::bidi::<
                    HelloRequest,
                    HelloReply,
                    _,
                >(conn, async move |grpc| inner.say_hello_chat(grpc).await)
                    .await
            }
        }
    }
    fn has_upgrade(&self, upgrade: &Upgrade) -> bool {
        trillium_grpc::has_bidi_upgrade(upgrade)
    }
    async fn upgrade(&self, upgrade: Upgrade) {
        trillium_grpc::drive_bidi_upgrade(upgrade).await;
    }
}
pub struct GreeterClient(::trillium_grpc::trillium_client::Client);
impl From<::trillium_grpc::trillium_client::Client> for GreeterClient {
    fn from(client: ::trillium_grpc::trillium_client::Client) -> Self {
        Self(trillium_grpc::with_service_prefix(client, "greeter.v1.Greeter"))
    }
}
impl ServiceClient for GreeterClient {
    fn client(&self) -> &::trillium_grpc::trillium_client::Client {
        &self.0
    }
    fn client_mut(&mut self) -> &mut ::trillium_grpc::trillium_client::Client {
        &mut self.0
    }
}
impl GreeterClient {
    pub fn say_hello(
        &self,
        request: HelloRequest,
    ) -> UnaryConn<HelloRequest, HelloReply> {
        UnaryConn::unary::<Prost>(&self.0, "SayHello", request)
    }
    pub fn say_hello_stream(
        &self,
        request: HelloRequest,
    ) -> StreamingConn<HelloRequest, HelloReply> {
        StreamingConn::server_streaming::<Prost>(&self.0, "SayHelloStream", request)
    }
    pub fn say_hello_many(
        &self,
        requests: impl Stream<Item = HelloRequest> + Send + 'static,
    ) -> UnaryConn<HelloRequest, HelloReply> {
        UnaryConn::client_streaming::<Prost>(&self.0, "SayHelloMany", requests)
    }
    pub fn say_hello_chat(&self) -> BidiConn<HelloRequest, HelloReply> {
        BidiConn::bidi::<Prost>(&self.0, "SayHelloChat")
    }
}
