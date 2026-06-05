use std::sync::Arc;
use trillium::{Conn, Handler, Method, Upgrade};
use trillium_grpc::{
    BidiResponder, GrpcServerConn, Prost, Server, Status, Stream, prepare_grpc_conn,
    prost,
};
#[derive(Clone, PartialEq, Eq, Hash, prost::Message)]
#[prost(prost_path = "prost")]
pub struct HelloRequest {
    #[prost(string, tag = "1")]
    pub name: prost::alloc::string::String,
}
#[derive(Clone, PartialEq, Eq, Hash, prost::Message)]
#[prost(prost_path = "prost")]
pub struct HelloReply {
    #[prost(string, tag = "1")]
    pub message: prost::alloc::string::String,
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
