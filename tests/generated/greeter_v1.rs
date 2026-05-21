use std::sync::Arc;
use trillium::{Conn, Handler, Method, Upgrade};
use trillium_grpc::{
    Channel, Client, Prost, RequestStream, ResponseSink, Server, ServiceClient, Status, Stream,
    prepare_grpc_conn,
};
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct HelloRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct HelloReply {
    #[prost(string, tag = "1")]
    pub message: ::prost::alloc::string::String,
}
pub trait Greeter: Send + Sync + 'static {
    fn say_hello(
        &self,
        request: HelloRequest,
    ) -> impl Future<Output = Result<HelloReply, Status>> + Send;
    fn say_hello_stream(
        &self,
        request: HelloRequest,
        responses: ResponseSink<'_, HelloReply>,
    ) -> impl Future<Output = Result<(), Status>> + Send;
    fn say_hello_many(
        &self,
        requests: RequestStream<'_, HelloRequest>,
    ) -> impl Future<Output = Result<HelloReply, Status>> + Send;
    fn say_hello_chat(
        &self,
        channel: Channel<'_, HelloRequest, HelloReply>,
    ) -> impl Future<Output = Result<(), Status>> + Send;
}
pub struct GreeterServer<T>(Arc<T>);
impl<T> GreeterServer<T> {
    pub fn new(inner: T) -> Self {
        Self(Arc::new(inner))
    }
}
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
        conn.with_state(dispatch).upgrade().halt()
    }
    fn has_upgrade(&self, upgrade: &Upgrade) -> bool {
        upgrade.state().get::<GreeterDispatch>().is_some()
    }
    async fn upgrade(&self, mut upgrade: Upgrade) {
        let dispatch = upgrade.state_mut().take::<GreeterDispatch>().unwrap();
        let inner = Arc::clone(&self.0);
        match dispatch {
            GreeterDispatch::SayHello => {
                Prost::unary(upgrade, async move |req| inner.say_hello(req).await).await
            }
            GreeterDispatch::SayHelloStream => {
                Prost::server_streaming(upgrade, async move |req, sink| {
                    inner.say_hello_stream(req, sink).await
                })
                .await
            }
            GreeterDispatch::SayHelloMany => {
                Prost::client_streaming(upgrade, async move |reqs| inner.say_hello_many(reqs).await)
                    .await
            }
            GreeterDispatch::SayHelloChat => {
                Prost::bidi(upgrade, async move |channel| {
                    inner.say_hello_chat(channel).await
                })
                .await
            }
        }
    }
}
pub struct GreeterClient(trillium_client::Client);
impl From<trillium_client::Client> for GreeterClient {
    fn from(client: trillium_client::Client) -> Self {
        Self(trillium_grpc::with_service_prefix(
            client,
            "greeter.v1.Greeter",
        ))
    }
}
impl ServiceClient for GreeterClient {
    fn client(&self) -> &trillium_client::Client {
        &self.0
    }
    fn client_mut(&mut self) -> &mut trillium_client::Client {
        &mut self.0
    }
}
impl GreeterClient {
    pub async fn say_hello(&self, request: HelloRequest) -> Result<HelloReply, Status> {
        Prost::unary_call(&self.0, "SayHello", request).await
    }
    pub async fn say_hello_stream(
        &self,
        request: HelloRequest,
    ) -> Result<impl Stream<Item = Result<HelloReply, Status>> + Send + 'static, Status> {
        Prost::server_streaming_call(&self.0, "SayHelloStream", request).await
    }
    pub async fn say_hello_many(
        &self,
        requests: impl Stream<Item = HelloRequest> + Send + 'static,
    ) -> Result<HelloReply, Status> {
        Prost::client_streaming_call(&self.0, "SayHelloMany", requests).await
    }
    pub async fn say_hello_chat(
        &self,
        requests: impl Stream<Item = HelloRequest> + Send + 'static,
    ) -> Result<impl Stream<Item = Result<HelloReply, Status>> + Send + 'static, Status> {
        Prost::bidi_call(&self.0, "SayHelloChat", requests).await
    }
}
