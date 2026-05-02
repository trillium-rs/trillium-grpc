use std::sync::Arc;
use trillium::{Conn, Handler, Method};
use trillium_grpc::{
    BufferedRequestStream, Client, Prost, Server, ServiceClient, Status, Stream,
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
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<HelloReply, Status>> + Send + 'static + use<Self>,
            Status,
        >,
    > + Send;
    fn say_hello_many(
        &self,
        requests: BufferedRequestStream<HelloRequest>,
    ) -> impl Future<Output = Result<HelloReply, Status>> + Send;
    fn say_hello_chat(
        &self,
        requests: BufferedRequestStream<HelloRequest>,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<HelloReply, Status>> + Send + 'static + use<Self>,
            Status,
        >,
    > + Send;
}
pub struct GreeterServer<T>(Arc<T>);
impl<T> GreeterServer<T> {
    pub fn new(inner: T) -> Self {
        Self(Arc::new(inner))
    }
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
        match method {
            "/SayHello" => {
                let inner = Arc::clone(&self.0);
                Prost::unary(conn, move |req| async move { inner.say_hello(req).await })
                    .await
            }
            "/SayHelloStream" => {
                let inner = Arc::clone(&self.0);
                Prost::server_streaming(
                        conn,
                        move |req| async move { inner.say_hello_stream(req).await },
                    )
                    .await
            }
            "/SayHelloMany" => {
                let inner = Arc::clone(&self.0);
                Prost::client_streaming(
                        conn,
                        move |reqs| async move { inner.say_hello_many(reqs).await },
                    )
                    .await
            }
            "/SayHelloChat" => {
                let inner = Arc::clone(&self.0);
                Prost::bidi(
                        conn,
                        move |reqs| async move { inner.say_hello_chat(reqs).await },
                    )
                    .await
            }
            _ => conn,
        }
    }
}
pub struct GreeterClient(trillium_client::Client);
impl From<trillium_client::Client> for GreeterClient {
    fn from(client: trillium_client::Client) -> Self {
        Self(trillium_grpc::with_service_prefix(client, "greeter.v1.Greeter"))
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
    ) -> Result<
        impl Stream<Item = Result<HelloReply, Status>> + Send + 'static,
        Status,
    > {
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
    ) -> Result<
        impl Stream<Item = Result<HelloReply, Status>> + Send + 'static,
        Status,
    > {
        Prost::bidi_call(&self.0, "SayHelloChat", requests).await
    }
}
