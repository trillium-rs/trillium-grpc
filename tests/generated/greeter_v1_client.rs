use trillium_grpc::{
    BidiConn, Prost, ServiceClient, Stream, StreamingConn, UnaryConn, prost,
    trillium_client::Client,
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
pub struct GreeterClient(Client);
impl From<Client> for GreeterClient {
    fn from(client: Client) -> Self {
        Self(trillium_grpc::with_service_prefix(client, "greeter.v1.Greeter"))
    }
}
impl ServiceClient for GreeterClient {
    fn client(&self) -> &Client {
        &self.0
    }
    fn client_mut(&mut self) -> &mut Client {
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
