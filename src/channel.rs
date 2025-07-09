use std::{future::Future, pin::Pin, task::ready};

use http::Uri;
use tokio::sync::mpsc::Sender;
use tonic::transport::Endpoint;
use tower::{util::BoxCloneService, Service};

/// A change in the service set.
#[derive(Debug, Clone)]
pub enum Change<K, V> {
    /// A new service identified by key `K` was identified.
    Insert(K, V),
    /// The service identified by key `K` disappeared.
    Remove(K),
}

/// A type alias to make the below types easier to represent.
pub type EndpointUpdater = Sender<Change<Uri, Endpoint>>;

/// Creates a balanced channel.
pub trait BalancedChannelBuilder {
    type Error;

    /// Makes a new balanced channel, given the provided options.
    fn balanced_channel(
        self,
        buffer_size: usize,
    ) -> Result<(Channel, EndpointUpdater), Self::Error>;
}

/// Create a simple Tonic channel.
pub struct Tonic;

impl BalancedChannelBuilder for Tonic {
    type Error = tonic::transport::Error;

    #[inline]
    fn balanced_channel(
        self,
        buffer_size: usize,
    ) -> Result<(Channel, EndpointUpdater), Self::Error> {
        let (chan, tx) = tonic::transport::Channel::balance_channel(buffer_size);

        let (bridge_tx, mut rx) = tokio::sync::mpsc::channel(buffer_size);
        tokio::spawn(async move {
            while let Some(change) = rx.recv().await {
                let change = match change {
                    Change::Insert(k, v) => tonic::transport::channel::Change::Insert(k, v),
                    Change::Remove(k) => tonic::transport::channel::Change::Remove(k),
                };

                if let Err(_) = tx.send(change).await {
                    break;
                }
            }
        });

        Ok((Channel::Tonic(chan), bridge_tx))
    }
}

/// Create an Openssl-backed channel.
#[cfg(feature = "tls-openssl")]
pub struct Openssl {
    pub(crate) conn: crate::openssl_tls::OpenSslConnector,
}

#[cfg(feature = "tls-openssl")]
impl BalancedChannelBuilder for Openssl {
    type Error = crate::error::Error;

    #[inline]
    fn balanced_channel(
        self,
        buffer_size: usize,
    ) -> Result<(Channel, EndpointUpdater), Self::Error> {
        let (chan, tx) = crate::openssl_tls::balanced_channel(self.conn)?;
        let (bridge_tx, mut rx) = tokio::sync::mpsc::channel(buffer_size);
        tokio::spawn(async move {
            while let Some(change) = rx.recv().await {
                let change = match change {
                    Change::Insert(k, v) => tower::discover::Change::Insert(k, v),
                    Change::Remove(k) => tower::discover::Change::Remove(k),
                };

                if let Err(_) = tx.send(change).await {
                    break;
                }
            }
        });

        Ok((Channel::Openssl(chan), bridge_tx))
    }
}

type TonicRequest = http::Request<tonic::body::Body>;
type TonicResponse = http::Response<tonic::body::Body>;
pub type CustomChannel = BoxCloneService<TonicRequest, TonicResponse, tower::BoxError>;

/// Represents a channel that can be created by a BalancedChannelBuilder
/// or may be initialized externally and passed into the client.
#[derive(Clone)]
pub enum Channel {
    /// A standard tonic channel.
    Tonic(tonic::transport::Channel),

    /// An OpenSSL channel.
    #[cfg(feature = "tls-openssl")]
    Openssl(crate::openssl_tls::OpenSslChannel),

    /// A custom Service impl, inside a Box.
    Custom(CustomChannel),
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel").finish_non_exhaustive()
    }
}

pub enum ChannelFuture {
    Tonic(<tonic::transport::Channel as Service<TonicRequest>>::Future),
    #[cfg(feature = "tls-openssl")]
    Openssl(<crate::openssl_tls::OpenSslChannel as Service<TonicRequest>>::Future),
    Custom(<CustomChannel as Service<TonicRequest>>::Future),
}

impl std::future::Future for ChannelFuture {
    type Output = Result<TonicResponse, tower::BoxError>;

    #[inline]
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // Safety: trivial projection
        unsafe {
            let this = self.get_unchecked_mut();
            match this {
                ChannelFuture::Tonic(fut) => {
                    let fut = Pin::new_unchecked(fut);
                    let result = ready!(Future::poll(fut, cx));
                    result.map_err(|e| Box::new(e) as tower::BoxError).into()
                }
                #[cfg(feature = "tls-openssl")]
                ChannelFuture::Openssl(fut) => {
                    let fut = Pin::new_unchecked(fut);
                    Future::poll(fut, cx)
                }
                ChannelFuture::Custom(fut) => {
                    let fut = Pin::new_unchecked(fut);
                    Future::poll(fut, cx)
                }
            }
        }
    }
}

impl ChannelFuture {
    #[inline]
    fn from_tonic(value: <tonic::transport::Channel as Service<TonicRequest>>::Future) -> Self {
        Self::Tonic(value)
    }

    #[cfg(feature = "tls-openssl")]
    #[inline]
    fn from_openssl(
        value: <crate::openssl_tls::OpenSslChannel as Service<TonicRequest>>::Future,
    ) -> Self {
        Self::Openssl(value)
    }

    #[inline]
    fn from_custom(value: <CustomChannel as Service<TonicRequest>>::Future) -> Self {
        Self::Custom(value)
    }
}

impl Service<TonicRequest> for Channel {
    type Response = TonicResponse;
    type Error = tower::BoxError;
    type Future = ChannelFuture;

    #[inline]
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        match self {
            Channel::Tonic(channel) => {
                let result = ready!(channel.poll_ready(cx));
                result.map_err(|e| Box::new(e) as tower::BoxError).into()
            }
            #[cfg(feature = "tls-openssl")]
            Channel::Openssl(openssl) => openssl.poll_ready(cx),
            Channel::Custom(custom) => custom.poll_ready(cx),
        }
    }

    #[inline]
    fn call(&mut self, req: TonicRequest) -> Self::Future {
        match self {
            Channel::Tonic(channel) => ChannelFuture::from_tonic(channel.call(req)),
            #[cfg(feature = "tls-openssl")]
            Channel::Openssl(openssl) => ChannelFuture::from_openssl(openssl.call(req)),
            Channel::Custom(custom) => ChannelFuture::from_custom(custom.call(req)),
        }
    }
}
