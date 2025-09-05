use std::{net::SocketAddr, sync::Arc};

use crate::{CongestionControl, Connect, ServerError, Session, Settings};

use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use iroh::NodeId;
use url::Url;

/// Construct a WebTransport [Server] using sane defaults.
///
/// This is optional; advanced users may use [Server::new] directly.
pub struct ServerBuilder {
    addr: Option<SocketAddr>,
    congestion_controller:
        Option<Arc<dyn quinn::congestion::ControllerFactory + Send + Sync + 'static>>,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerBuilder {
    /// Create a server builder with sane defaults.
    pub fn new() -> Self {
        Self {
            addr: None,
            congestion_controller: None,
        }
    }

    /// Listen on the specified address.
    pub fn with_addr(self, addr: SocketAddr) -> Self {
        Self {
            addr: Some(addr),
            ..self
        }
    }

    /// Enable the specified congestion controller.
    pub fn with_congestion_control(mut self, algorithm: CongestionControl) -> Self {
        self.congestion_controller = match algorithm {
            CongestionControl::LowLatency => {
                Some(Arc::new(quinn::congestion::BbrConfig::default()))
            }
            // TODO BBR is also higher throughput in theory.
            CongestionControl::Throughput => {
                Some(Arc::new(quinn::congestion::CubicConfig::default()))
            }
            CongestionControl::Default => None,
        };

        self
    }

    pub async fn with_secret_key(self, secret_key: iroh::SecretKey) -> Result<Server, ServerError> {
        let mut builder = iroh::Endpoint::builder()
            .secret_key(secret_key)
            .discovery_n0()
            .alpns(vec![crate::ALPN.as_bytes().to_vec()]);
        if let Some(addr) = self.addr {
            builder = match addr {
                SocketAddr::V4(addr) => builder.bind_addr_v4(addr),
                SocketAddr::V6(addr) => builder.bind_addr_v6(addr),
            };
        }
        let endpoint = builder
            .bind()
            .await
            .map_err(|err| ServerError::Bind(Arc::new(err)))?;

        Ok(Server::new(endpoint))
    }
}

/// A WebTransport server that accepts new sessions.
pub struct Server {
    endpoint: iroh::Endpoint,
    accept: FuturesUnordered<BoxFuture<'static, Result<Request, ServerError>>>,
}

impl Server {
    /// Manaully create a new server with a manually constructed Endpoint.
    ///
    /// NOTE: The ALPN must be set to `crate::ALPN` for WebTransport to work.
    pub fn new(endpoint: iroh::Endpoint) -> Self {
        Self {
            endpoint,
            accept: Default::default(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.endpoint.node_id()
    }

    /// Accept a new WebTransport session Request from a client.
    pub async fn accept(&mut self) -> Option<Request> {
        loop {
            tokio::select! {
                res = self.endpoint.accept() => {
                    let conn = res?;
                    self.accept.push(Box::pin(async move {
                        let conn = conn.await?;
                        Request::accept(conn).await
                    }));
                }
                Some(res) = self.accept.next() => {
                    if let Ok(session) = res {
                        return Some(session)
                    }
                }
            }
        }
    }
}

/// A mostly complete WebTransport handshake, just awaiting the server's decision on whether to accept or reject the session based on the URL.
pub struct Request {
    conn: iroh::endpoint::Connection,
    settings: Settings,
    connect: Connect,
}

impl Request {
    /// Accept a new WebTransport session from a client.
    pub async fn accept(conn: iroh::endpoint::Connection) -> Result<Self, ServerError> {
        // Perform the H3 handshake by sending/reciving SETTINGS frames.
        let settings = Settings::connect(&conn).await?;

        // Accept the CONNECT request but don't send a response yet.
        let connect = Connect::accept(&conn).await?;

        // Return the resulting request with a reference to the settings/connect streams.
        Ok(Self {
            conn,
            settings,
            connect,
        })
    }

    /// Returns the URL provided by the client.
    pub fn url(&self) -> &Url {
        self.connect.url()
    }

    /// Accept the session, returning a 200 OK.
    pub async fn ok(mut self) -> Result<Session, quinn::WriteError> {
        self.connect.respond(http::StatusCode::OK).await?;
        Ok(Session::new(self.conn, self.settings, self.connect))
    }

    /// Reject the session, returing your favorite HTTP status code.
    pub async fn close(mut self, status: http::StatusCode) -> Result<(), quinn::WriteError> {
        self.connect.respond(status).await?;
        Ok(())
    }
}
