//! The transport seam.
//!
//! Everything that touches a socket is behind [`Network`]. The ingest loop in
//! [`crate::ingest`] is generic over it, which is what lets the reconnect and
//! gap-fill behaviour — the part of this project most likely to be wrong and
//! least likely to be caught in production — be driven entirely offline by
//! [`fake::FakeNetwork`]. The plan is explicit that chaos and fault injection
//! are permitted only against a fake, never against a live feed, because
//! venues ban on reconnect storms. This trait is what makes that possible.
//!
//! # Why the futures carry an explicit `Send` bound
//!
//! `async fn` in a trait does not promise its future is `Send`, and an ingest
//! task has to be spawnable onto a multi-threaded runtime. Writing the methods
//! as `-> impl Future<Output = _> + Send` states that requirement in the trait
//! rather than discovering it at the first `tokio::spawn`.

use std::future::Future;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("could not connect to {url}: {source}")]
    Connect {
        url: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("websocket error: {0}")]
    Socket(Box<dyn std::error::Error + Send + Sync>),
    #[error("http {status} from {url}")]
    HttpStatus { url: String, status: u16 },
    #[error("http request to {url} failed: {source}")]
    Http {
        url: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// One open connection to a venue.
pub trait Transport: Send {
    /// Send a text frame — every venue in this project subscribes with JSON.
    fn send_text(&mut self, text: &str) -> impl Future<Output = Result<(), NetError>> + Send;

    /// Wait for the next payload-carrying frame.
    ///
    /// `Ok(None)` means the peer closed cleanly. Protocol housekeeping —
    /// pings, pongs — is handled beneath this and never surfaces, so a caller
    /// counting frames is counting venue messages rather than plumbing.
    fn recv(&mut self) -> impl Future<Output = Result<Option<Vec<u8>>, NetError>> + Send;
}

/// How to reach venues. One implementation talks to the internet; the other
/// reads from a script.
pub trait Network: Send + Sync + 'static {
    type Socket: Transport;

    fn connect(&self, url: &str) -> impl Future<Output = Result<Self::Socket, NetError>> + Send;

    /// GET a URL and return the body as text. Used only for the REST depth
    /// snapshot that Bitstamp's recovery needs; the other two venues send
    /// their snapshot over the websocket and never call this.
    fn get(&self, url: &str) -> impl Future<Output = Result<String, NetError>> + Send;
}

/// The real one: `tokio-tungstenite` over rustls, and `reqwest` for REST.
#[derive(Debug, Clone)]
pub struct LiveNetwork {
    http: reqwest::Client,
}

impl LiveNetwork {
    /// # Errors
    /// If the HTTP client cannot be built — in practice only a TLS backend
    /// failure, which is not recoverable at runtime.
    pub fn new() -> Result<Self, NetError> {
        let http = reqwest::Client::builder()
            // A depth snapshot that takes longer than this is not worth
            // waiting for: the book it describes has moved on, and the ingest
            // task will ask again.
            .timeout(Duration::from_secs(10))
            .user_agent(concat!("market-aggregator/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| NetError::Http {
                url: "<client construction>".to_owned(),
                source: Box::new(e),
            })?;
        Ok(Self { http })
    }
}

/// A live websocket, with the protocol frames filtered out.
#[derive(Debug)]
pub struct LiveSocket {
    inner: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
}

impl Transport for LiveSocket {
    async fn send_text(&mut self, text: &str) -> Result<(), NetError> {
        use futures_util::SinkExt;
        self.inner
            .send(tokio_tungstenite::tungstenite::Message::text(text))
            .await
            .map_err(|e| NetError::Socket(Box::new(e)))
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>, NetError> {
        use futures_util::StreamExt;
        use tokio_tungstenite::tungstenite::Message;

        loop {
            let Some(message) = self.inner.next().await else {
                return Ok(None);
            };
            match message.map_err(|e| NetError::Socket(Box::new(e)))? {
                Message::Text(text) => return Ok(Some(text.as_bytes().to_vec())),
                Message::Binary(bytes) => return Ok(Some(bytes.to_vec())),
                Message::Close(_) => return Ok(None),
                // tungstenite queues the pong itself; there is nothing for a
                // caller to do with either, and surfacing them would make the
                // idle watchdog treat plumbing as venue liveness.
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }
}

impl Network for LiveNetwork {
    type Socket = LiveSocket;

    async fn connect(&self, url: &str) -> Result<Self::Socket, NetError> {
        let (inner, _response) =
            tokio_tungstenite::connect_async(url)
                .await
                .map_err(|e| NetError::Connect {
                    url: url.to_owned(),
                    source: Box::new(e),
                })?;
        Ok(LiveSocket { inner })
    }

    async fn get(&self, url: &str) -> Result<String, NetError> {
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| NetError::Http {
                url: url.to_owned(),
                source: Box::new(e),
            })?;

        let status = response.status();
        if !status.is_success() {
            return Err(NetError::HttpStatus {
                url: url.to_owned(),
                status: status.as_u16(),
            });
        }

        response.text().await.map_err(|e| NetError::Http {
            url: url.to_owned(),
            source: Box::new(e),
        })
    }
}

/// A scripted network, for driving the ingest loop with no sockets at all.
pub mod fake {
    use super::{NetError, Network, Transport};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex, PoisonError};

    /// What one connection attempt does.
    #[derive(Clone, Debug)]
    pub enum Session {
        /// The connection is refused outright. Drives the backoff schedule.
        Refuse,
        /// The connection opens, yields these payloads in order, then ends
        /// the way [`Session::Serve::then`] says.
        Serve {
            frames: Vec<String>,
            then: SessionEnd,
        },
    }

    /// How a served session finishes.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum SessionEnd {
        /// Peer closed cleanly.
        Close,
        /// Socket error mid-stream.
        Error,
        /// Nothing further is ever sent, and the socket stays open. This is
        /// the case a plain "did it reconnect?" test misses entirely: the
        /// connection is not broken, it is *silent*, and only the idle
        /// watchdog can tell the difference between that and a quiet market.
        Hang,
    }

    /// Hands out [`Session`]s in order; every attempt past the end of the
    /// script repeats the last one, so a test can specify "fail twice then
    /// stay up" without padding.
    #[derive(Debug, Clone)]
    pub struct FakeNetwork {
        script: Arc<Mutex<VecDeque<Session>>>,
        last: Arc<Mutex<Session>>,
        attempts: Arc<Mutex<Vec<String>>>,
        rest_body: Arc<Mutex<Result<String, ()>>>,
    }

    impl FakeNetwork {
        pub fn new(sessions: impl IntoIterator<Item = Session>) -> Self {
            let script: VecDeque<Session> = sessions.into_iter().collect();
            let last = script.back().cloned().unwrap_or(Session::Refuse);
            Self {
                script: Arc::new(Mutex::new(script)),
                last: Arc::new(Mutex::new(last)),
                attempts: Arc::new(Mutex::new(Vec::new())),
                rest_body: Arc::new(Mutex::new(Err(()))),
            }
        }

        /// Body to return from [`Network::get`], for the Bitstamp REST splice.
        #[must_use]
        pub fn with_rest_body(self, body: impl Into<String>) -> Self {
            *lock(&self.rest_body) = Ok(body.into());
            self
        }

        /// Every URL `connect` was called with, in order. Asserting on the
        /// count is how the reconnect tests count attempts.
        pub fn attempts(&self) -> Vec<String> {
            lock(&self.attempts).clone()
        }
    }

    fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        m.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[derive(Debug)]
    pub struct FakeSocket {
        frames: VecDeque<String>,
        then: SessionEnd,
        sent: Vec<String>,
    }

    impl FakeSocket {
        /// Frames the ingest task sent us — the subscribe payloads.
        pub fn sent(&self) -> &[String] {
            &self.sent
        }
    }

    impl Transport for FakeSocket {
        async fn send_text(&mut self, text: &str) -> Result<(), NetError> {
            self.sent.push(text.to_owned());
            Ok(())
        }

        async fn recv(&mut self) -> Result<Option<Vec<u8>>, NetError> {
            if let Some(frame) = self.frames.pop_front() {
                return Ok(Some(frame.into_bytes()));
            }
            match self.then {
                SessionEnd::Close => Ok(None),
                SessionEnd::Error => Err(NetError::Socket("scripted socket error".into())),
                // Never resolves. Under tokio's paused clock the idle
                // watchdog's timeout is what completes instead, which is
                // exactly the behaviour being tested.
                SessionEnd::Hang => std::future::pending().await,
            }
        }
    }

    impl Network for FakeNetwork {
        type Socket = FakeSocket;

        async fn connect(&self, url: &str) -> Result<Self::Socket, NetError> {
            lock(&self.attempts).push(url.to_owned());

            let session = match lock(&self.script).pop_front() {
                Some(session) => {
                    *lock(&self.last) = session.clone();
                    session
                }
                None => lock(&self.last).clone(),
            };

            match session {
                Session::Refuse => Err(NetError::Connect {
                    url: url.to_owned(),
                    source: "scripted refusal".into(),
                }),
                Session::Serve { frames, then } => Ok(FakeSocket {
                    frames: frames.into(),
                    then,
                    sent: Vec::new(),
                }),
            }
        }

        async fn get(&self, url: &str) -> Result<String, NetError> {
            match &*lock(&self.rest_body) {
                Ok(body) => Ok(body.clone()),
                Err(()) => Err(NetError::HttpStatus {
                    url: url.to_owned(),
                    status: 503,
                }),
            }
        }
    }
}
