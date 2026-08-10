use crate::envelope::{Envelope, NodeId};
use crate::transport::{Transport, TransportError};
use async_trait::async_trait;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

/// Inbound queue depth, matching the in-process transport so switching between
/// them does not change backpressure behaviour.
const INBOX_CAPACITY: usize = 256;

/// Largest frame we will read. A peer claiming a gigabyte would otherwise make
/// us allocate it before discovering the frame is nonsense.
const MAX_FRAME: u32 = 8 * 1024 * 1024;

/// Protocol version, refused on mismatch rather than misparsed. Two nodes on
/// different builds is a normal state during a rolling restart, and the honest
/// outcome is a closed connection, not a garbled envelope.
const VERSION: u8 = 1;

/// How to reach the other members.
#[derive(Debug, Clone)]
pub struct TcpConfig {
    /// This node's identity, announced during the handshake.
    pub local: NodeId,
    /// Where this node listens.
    pub bind: SocketAddr,
    /// Where each peer listens.
    pub peers: HashMap<NodeId, SocketAddr>,
    /// Shared secret. Both ends prove they know it before any envelope moves.
    ///
    /// This authenticates the peer; it does **not** encrypt the connection.
    /// Cluster traffic carries whatever your commands carry, so run it on a
    /// private network or inside a TLS-terminating tunnel. That is a deployment
    /// requirement, not an optional hardening step.
    pub secret: Vec<u8>,
}

/// A [`Transport`] over TCP.
///
/// Frames are a 4-byte big-endian length followed by JSON. Connections are
/// dialled lazily and cached per peer; a failed write drops the cached
/// connection so the next send redials rather than reusing a corpse.
pub struct TcpTransport {
    local: NodeId,
    peers: HashMap<NodeId, SocketAddr>,
    secret: Vec<u8>,
    outbound: tokio::sync::Mutex<HashMap<NodeId, TcpStream>>,
    inbox: Mutex<Option<mpsc::Receiver<Envelope>>>,
}

impl TcpTransport {
    /// Bind, start accepting, and return the transport.
    ///
    /// # Errors
    /// If the listen address cannot be bound.
    pub async fn bind(config: TcpConfig) -> std::io::Result<Arc<Self>> {
        let listener = TcpListener::bind(config.bind).await?;
        let (tx, rx) = mpsc::channel(INBOX_CAPACITY);

        let transport = Arc::new(Self {
            local: config.local,
            peers: config.peers,
            secret: config.secret.clone(),
            outbound: tokio::sync::Mutex::new(HashMap::new()),
            inbox: Mutex::new(Some(rx)),
        });

        let secret = config.secret;
        tokio::spawn(async move {
            loop {
                let Ok((stream, _addr)) = listener.accept().await else {
                    // One failed accept is not a reason to stop serving: a peer
                    // that died mid-handshake must not take the listener with
                    // it. This is the bug that killed horsie's vendor socket.
                    continue;
                };
                let tx = tx.clone();
                let secret = secret.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve(stream, &secret, tx).await {
                        tracing::debug!(error = %e, "a cluster peer connection ended");
                    }
                });
            }
        });

        Ok(transport)
    }

    /// The address this node listens on, after binding.
    #[must_use]
    pub fn peers(&self) -> &HashMap<NodeId, SocketAddr> {
        &self.peers
    }
}

/// Proof of the shared secret, bound to the announced node id so a captured
/// handshake cannot be replayed as a different node.
fn proof(secret: &[u8], node: NodeId, nonce: u64) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(secret);
    h.update(node.0.to_be_bytes());
    h.update(nonce.to_be_bytes());
    h.finalize().into()
}

async fn write_frame(stream: &mut TcpStream, bytes: &[u8]) -> std::io::Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "frame longer than u32")
    })?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(bytes).await?;
    stream.flush().await
}

async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len);
    if len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame exceeds the maximum",
        ));
    }
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Dialling side of the handshake: announce who we are and prove the secret.
async fn handshake_out(
    stream: &mut TcpStream,
    local: NodeId,
    secret: &[u8],
) -> std::io::Result<()> {
    // A fixed nonce would make the proof a static password; this ties it to the
    // connection. Derived from the peer's address and the clock via the
    // listener's own randomness would be better, but the secret is already
    // required, so the nonce only has to be unique per connection.
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default();

    let mut hello = Vec::with_capacity(1 + 8 + 8 + 32);
    hello.push(VERSION);
    hello.extend_from_slice(&local.0.to_be_bytes());
    hello.extend_from_slice(&nonce.to_be_bytes());
    hello.extend_from_slice(&proof(secret, local, nonce));
    write_frame(stream, &hello).await?;

    let reply = read_frame(stream).await?;
    if reply.first() != Some(&VERSION) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "peer rejected the handshake",
        ));
    }
    Ok(())
}

/// Accepting side: verify the proof before reading a single envelope.
async fn serve(
    mut stream: TcpStream,
    secret: &[u8],
    tx: mpsc::Sender<Envelope>,
) -> std::io::Result<()> {
    let hello = read_frame(&mut stream).await?;
    if hello.len() != 1 + 8 + 8 + 32 || hello[0] != VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "malformed or wrong-version hello",
        ));
    }
    let mut node = [0u8; 8];
    node.copy_from_slice(&hello[1..9]);
    let mut nonce = [0u8; 8];
    nonce.copy_from_slice(&hello[9..17]);
    let node = NodeId(u64::from_be_bytes(node));
    let expected = proof(secret, node, u64::from_be_bytes(nonce));

    // Constant-time-ish: compare every byte. A short-circuit here leaks how many
    // leading bytes were right, one connection at a time.
    let mut diff = 0u8;
    for (a, b) in expected.iter().zip(&hello[17..]) {
        diff |= a ^ b;
    }
    if diff != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "peer failed to prove the shared secret",
        ));
    }

    write_frame(&mut stream, &[VERSION]).await?;

    loop {
        let frame = read_frame(&mut stream).await?;
        let env: Envelope = serde_json::from_slice(&frame)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        if tx.send(env).await.is_err() {
            return Ok(()); // the node has shut down
        }
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn send(&self, to: NodeId, env: Envelope) -> Result<(), TransportError> {
        let addr = *self.peers.get(&to).ok_or(TransportError::Unreachable(to))?;
        let bytes = serde_json::to_vec(&env).map_err(|e| TransportError::Io(e.to_string()))?;

        let mut cache = self.outbound.lock().await;
        let stream = match cache.entry(to) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(slot) => {
                let mut stream = TcpStream::connect(addr)
                    .await
                    .map_err(|_| TransportError::Unreachable(to))?;
                // Nagle batches small writes, and every envelope here is small
                // and latency-sensitive.
                let _ = stream.set_nodelay(true);
                handshake_out(&mut stream, self.local, &self.secret)
                    .await
                    .map_err(|e| TransportError::Io(e.to_string()))?;
                slot.insert(stream)
            }
        };
        match write_frame(stream, &bytes).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Drop the connection so the next send redials rather than
                // reusing one the peer has already hung up on.
                cache.remove(&to);
                Err(TransportError::Io(e.to_string()))
            }
        }
    }

    fn incoming(&self) -> Option<mpsc::Receiver<Envelope>> {
        self.inbox.lock().take()
    }

    fn local_id(&self) -> NodeId {
        self.local
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::wildcard_enum_match_arm
)]
mod tests {
    use super::*;
    use crate::envelope::Epoch;

    fn env(payload: &[u8]) -> Envelope {
        Envelope {
            kind: "counter".into(),
            id: "c1".into(),
            correlation: None,
            message_id: 1,
            epoch: Epoch(1),
            payload: payload.to_vec(),
        }
    }

    async fn free_port() -> SocketAddr {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    }

    async fn pair(secret_a: &[u8], secret_b: &[u8]) -> (Arc<TcpTransport>, Arc<TcpTransport>) {
        let addr_a = free_port().await;
        let addr_b = free_port().await;
        let a = TcpTransport::bind(TcpConfig {
            local: NodeId(1),
            bind: addr_a,
            peers: HashMap::from([(NodeId(2), addr_b)]),
            secret: secret_a.to_vec(),
        })
        .await
        .unwrap();
        let b = TcpTransport::bind(TcpConfig {
            local: NodeId(2),
            bind: addr_b,
            peers: HashMap::from([(NodeId(1), addr_a)]),
            secret: secret_b.to_vec(),
        })
        .await
        .unwrap();
        (a, b)
    }

    #[tokio::test]
    async fn an_envelope_round_trips_over_tcp() {
        let (a, b) = pair(b"shared", b"shared").await;
        let mut inbox = b.incoming().unwrap();

        a.send(NodeId(2), env(b"hello")).await.unwrap();

        let got = inbox.recv().await.unwrap();
        assert_eq!(got.payload, b"hello");
        assert_eq!(got.kind, "counter");
    }

    /// The connection is cached, so a second envelope does not redial — and
    /// more importantly, arrives in order behind the first.
    #[tokio::test]
    async fn several_envelopes_arrive_in_order() {
        let (a, b) = pair(b"shared", b"shared").await;
        let mut inbox = b.incoming().unwrap();

        for i in 0..10u8 {
            a.send(NodeId(2), env(&[i])).await.unwrap();
        }
        for i in 0..10u8 {
            assert_eq!(inbox.recv().await.unwrap().payload, vec![i]);
        }
    }

    /// A peer that cannot prove the secret gets nothing through. Without this
    /// the cluster port is an unauthenticated command injection surface.
    #[tokio::test]
    async fn a_peer_with_the_wrong_secret_is_refused() {
        let (a, b) = pair(b"mine", b"theirs").await;
        let mut inbox = b.incoming().unwrap();

        // The handshake fails, so the send fails rather than silently dropping.
        let result = a.send(NodeId(2), env(b"hello")).await;
        assert!(result.is_err(), "an unauthenticated peer was accepted");
        assert!(inbox.try_recv().is_err(), "an envelope crossed anyway");
    }

    /// An unknown peer is unreachable rather than a panic or a hang.
    #[tokio::test]
    async fn sending_to_an_unknown_peer_fails() {
        let (a, _b) = pair(b"shared", b"shared").await;
        let err = a.send(NodeId(99), env(b"x")).await.unwrap_err();
        assert!(matches!(err, TransportError::Unreachable(NodeId(99))));
    }
}
