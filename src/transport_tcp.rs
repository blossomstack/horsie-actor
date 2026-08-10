use crate::envelope::{Envelope, NodeId};
use crate::transport::{RpcRequest, Transport, TransportError};
use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

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

/// What travels over a connection.
///
/// Tagged rather than inferred from shape: a request and an envelope are both
/// JSON objects, and guessing between them would misroute the first message
/// whose fields happened to overlap.
#[derive(Debug, Serialize, Deserialize)]
enum Frame {
    /// Fire-and-forget delivery to an actor.
    Envelope(Envelope),
    /// A request expecting exactly one [`Frame::Response`] carrying the same id.
    Request { id: u64, payload: Vec<u8> },
    /// The answer to a request. `payload` is `None` when the responder was
    /// dropped without answering, which the caller must see as a failure rather
    /// than wait out.
    Response { id: u64, payload: Option<Vec<u8>> },
}

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

/// Callers waiting on a reply, keyed by request id.
type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Vec<u8>>>>>;

/// One dialled connection to a peer.
///
/// The write half is behind an async mutex because frames must not interleave;
/// the read half belongs to a spawned task, since somebody has to be reading
/// continuously for a reply ever to arrive. That task is the reason this type
/// exists at all — the previous version only ever wrote, which is fine for
/// fire-and-forget and impossible for request/response.
struct Peer {
    writer: tokio::sync::Mutex<OwnedWriteHalf>,
    pending: Pending,
    next_request: AtomicU64,
}

impl Peer {
    async fn write(&self, frame: &Frame) -> std::io::Result<()> {
        let bytes = serde_json::to_vec(frame)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        let mut writer = self.writer.lock().await;
        write_frame(&mut *writer, &bytes).await
    }
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
    outbound: tokio::sync::Mutex<HashMap<NodeId, Arc<Peer>>>,
    inbox: Mutex<Option<mpsc::Receiver<Envelope>>>,
    rpc_inbox: Mutex<Option<mpsc::Receiver<RpcRequest>>>,
}

impl TcpTransport {
    /// Bind, start accepting, and return the transport.
    ///
    /// # Errors
    /// If the listen address cannot be bound.
    pub async fn bind(config: TcpConfig) -> std::io::Result<Arc<Self>> {
        let listener = TcpListener::bind(config.bind).await?;
        let (tx, rx) = mpsc::channel(INBOX_CAPACITY);
        let (rpc_tx, rpc_rx) = mpsc::channel(INBOX_CAPACITY);

        let transport = Arc::new(Self {
            local: config.local,
            peers: config.peers,
            secret: config.secret.clone(),
            outbound: tokio::sync::Mutex::new(HashMap::new()),
            inbox: Mutex::new(Some(rx)),
            rpc_inbox: Mutex::new(Some(rpc_rx)),
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
                let rpc_tx = rpc_tx.clone();
                let secret = secret.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve(stream, &secret, tx, rpc_tx).await {
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

    /// The cached connection to `to`, dialling and handshaking if there is none.
    async fn peer(&self, to: NodeId) -> Result<Arc<Peer>, TransportError> {
        let addr = *self.peers.get(&to).ok_or(TransportError::Unreachable(to))?;
        let mut cache = self.outbound.lock().await;
        if let Some(existing) = cache.get(&to) {
            return Ok(existing.clone());
        }

        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|_| TransportError::Unreachable(to))?;
        // Nagle batches small writes, and every frame here is small and
        // latency-sensitive.
        let _ = stream.set_nodelay(true);
        handshake_out(&mut stream, self.local, &self.secret)
            .await
            .map_err(|e| TransportError::Io(e.to_string()))?;

        let (reader, writer) = stream.into_split();
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let peer = Arc::new(Peer {
            writer: tokio::sync::Mutex::new(writer),
            pending: pending.clone(),
            next_request: AtomicU64::new(0),
        });
        tokio::spawn(read_replies(reader, pending));
        cache.insert(to, peer.clone());
        Ok(peer)
    }

    /// Forget the connection to `to`, so the next call redials.
    async fn drop_peer(&self, to: NodeId) {
        self.outbound.lock().await.remove(&to);
    }
}

/// Drain replies from a dialled connection until it closes.
///
/// On close every waiting caller is failed by dropping its responder, rather
/// than left to wait for an answer that can no longer arrive.
async fn read_replies(mut reader: OwnedReadHalf, pending: Pending) {
    loop {
        let Ok(frame) = read_frame_from(&mut reader).await else {
            break;
        };
        match serde_json::from_slice::<Frame>(&frame) {
            Ok(Frame::Response { id, payload }) => {
                let waiting = pending.lock().remove(&id);
                if let (Some(waiting), Some(payload)) = (waiting, payload) {
                    let _ = waiting.send(payload);
                }
            }
            Ok(_) => {
                // The dialling side never receives envelopes or requests: each
                // node dials its peers, so inbound traffic arrives on the
                // listener instead. Anything else here is a peer running a
                // protocol we do not.
                tracing::warn!("a peer sent an unexpected frame on a dialled connection");
            }
            Err(e) => {
                tracing::warn!(error = %e, "could not decode a frame from a peer");
                break;
            }
        }
    }
    pending.lock().clear();
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

async fn write_frame<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    bytes: &[u8],
) -> std::io::Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "frame longer than u32")
    })?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(bytes).await?;
    stream.flush().await
}

async fn read_frame_from<R: AsyncReadExt + Unpin>(stream: &mut R) -> std::io::Result<Vec<u8>> {
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

    let reply = read_frame_from(stream).await?;
    if reply.first() != Some(&VERSION) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "peer rejected the handshake",
        ));
    }
    Ok(())
}

/// Accepting side: verify the proof before reading a single frame.
async fn serve(
    mut stream: TcpStream,
    secret: &[u8],
    tx: mpsc::Sender<Envelope>,
    rpc_tx: mpsc::Sender<RpcRequest>,
) -> std::io::Result<()> {
    let hello = read_frame_from(&mut stream).await?;
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

    let (mut reader, writer) = stream.into_split();
    // Shared because each request is answered by its own task: a slow handler
    // must not stop the next frame being read, or one consensus round trip
    // would serialise every other message from this peer behind it.
    let writer = Arc::new(tokio::sync::Mutex::new(writer));

    loop {
        let frame = read_frame_from(&mut reader).await?;
        let frame: Frame = serde_json::from_slice(&frame)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        match frame {
            Frame::Envelope(env) => {
                if tx.send(env).await.is_err() {
                    return Ok(()); // the node has shut down
                }
            }
            Frame::Request { id, payload } => {
                let (reply, answer) = oneshot::channel();
                if rpc_tx.send(RpcRequest { payload, reply }).await.is_err() {
                    return Ok(());
                }
                let writer = writer.clone();
                tokio::spawn(async move {
                    // `Err` means the handler dropped the responder. Answering
                    // with `None` rather than staying silent is what turns a
                    // node that declined into a failed call instead of a hang.
                    let payload = answer.await.ok();
                    let frame = match serde_json::to_vec(&Frame::Response { id, payload }) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            tracing::warn!(error = %e, "could not encode an rpc response");
                            return;
                        }
                    };
                    let mut writer = writer.lock().await;
                    if let Err(e) = write_frame(&mut *writer, &frame).await {
                        tracing::debug!(error = %e, "could not write an rpc response");
                    }
                });
            }
            Frame::Response { .. } => {
                tracing::warn!("a peer sent a response on a connection it dialled");
            }
        }
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn send(&self, to: NodeId, env: Envelope) -> Result<(), TransportError> {
        let peer = self.peer(to).await?;
        match peer.write(&Frame::Envelope(env)).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Drop the connection so the next send redials rather than
                // reusing one the peer has already hung up on.
                self.drop_peer(to).await;
                Err(TransportError::Io(e.to_string()))
            }
        }
    }

    async fn rpc(&self, to: NodeId, payload: Vec<u8>) -> Result<Vec<u8>, TransportError> {
        let peer = self.peer(to).await?;
        let id = peer.next_request.fetch_add(1, Ordering::Relaxed);
        let (reply, answer) = oneshot::channel();
        peer.pending.lock().insert(id, reply);

        if let Err(e) = peer.write(&Frame::Request { id, payload }).await {
            peer.pending.lock().remove(&id);
            self.drop_peer(to).await;
            return Err(TransportError::Io(e.to_string()));
        }

        // The reader task clears every waiter when the connection closes, so a
        // peer that dies mid-request fails the call rather than parking it
        // forever.
        answer
            .await
            .map_err(|_| TransportError::Io(format!("{to} did not answer")))
    }

    fn incoming(&self) -> Option<mpsc::Receiver<Envelope>> {
        self.inbox.lock().take()
    }

    fn incoming_rpc(&self) -> Option<mpsc::Receiver<RpcRequest>> {
        self.rpc_inbox.lock().take()
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

    fn env(payload: &[u8]) -> Envelope {
        Envelope {
            kind: "counter".into(),
            id: "c1".into(),
            correlation: None,
            message_id: 1,
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

    /// Answer every request with the request bytes reversed, so a test can tell
    /// a real round trip from an echo of its own buffer.
    fn spawn_reverser(t: &Arc<TcpTransport>) {
        let mut inbox = t.incoming_rpc().unwrap();
        tokio::spawn(async move {
            while let Some(req) = inbox.recv().await {
                let mut answer = req.payload.clone();
                answer.reverse();
                let _ = req.reply.send(answer);
            }
        });
    }

    /// The whole point of the new frame type: a request goes out and its answer
    /// comes back on the same connection.
    #[tokio::test]
    async fn a_request_gets_its_answer_back() {
        let (a, b) = pair(b"shared", b"shared").await;
        spawn_reverser(&b);

        let answer = a.rpc(NodeId(2), vec![1, 2, 3]).await.unwrap();
        assert_eq!(answer, vec![3, 2, 1]);
    }

    /// Answers are matched by request id, not by arrival order. Without that a
    /// slow handler would hand its answer to whoever asked next — and consensus
    /// would act on a reply to a different question.
    #[tokio::test]
    async fn concurrent_requests_get_their_own_answers() {
        let (a, b) = pair(b"shared", b"shared").await;
        let mut inbox = b.incoming_rpc().unwrap();
        tokio::spawn(async move {
            let mut held: Vec<RpcRequest> = Vec::new();
            while let Some(req) = inbox.recv().await {
                held.push(req);
                // Answer in reverse order, so replies arrive scrambled relative
                // to the requests.
                if held.len() == 3 {
                    while let Some(req) = held.pop() {
                        let mut answer = req.payload.clone();
                        answer.reverse();
                        let _ = req.reply.send(answer);
                    }
                }
            }
        });

        let (x, y, z) = tokio::join!(
            a.rpc(NodeId(2), vec![1, 0]),
            a.rpc(NodeId(2), vec![2, 0]),
            a.rpc(NodeId(2), vec![3, 0]),
        );
        assert_eq!(x.unwrap(), vec![0, 1]);
        assert_eq!(y.unwrap(), vec![0, 2]);
        assert_eq!(z.unwrap(), vec![0, 3]);
    }

    /// Envelopes and requests share one connection without confusing each
    /// other, which is the reason frames are tagged rather than shape-sniffed.
    #[tokio::test]
    async fn envelopes_and_requests_share_a_connection() {
        let (a, b) = pair(b"shared", b"shared").await;
        let mut envelopes = b.incoming().unwrap();
        spawn_reverser(&b);

        a.send(NodeId(2), env(b"first")).await.unwrap();
        let answer = a.rpc(NodeId(2), vec![9, 8]).await.unwrap();
        a.send(NodeId(2), env(b"second")).await.unwrap();

        assert_eq!(answer, vec![8, 9]);
        assert_eq!(envelopes.recv().await.unwrap().payload, b"first");
        assert_eq!(envelopes.recv().await.unwrap().payload, b"second");
    }

    /// A handler that declines fails the call. Silence would be worse than an
    /// error: consensus would wait out its own timeout on a peer that already
    /// decided not to answer.
    #[tokio::test]
    async fn a_dropped_responder_fails_the_request() {
        let (a, b) = pair(b"shared", b"shared").await;
        let mut inbox = b.incoming_rpc().unwrap();
        tokio::spawn(async move {
            while let Some(req) = inbox.recv().await {
                drop(req.reply);
            }
        });

        let outcome =
            tokio::time::timeout(std::time::Duration::from_secs(2), a.rpc(NodeId(2), vec![1]))
                .await
                .expect("a declined request must return rather than hang");
        assert!(outcome.is_err());
    }
}
