use std::convert::{Into, TryFrom, TryInto};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::time::Duration;

use actix::dev::{MessageResponse, ResponseChannel};
use actix::{Actor, Message, Recipient};
use chrono::{DateTime, Utc};
use protobuf::well_known_types::UInt32Value;
use protobuf::{RepeatedField, SingularPtrField};
use serde_derive::{Deserialize, Serialize};
use tokio::net::TcpStream;

use near_chain::{Block, BlockApproval, BlockHeader, Weight};
use near_protos::network as network_proto;
use primitives::crypto::signature::{PublicKey, SecretKey, Signature};
use primitives::hash::CryptoHash;
use primitives::logging::pretty_str;
use primitives::traits::Base58Encoded;
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, BlockIndex, MerkleHash, ShardId};
use primitives::utils::{proto_to_type, to_string_value};

/// Current latest version of the protocol
pub const PROTOCOL_VERSION: u32 = 1;

/// Peer id is the public key.
#[derive(Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Serialize, Deserialize, Debug)]
pub struct PeerId(PublicKey);

impl From<PeerId> for Vec<u8> {
    fn from(peer_id: PeerId) -> Vec<u8> {
        peer_id.0.into()
    }
}

impl From<PublicKey> for PeerId {
    fn from(public_key: PublicKey) -> PeerId {
        PeerId(public_key)
    }
}

impl Hash for PeerId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.0.as_ref());
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", pretty_str(&self.0.to_base58(), 4))
    }
}

/// Peer information.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: PeerId,
    pub addr: Option<SocketAddr>,
    pub account_id: Option<AccountId>,
}

impl PeerInfo {
    pub fn addr_port(&self) -> Option<u16> {
        self.addr.map(|addr| addr.port())
    }
}

impl PeerInfo {
    pub fn new(id: PeerId, addr: SocketAddr) -> Self {
        PeerInfo { id, addr: Some(addr), account_id: None }
    }
}

impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(acc) = self.account_id.as_ref() {
            write!(f, "({}, {:?}, {})", self.id, self.addr, acc)
        } else {
            write!(f, "({}, {:?})", self.id, self.addr)
        }
    }
}

impl TryFrom<network_proto::PeerInfo> for PeerInfo {
    type Error = String;

    fn try_from(proto: network_proto::PeerInfo) -> Result<Self, Self::Error> {
        let addr = proto.addr.into_option().and_then(|s| s.value.parse::<SocketAddr>().ok());
        let account_id = proto.account_id.into_option().map(|s| s.value);
        Ok(PeerInfo { id: PublicKey::try_from(proto.id)?.into(), addr, account_id })
    }
}

impl From<PeerInfo> for network_proto::PeerInfo {
    fn from(peer_info: PeerInfo) -> network_proto::PeerInfo {
        let id = peer_info.id;
        let addr = SingularPtrField::from_option(
            peer_info.addr.map(|s| to_string_value(format!("{}", s))),
        );
        let account_id = SingularPtrField::from_option(peer_info.account_id.map(to_string_value));
        network_proto::PeerInfo { id: id.0.into(), addr, account_id, ..Default::default() }
    }
}

/// Peer chain information.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PeerChainInfo {
    /// Last known chain height of the peer.
    pub height: BlockIndex,
    /// Last known chain weight of the peer.
    pub total_weight: Weight,
}

impl TryFrom<network_proto::PeerChainInfo> for PeerChainInfo {
    type Error = String;

    fn try_from(proto: network_proto::PeerChainInfo) -> Result<Self, Self::Error> {
        Ok(PeerChainInfo { height: proto.height, total_weight: proto.total_weight.into() })
    }
}

impl From<PeerChainInfo> for network_proto::PeerChainInfo {
    fn from(chain_peer_info: PeerChainInfo) -> network_proto::PeerChainInfo {
        network_proto::PeerChainInfo {
            height: chain_peer_info.height,
            total_weight: chain_peer_info.total_weight.to_num(),
            ..Default::default()
        }
    }
}

/// Peer type.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PeerType {
    /// Inbound session
    Inbound,
    /// Outbound session
    Outbound,
}

/// Peer status.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PeerStatus {
    /// Waiting for handshake.
    Connecting,
    /// Ready to go.
    Ready,
    /// Banned, should shutdown this peer.
    Banned(ReasonForBan),
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Handshake {
    /// Protocol version.
    pub version: u32,
    /// Sender's peer id.
    pub peer_id: PeerId,
    /// Sender's account id, if present.
    pub account_id: Option<AccountId>,
    /// Sender's listening addr.
    pub listen_port: Option<u16>,
    /// Sender's information about known peers.
    pub peers_info: PeersInfo,
    /// Peer's chain information.
    pub chain_info: PeerChainInfo,
}

impl Handshake {
    pub fn new(
        peer_id: PeerId,
        account_id: Option<AccountId>,
        listen_port: Option<u16>,
        chain_info: PeerChainInfo,
    ) -> Self {
        Handshake {
            version: PROTOCOL_VERSION,
            peer_id,
            account_id,
            listen_port,
            peers_info: vec![],
            chain_info,
        }
    }
}

impl TryFrom<network_proto::Handshake> for Handshake {
    type Error = String;

    fn try_from(proto: network_proto::Handshake) -> Result<Self, Self::Error> {
        let account_id = proto.account_id.into_option().map(|s| s.value);
        let listen_port = proto.listen_port.into_option().map(|v| v.value as u16);
        let peers_info =
            proto.peers_info.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?;
        let peer_id: PublicKey = proto.peer_id.try_into().map_err(|e| format!("{}", e))?;
        let chain_info = proto_to_type(proto.chain_info)?;
        Ok(Handshake {
            version: proto.version,
            peer_id: peer_id.into(),
            account_id,
            listen_port,
            peers_info,
            chain_info,
        })
    }
}

impl From<Handshake> for network_proto::Handshake {
    fn from(handshake: Handshake) -> network_proto::Handshake {
        let account_id = SingularPtrField::from_option(handshake.account_id.map(to_string_value));
        let listen_port = SingularPtrField::from_option(handshake.listen_port.map(|v| {
            let mut res = UInt32Value::new();
            res.set_value(u32::from(v));
            res
        }));
        network_proto::Handshake {
            version: handshake.version,
            peer_id: handshake.peer_id.into(),
            peers_info: RepeatedField::from_iter(
                handshake.peers_info.into_iter().map(std::convert::Into::into),
            ),
            account_id,
            listen_port,
            chain_info: SingularPtrField::some(handshake.chain_info.into()),
            ..Default::default()
        }
    }
}

pub type PeersInfo = Vec<PeerInfo>;

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum PeerMessage {
    Handshake(Handshake),
    InfoGossip(PeersInfo),
    BlockAnnounce(Block),
    BlockHeaderAnnounce(BlockHeader),
    Transaction(SignedTransaction),
    BlockApproval(AccountId, CryptoHash, Signature),
}

impl fmt::Display for PeerMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PeerMessage::Handshake(_) => f.write_str("Handshake"),
            PeerMessage::InfoGossip(_) => f.write_str("InfoGossip"),
            PeerMessage::BlockAnnounce(_) => f.write_str("BlockAnnounce"),
            PeerMessage::BlockHeaderAnnounce(_) => f.write_str("BlockHeaderAnnounce"),
            PeerMessage::Transaction(_) => f.write_str("Transaction"),
            PeerMessage::BlockApproval(_, _, _) => f.write_str("BlockApproval"),
        }
    }
}

impl TryFrom<network_proto::PeerMessage> for PeerMessage {
    type Error = String;

    fn try_from(proto: network_proto::PeerMessage) -> Result<Self, Self::Error> {
        match proto.message_type {
            Some(network_proto::PeerMessage_oneof_message_type::hand_shake(hand_shake)) => {
                hand_shake.try_into().map(PeerMessage::Handshake)
            }
            Some(network_proto::PeerMessage_oneof_message_type::info_gossip(gossip)) => {
                let peer_info = gossip
                    .info_gossip
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PeerMessage::InfoGossip(peer_info))
            }
            Some(network_proto::PeerMessage_oneof_message_type::block_announce(block)) => {
                Ok(PeerMessage::BlockAnnounce(block.try_into()?))
            }
            Some(network_proto::PeerMessage_oneof_message_type::block_header_announce(header)) => {
                Ok(PeerMessage::BlockHeaderAnnounce(header.try_into()?))
            }
            Some(network_proto::PeerMessage_oneof_message_type::transaction(transaction)) => {
                Ok(PeerMessage::Transaction(transaction.try_into()?))
            }
            Some(network_proto::PeerMessage_oneof_message_type::block_approval(block_approval)) => {
                Ok(PeerMessage::BlockApproval(
                    block_approval.account_id,
                    block_approval.hash.try_into()?,
                    block_approval.signature.try_into()?,
                ))
            }
            None => unreachable!(),
        }
    }
}

impl From<PeerMessage> for network_proto::PeerMessage {
    fn from(message: PeerMessage) -> network_proto::PeerMessage {
        let message_type = match message {
            PeerMessage::Handshake(hand_shake) => {
                Some(network_proto::PeerMessage_oneof_message_type::hand_shake(hand_shake.into()))
            }
            PeerMessage::InfoGossip(peers_info) => {
                let gossip = network_proto::PeerInfoGossip {
                    info_gossip: RepeatedField::from_iter(
                        peers_info.into_iter().map(std::convert::Into::into),
                    ),
                    ..Default::default()
                };
                Some(network_proto::PeerMessage_oneof_message_type::info_gossip(gossip))
            }
            PeerMessage::BlockAnnounce(block) => {
                Some(network_proto::PeerMessage_oneof_message_type::block_announce(block.into()))
            }
            PeerMessage::BlockHeaderAnnounce(header) => Some(
                network_proto::PeerMessage_oneof_message_type::block_header_announce(header.into()),
            ),
            PeerMessage::Transaction(transaction) => {
                Some(network_proto::PeerMessage_oneof_message_type::transaction(transaction.into()))
            }
            PeerMessage::BlockApproval(account_id, hash, signature) => {
                let block_approval = network_proto::BlockApproval {
                    account_id,
                    hash: hash.into(),
                    signature: signature.into(),
                    ..Default::default()
                };
                Some(network_proto::PeerMessage_oneof_message_type::block_approval(block_approval))
            }
        };
        network_proto::PeerMessage { message_type, ..Default::default() }
    }
}

/// Configuration for the peer-to-peer manager.
pub struct NetworkConfig {
    pub public_key: PublicKey,
    pub private_key: SecretKey,
    pub account_id: Option<AccountId>,
    pub addr: Option<SocketAddr>,
    pub boot_nodes: Vec<PeerInfo>,
    pub handshake_timeout: Duration,
    pub reconnect_delay: Duration,
    pub bootstrap_peers_period: Duration,
    pub peer_max_count: u32,
}

/// Status of the known peers.
#[derive(Serialize, Deserialize, Eq, PartialEq)]
pub enum KnownPeerStatus {
    Unknown,
    NotConnected,
    Connected,
    Banned,
}

/// Information node stores about known peers.
#[derive(Serialize, Deserialize)]
pub struct KnownPeerState {
    pub peer_info: PeerInfo,
    pub status: KnownPeerStatus,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

impl KnownPeerState {
    pub fn new(peer_info: PeerInfo) -> Self {
        KnownPeerState {
            peer_info,
            status: KnownPeerStatus::Unknown,
            first_seen: Utc::now(),
            last_seen: Utc::now(),
        }
    }
}

/// Actor message that holds the TCP stream from an inbound TCP connection
#[derive(Message)]
pub struct InboundTcpConnect {
    /// Tcp stream of the inbound connections
    pub stream: TcpStream,
}

impl InboundTcpConnect {
    /// Method to create a new InboundTcpConnect message from a TCP stream
    pub fn new(stream: TcpStream) -> InboundTcpConnect {
        InboundTcpConnect { stream }
    }
}

/// Actor message to request the creation of an outbound TCP connection to a peer.
#[derive(Message)]
pub struct OutboundTcpConnect {
    /// Peer information of the outbound connection
    pub peer_info: PeerInfo,
}

#[derive(Message, Clone)]
pub struct SendMessage {
    pub message: PeerMessage,
}

/// Actor message to consolidate potential new peer.
/// Returns if connection should be kept or dropped.
pub struct Consolidate {
    pub actor: Recipient<SendMessage>,
    pub peer_info: PeerInfo,
    pub peer_type: PeerType,
    pub chain_info: PeerChainInfo,
}

impl Message for Consolidate {
    type Result = bool;
}

#[derive(Message)]
pub struct Unregister {
    pub peer_id: PeerId,
}

// Ban reason.
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum ReasonForBan {
    None = 0,
    BadBlock = 1,
    BadBlockHeader = 2,
    HeightFraud = 3,
    BadHandshake = 4,
    BadBlockApproval = 5,
}

#[derive(Message)]
pub struct Ban {
    pub peer_id: PeerId,
    pub ban_reason: ReasonForBan,
}

#[derive(Debug)]
pub enum NetworkRequests {
    FetchInfo,
    /// SEnds block announcement, when block was just produced.
    BlockAnnounce {
        block: Block,
    },
    /// Sends block header announcement, with possibly attaching approval for this block if
    /// participating in this epoch.
    BlockHeaderAnnounce {
        header: BlockHeader,
        approval: Option<BlockApproval>,
    },
    /// Request block with given hash from given peer.
    BlockRequest {
        hash: CryptoHash,
        peer_info: PeerInfo,
    },
    /// Request given block headers.
    BlockHeadersRequest {
        hashes: Vec<CryptoHash>,
    },
    /// Request given blocks.
    BlocksRequest {
        hashes: Vec<CryptoHash>,
    },
    /// Request state for given shard at given state root.
    StateRequest {
        shard_id: ShardId,
        state_root: MerkleHash,
    },
}

/// Combines peer address info and chain information.
#[derive(Debug, Clone)]
pub struct FullPeerInfo {
    pub peer_info: PeerInfo,
    pub chain_info: PeerChainInfo,
}

pub enum NetworkResponses {
    NoResponse,
    Info { num_active_peers: usize, peer_max_count: u32, most_weight_peers: Vec<FullPeerInfo> },
}

impl<A, M> MessageResponse<A, M> for NetworkResponses
where
    A: Actor,
    M: Message<Result = NetworkResponses>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

impl Message for NetworkRequests {
    type Result = NetworkResponses;
}

#[derive(Debug)]
pub enum NetworkClientMessages {
    /// Received transaction.
    Transaction(SignedTransaction),
    /// Received block header.
    BlockHeader(BlockHeader, PeerInfo),
    /// Received block, possibly requested.
    Block(Block, PeerInfo, bool),
    /// Received list of blocks for syncing.
    Blocks(Vec<Block>, PeerInfo),
    /// Get Chain information from Client.
    GetChainInfo,
    /// Block approval.
    BlockApproval(AccountId, CryptoHash, Signature),
}

pub enum NetworkClientResponses {
    /// No response.
    NoResponse,
    /// Ban peer for malicious behaviour.
    Ban { ban_reason: ReasonForBan },
    /// Chain information.
    ChainInfo { height: BlockIndex, total_weight: Weight },
}

impl<A, M> MessageResponse<A, M> for NetworkClientResponses
where
    A: Actor,
    M: Message<Result = NetworkClientResponses>,
{
    fn handle<R: ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

impl Message for NetworkClientMessages {
    type Result = NetworkClientResponses;
}
