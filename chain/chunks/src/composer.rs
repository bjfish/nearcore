use crate::orchestrator::BaseOrchestrator;
use crate::shard_chain::{shard_chain_block_producer, shard_chain_worker};
use crate::shard_chain_chunks_mgr::{
    shard_chain_chunks_exchange_worker, shard_chain_chunks_mgr_worker, ShardChainChunksManager,
};
use crossbeam::ScopedJoinHandle;
use near_chain::Block;
use near_network::types::{ChunkHeaderAndPartMsg, ChunkHeaderMsg, ChunkPartMsg};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::AuthorityId;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, RwLock};
use std::thread;

pub struct Composer<Orchestrator: BaseOrchestrator> {
    shard_thread_handles: Vec<ScopedJoinHandle<()>>,
    terminated: Arc<RwLock<bool>>,
    pub connectors: HashMap<u64, ComposerShardConnector>,
    orchestrator: Arc<RwLock<Orchestrator>>,

    shard_chunk_headers: Vec<ShardChunkHeader>,
}

impl<Orchestrator: BaseOrchestrator> Composer<Orchestrator> {
    pub fn new(orchestrator: Orchestrator) -> Self {
        Composer {
            shard_thread_handles: vec![],
            terminated: Arc::new(RwLock::new(false)),
            connectors: HashMap::new(),
            orchestrator: Arc::new(RwLock::new(orchestrator)),

            shard_chunk_headers: vec![],
        }
    }
}

pub struct ComposerShardConnector {
    pub finalized_chunk_tx: Sender<(CryptoHash, u64, bool)>,
    pub targeted_chunk_header_request_tx: Sender<(AuthorityId, CryptoHash)>,
    pub targeted_chunk_part_request_tx: Sender<(AuthorityId, CryptoHash, u64)>,
    pub chunk_header_tx: Sender<ChunkHeaderMsg>,
    pub chunk_part_tx: Sender<ChunkPartMsg>,
    pub chunk_header_and_part_tx: Sender<ChunkHeaderAndPartMsg>,
}

impl<Orchestrator: BaseOrchestrator + Send + Sync> Composer<Orchestrator> {
    pub fn prepare_chunks(&mut self) -> Vec<ShardChunkHeader> {
        self.shard_chunk_headers.clone()
    }

    pub fn reconcile_block(&mut self, block: &Block, is_syncing: bool) {
        let height = block.header.height;
        self.shard_chunk_headers = block.chunks.clone();
        for (shard_id, connector) in self.connectors.iter() {
            let _ = connector.finalized_chunk_tx.send((
                block.chunks[*shard_id as usize].encoded_merkle_root,
                height,
                is_syncing,
            ));
        }
    }
}

fn composer_shard_connector_worker<Orchestrator: BaseOrchestrator + Send + Sync>(
    composer: Arc<RwLock<Composer<Orchestrator>>>,
    runtime: Arc<RwLock<Orchestrator>>,
    me: AuthorityId,
    shard_id: u64,
) {
    let terminated = {
        let composer = composer.write().unwrap();
        composer.terminated.clone()
    };

    crossbeam::scope(|scope| {
        let chunks_mgr = Arc::new(RwLock::new(ShardChainChunksManager::default()));
        let orchestrator = runtime;

        let (finalized_chunk_tx, finalized_chunk_rx) = channel();
        let (candidate_chunk_tx, candidate_chunk_rx) = channel();
        let (in_chunk_publishing_tx, in_chunk_publishing_rx) = channel();
        let (out_chunk_publishing_tx, out_chunk_publishing_rx) = channel();

        let (targeted_chunk_header_request_tx, targeted_chunk_header_request_rx) = channel();
        let (targeted_chunk_part_request_tx, targeted_chunk_part_request_rx) = channel();
        let (targeted_chunk_header_tx, _targeted_chunk_header_rx) = channel();
        let (targeted_chunk_part_tx, _targeted_chunk_part_rx) = channel();
        let (targeted_chunk_header_and_part_tx, _targeted_chunk_header_and_part_rx) = channel();

        let (chunk_header_request_tx, _chunk_header_request_rx) = channel();
        let (chunk_part_request_tx, _chunk_part_request_rx) = channel();
        let (chunk_header_tx, chunk_header_rx) = channel();
        let (chunk_part_tx, chunk_part_rx) = channel();
        let (chunk_header_and_part_tx, chunk_header_and_part_rx) = channel();

        let last_chunk_hash_bridge = Arc::new(RwLock::new(None));
        let chunk_producer_bridge = Arc::new(RwLock::new(None));

        let terminated1 = terminated.clone();
        let orchestrator1 = orchestrator.clone();
        let last_chunk_hash_bridge1 = last_chunk_hash_bridge.clone();
        let chunk_producer_bridge1 = chunk_producer_bridge.clone();
        let handle1 = scope.spawn(move || {
            shard_chain_worker(
                me,
                shard_id,
                orchestrator1,
                finalized_chunk_rx,
                candidate_chunk_rx,
                last_chunk_hash_bridge1,
                chunk_producer_bridge1,
                in_chunk_publishing_tx,
                terminated1,
            )
        });

        let terminated2 = terminated.clone();
        let orchestrator2 = orchestrator.clone();
        let chunks_mgr2 = chunks_mgr.clone();
        let last_chunk_hash_bridge2 = last_chunk_hash_bridge.clone();
        let chunk_producer_bridge2 = chunk_producer_bridge.clone();
        let handle2 = scope.spawn(move || {
            shard_chain_block_producer(
                me,
                shard_id,
                orchestrator2,
                chunks_mgr2,
                last_chunk_hash_bridge2,
                chunk_producer_bridge2,
                terminated2,
            )
        });

        let chunks_mgr3 = chunks_mgr.clone();
        let orchestrator3 = orchestrator.clone();
        let targeted_chunk_part_tx3 = targeted_chunk_part_tx.clone();
        let terminated3 = terminated.clone();
        let handle3 = scope.spawn(move || {
            shard_chain_chunks_exchange_worker(
                chunks_mgr3,
                orchestrator3,
                targeted_chunk_header_request_rx,
                targeted_chunk_part_request_rx,
                targeted_chunk_header_tx,
                targeted_chunk_part_tx3,
                targeted_chunk_header_and_part_tx,
                out_chunk_publishing_rx,
                terminated3,
            )
        });

        let chunks_mgr4 = chunks_mgr.clone();
        let orchestrator4 = orchestrator.clone();
        let terminated4 = terminated.clone();
        let handle4 = scope.spawn(move || {
            shard_chain_chunks_mgr_worker(
                chunks_mgr4,
                orchestrator4,
                chunk_header_request_tx,
                chunk_part_request_tx,
                targeted_chunk_part_tx,
                chunk_header_rx,
                chunk_part_rx,
                chunk_header_and_part_rx,
                candidate_chunk_tx,
                terminated4,
            )
        });

        {
            let connector = ComposerShardConnector {
                targeted_chunk_header_request_tx,
                targeted_chunk_part_request_tx,
                chunk_header_tx,
                chunk_part_tx,
                chunk_header_and_part_tx,
                finalized_chunk_tx,
            };

            let mut composer = composer.write().unwrap();
            composer.connectors.insert(shard_id, connector);
        }

        loop {
            {
                if *terminated.read().unwrap() {
                    break;
                }
            }

            let mut work_done = false;

            for chunk in in_chunk_publishing_rx.try_iter() {
                let mut composer = composer.write().unwrap();
                if chunk.header.prev_chunk_hash
                    == composer.shard_chunk_headers[shard_id as usize].encoded_merkle_root
                {
                    composer.shard_chunk_headers[shard_id as usize] = chunk.header.clone();
                }
                let _ = out_chunk_publishing_tx.send(chunk);
                work_done = true;
            }

            if !work_done {
                thread::yield_now();
            }
        }

        let _ = handle1.join();
        let _ = handle2.join();
        let _ = handle3.join();
        let _ = handle4.join();
    });
}
