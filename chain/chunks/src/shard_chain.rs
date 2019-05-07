use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};

use near_primitives::hash::{hash, CryptoHash};
use near_primitives::sharding::EncodedShardChunk;
use near_primitives::types::AuthorityId;

use super::orchestrator::BaseOrchestrator;
use crate::shard_chain_chunks_mgr::{ShardChainChunkFetchError, ShardChainChunksManager};
use reed_solomon_erasure::Shard;

pub fn shard_chain_worker(
    me: AuthorityId,
    shard_id: u64,
    orchestrator: Arc<RwLock<impl BaseOrchestrator>>,

    finalized_chunk_rx: Receiver<(CryptoHash, u64, bool)>,
    candidate_chunk_rx: Receiver<(CryptoHash, CryptoHash)>,
    last_chunk_hash_bridge: Arc<RwLock<Option<(CryptoHash, u64)>>>,
    chunk_producer_bridge: Arc<RwLock<Option<EncodedShardChunk>>>,
    chunk_publishing_tx: Sender<EncodedShardChunk>,

    terminated: Arc<RwLock<bool>>,
) {
    let shard_genesis_hash: CryptoHash = hash(&[1u8]);

    let mut last_finalized_chunk_hash: CryptoHash = shard_genesis_hash;
    let mut last_height: u64 = 0;
    let mut is_syncing: bool = true;

    let mut current_chunk_hash: Option<CryptoHash> = None;
    let mut next_chunk: Option<EncodedShardChunk> = None;

    loop {
        {
            if *terminated.read().unwrap() {
                break;
            }
        }

        let mut work_done = false;

        if let Ok((new_finalized_chunk_hash, new_height, new_is_syncing)) =
            finalized_chunk_rx.try_recv()
        {
            last_finalized_chunk_hash = new_finalized_chunk_hash;
            last_height = new_height;
            is_syncing = new_is_syncing;

            let mut matches_next_prev = false;
            if let Some(next_chunk) = &next_chunk {
                if next_chunk.header.prev_chunk_hash == new_finalized_chunk_hash {
                    matches_next_prev = true;
                }
            }

            if matches_next_prev {
                current_chunk_hash = Some(next_chunk.as_ref().unwrap().chunk_hash());
                let _ = chunk_publishing_tx.send(next_chunk.unwrap());
                *last_chunk_hash_bridge.write().unwrap() =
                    Some((current_chunk_hash.unwrap(), last_height + 1));
            } else {
                current_chunk_hash = None;
                *last_chunk_hash_bridge.write().unwrap() =
                    Some((last_finalized_chunk_hash, last_height));
            }

            next_chunk = None;
            work_done = true;
        }

        if is_syncing {
            std::thread::yield_now();
            continue;
        }

        {
            let orchestrator = &*orchestrator.read().unwrap();

            if let Ok((parent_hash, current_hash)) = candidate_chunk_rx.try_recv() {
                // Ignore potential blocks for the next height if the current collator is a chunk producer for it
                if parent_hash == last_finalized_chunk_hash
                    && !orchestrator.is_shard_chunk_producer(me, shard_id, last_height + 1)
                    && current_chunk_hash.is_none()
                {
                    current_chunk_hash = Some(current_hash);
                    *last_chunk_hash_bridge.write().unwrap() =
                        Some((current_hash, last_height + 1));
                    work_done = true;
                }
            }

            if orchestrator.is_shard_chunk_producer(me, shard_id, last_height + 1)
                && current_chunk_hash.is_none()
            {
                let mut bridge_locked = chunk_producer_bridge.write().unwrap();
                let last_created_chunk = &mut *bridge_locked;
                if let Some(x) = last_created_chunk {
                    println!("Ready to produce at height {:?}", last_height);
                    if x.header.prev_chunk_hash == last_finalized_chunk_hash {
                        let current_chunk = last_created_chunk.take();
                        current_chunk_hash = Some(current_chunk.as_ref().unwrap().chunk_hash());
                        let _ = chunk_publishing_tx.send(current_chunk.unwrap());
                        *last_chunk_hash_bridge.write().unwrap() =
                            Some((current_chunk_hash.unwrap(), last_height + 1));
                        println!("Produced 1 by {:?}", me);
                    }
                    println!("Produced by {:?}", me);
                    *bridge_locked = None;
                    work_done = true;
                }
            }

            if orchestrator.is_shard_chunk_producer(me, shard_id, last_height + 2)
                && current_chunk_hash.is_some()
                && next_chunk.is_none()
            {
                let mut bridge_locked = chunk_producer_bridge.write().unwrap();
                let last_created_chunk = &mut *bridge_locked;
                if let Some(x) = last_created_chunk {
                    if x.header.prev_chunk_hash == current_chunk_hash.unwrap() {
                        next_chunk = last_created_chunk.take();
                    }
                    *last_chunk_hash_bridge.write().unwrap() = None;
                    *bridge_locked = None;
                    work_done = true;
                }
            }
        }

        if !work_done {
            std::thread::yield_now();
        }
    }
}

pub fn shard_chain_block_producer(
    me: AuthorityId,
    shard_id: u64,
    orchestrator: Arc<RwLock<impl BaseOrchestrator>>,

    chunks_mgr: Arc<RwLock<ShardChainChunksManager>>,

    last_chunk_hash_bridge: Arc<RwLock<Option<(CryptoHash, u64)>>>,
    chunk_producer_bridge: Arc<RwLock<Option<EncodedShardChunk>>>,

    terminated: Arc<RwLock<bool>>,
) {
    let shard_genesis_hash: CryptoHash = hash(&[1u8]);

    loop {
        {
            if *terminated.read().unwrap() {
                break;
            }
        }

        std::thread::yield_now();
        let last_chunk_hash_and_height = *last_chunk_hash_bridge.read().unwrap();

        if last_chunk_hash_and_height.is_none() {
            continue;
        }

        let (last_chunk_hash, last_height) = last_chunk_hash_and_height.unwrap();

        if last_chunk_hash == shard_genesis_hash {
            {
                let orchestrator = &*orchestrator.read().unwrap();
                if orchestrator.is_shard_chunk_producer(me, shard_id, last_height + 1) {
                    println!("YAY!");
                    let new_chunk = produce_chunk(orchestrator, last_chunk_hash, last_height);

                    *chunk_producer_bridge.write().unwrap() = Some(new_chunk);

                    {
                        // only overwrite the value of last_chunk_hash_bridge if it didn't change
                        let mut bridge_local = last_chunk_hash_bridge.write().unwrap();
                        if let Some((current_hash, _)) = *bridge_local {
                            if current_hash == last_chunk_hash {
                                *bridge_local = None;
                            }
                        }
                    }
                }
            }

            continue;
        }

        let mut need_to_request = false;

        {
            match chunks_mgr.read().unwrap().get_encoded_chunk(last_chunk_hash) {
                Ok(_) => {
                    let orchestrator = &*orchestrator.read().unwrap();
                    // TODO: some proper block production / previous blocks fetching needs to be happening here
                    if orchestrator.is_shard_chunk_producer(me, shard_id, last_height + 1) {
                        let new_chunk = produce_chunk(orchestrator, last_chunk_hash, last_height);

                        *chunk_producer_bridge.write().unwrap() = Some(new_chunk);

                        {
                            // only overwrite the value of last_chunk_hash_bridge if it didn't change
                            let mut bridge_local = last_chunk_hash_bridge.write().unwrap();
                            if let Some((current_hash, _)) = *bridge_local {
                                if current_hash == last_chunk_hash {
                                    *bridge_local = None;
                                }
                            }
                        }
                    }
                }
                Err(err) => match err {
                    ShardChainChunkFetchError::Unknown => {
                        need_to_request = true;
                    }
                    ShardChainChunkFetchError::NotReady => {}
                    ShardChainChunkFetchError::Failed => {
                        *last_chunk_hash_bridge.write().unwrap() = None;
                    }
                },
            }
        }

        if need_to_request {
            chunks_mgr.write().unwrap().request_fetch(last_chunk_hash)
        }
    }
}

fn produce_chunk(
    orchestrator: &impl BaseOrchestrator,
    last_chunk_hash: CryptoHash,
    last_height: u64,
) -> EncodedShardChunk {
    let mut parts: Vec<Option<Shard>> = vec![];
    let data_parts_num = orchestrator.get_data_chunk_parts_num();
    let parity_parts_num = orchestrator.get_total_chunk_parts_num() - data_parts_num;
    for i in 0..data_parts_num {
        parts.push(Some(Box::new([(i % 256) as u8; 16]) as Box<[u8]>));
    }
    for _i in 0..parity_parts_num {
        parts.push(None);
    }
    let new_chunk: EncodedShardChunk = EncodedShardChunk::from_parts_and_metadata(
        last_chunk_hash,
        last_height + 1,
        parts,
        data_parts_num,
        parity_parts_num,
    );
    new_chunk
}
