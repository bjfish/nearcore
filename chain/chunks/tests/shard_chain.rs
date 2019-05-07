use near_chunks::orchestrator::BaseOrchestrator;
use near_chunks::shard_chain::{shard_chain_block_producer, shard_chain_worker};
use near_chunks::shard_chain_chunks_mgr::{
    shard_chain_chunks_exchange_worker, shard_chain_chunks_mgr_worker, ShardChainChunksManager,
};
use near_network::types::{ChunkHeaderAndPartMsg, ChunkHeaderMsg, ChunkPartMsg};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::sharding::EncodedShardChunk;
use near_primitives::types::AuthorityId;
use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, RwLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

struct TestOrchestrator {
    n_producers: u64,
}

impl TestOrchestrator {
    pub fn new(n_producers: u64) -> Self {
        Self { n_producers }
    }
}

impl BaseOrchestrator for TestOrchestrator {
    fn is_shard_chunk_producer(
        &self,
        authority_id: AuthorityId,
        _shard_id: u64,
        height: u64,
    ) -> bool {
        height % self.n_producers == (1 + authority_id as u64) % self.n_producers
    }
    fn is_block_producer(&self, _authority_id: AuthorityId, _height: u64) -> bool {
        false
    }

    fn get_authority_id_for_part(&self, part_id: usize) -> AuthorityId {
        return part_id % (self.n_producers as usize);
    }

    fn get_total_chunk_parts_num(&self) -> usize {
        if self.n_producers == 1 {
            2
        } else {
            self.n_producers as usize
        }
    }
    fn get_data_chunk_parts_num(&self) -> usize {
        (self.n_producers as usize + 1) / 2
    }
    fn get_num_shards(&self) -> usize {
        unreachable!()
    }
}

fn wait_for_n<T>(receiver: &Receiver<T>, num: usize, timeout_ms: u64) -> Vec<T> {
    let mut ret = vec![];
    for _i in 0..num {
        ret.push(
            receiver
                .recv_timeout(Duration::from_millis(timeout_ms))
                .expect("wait_for_n gave up on waiting"),
        );
    }
    return ret;
}

enum WhaleCunksTransmittingMode {
    NoBroadcast, // the chunks are not broadcasted, and no chunks manager worker spawned
    Broadcast(
        Sender<CryptoHash>,
        Sender<(CryptoHash, u64)>,
        Receiver<ChunkHeaderMsg>,
        Receiver<ChunkPartMsg>,
    ), // the mgr worker spawned, but no serving worker
    Serve(
        Sender<CryptoHash>,
        Sender<(CryptoHash, u64)>,
        Sender<(AuthorityId, ChunkHeaderMsg)>,
        Sender<(AuthorityId, ChunkPartMsg)>,
        Sender<(AuthorityId, ChunkHeaderAndPartMsg)>,
        Receiver<(AuthorityId, CryptoHash)>,
        Receiver<(AuthorityId, CryptoHash, u64)>,
        Receiver<ChunkHeaderMsg>,
        Receiver<ChunkPartMsg>,
        Receiver<ChunkHeaderAndPartMsg>,
        Receiver<EncodedShardChunk>,
    ), // the worker spawned
}

fn spawn_whale(
    me: AuthorityId,
    shard_id: u64,
    transmitting_mode: WhaleCunksTransmittingMode,
    orchestrator: &Arc<RwLock<impl BaseOrchestrator + 'static>>,
    finalized_chunk_rx: Receiver<(CryptoHash, u64, bool)>,
    chunk_publishing_tx: Sender<EncodedShardChunk>,
    terminated: &Arc<RwLock<bool>>,
    chunks_mgr: Option<Arc<RwLock<ShardChainChunksManager>>>,
) -> (JoinHandle<()>, JoinHandle<()>, JoinHandle<()>, JoinHandle<()>) {
    let chunks_mgr = match chunks_mgr {
        Some(x) => x,
        None => Arc::new(RwLock::new(ShardChainChunksManager::default())),
    };

    let (candidate_chunk_tx, candidate_chunk_rx) = channel();

    let orchestrator1 = orchestrator.clone();
    let last_chunk_hash_bridge = Arc::new(RwLock::new(None));
    let chunk_producer_bridge = Arc::new(RwLock::new(None));
    let last_chunk_hash_bridge1 = last_chunk_hash_bridge.clone();
    let chunk_producer_bridge1 = chunk_producer_bridge.clone();
    let terminated1 = terminated.clone();
    let t1 = thread::spawn(move || {
        shard_chain_worker(
            me,
            shard_id,
            orchestrator1,
            finalized_chunk_rx,
            candidate_chunk_rx,
            last_chunk_hash_bridge1,
            chunk_producer_bridge1,
            chunk_publishing_tx,
            terminated1,
        )
    });
    let orchestrator2 = orchestrator.clone();
    let last_chunk_hash_bridge2 = last_chunk_hash_bridge.clone();
    let chunk_producer_bridge2 = chunk_producer_bridge.clone();
    let chunks_mgr2 = chunks_mgr.clone();
    let terminated2 = terminated.clone();
    let t2 = thread::spawn(move || {
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

    let (t3, t4) = match transmitting_mode {
        WhaleCunksTransmittingMode::NoBroadcast => {
            (thread::spawn(move || {}), thread::spawn(move || {}))
        }
        WhaleCunksTransmittingMode::Broadcast(
            chunk_header_request_tx,
            chunk_part_request_tx,
            chunk_header_rx,
            chunk_part_rx,
        ) => {
            let chunks_mgr3 = chunks_mgr.clone();
            let orchestrator3 = orchestrator.clone();
            let terminated3 = terminated.clone();

            let (_, unused_rx) = channel();
            let (unused_tx, _) = channel();
            (
                thread::spawn(move || {
                    shard_chain_chunks_mgr_worker(
                        chunks_mgr3,
                        orchestrator3,
                        chunk_header_request_tx,
                        chunk_part_request_tx,
                        unused_tx,
                        chunk_header_rx,
                        chunk_part_rx,
                        unused_rx,
                        candidate_chunk_tx,
                        terminated3,
                    )
                }),
                thread::spawn(move || {}),
            )
        }
        WhaleCunksTransmittingMode::Serve(
            chunk_header_request_tx,
            chunk_part_request_tx,
            chunk_header_tx,
            chunk_part_tx,
            chunk_header_and_part_tx,
            chunk_header_request_rx,
            chunk_part_request_rx,
            chunk_header_rx,
            chunk_part_rx,
            chunk_header_and_part_rx,
            chunk_publishing_rx,
        ) => {
            let chunks_mgr3 = chunks_mgr.clone();
            let orchestrator3 = orchestrator.clone();
            let terminated3 = terminated.clone();

            let chunks_mgr4 = chunks_mgr.clone();
            let orchestrator4 = orchestrator.clone();
            let terminated4 = terminated.clone();

            let chunk_part_tx4 = chunk_part_tx.clone();

            (
                thread::spawn(move || {
                    shard_chain_chunks_mgr_worker(
                        chunks_mgr3,
                        orchestrator3,
                        chunk_header_request_tx,
                        chunk_part_request_tx,
                        chunk_part_tx4,
                        chunk_header_rx,
                        chunk_part_rx,
                        chunk_header_and_part_rx,
                        candidate_chunk_tx,
                        terminated3,
                    )
                }),
                thread::spawn(move || {
                    shard_chain_chunks_exchange_worker(
                        chunks_mgr4,
                        orchestrator4,
                        chunk_header_request_rx,
                        chunk_part_request_rx,
                        chunk_header_tx,
                        chunk_part_tx,
                        chunk_header_and_part_tx,
                        chunk_publishing_rx,
                        terminated4,
                    )
                }),
            )
        }
    };

    (t1, t2, t3, t4)
}

fn test_multiple_workers_no_serving_common(num_whales: u64, num_offline: u64) {
    let terminated = Arc::new(RwLock::new(false));
    let orchestrator = Arc::new(RwLock::new(TestOrchestrator::new(num_whales)));

    let mut chunk_publishing_rxs = vec![];
    let mut finalized_chunk_txs = vec![];

    let mut chunk_header_request_rxs = vec![];
    let mut chunk_part_request_rxs = vec![];
    let mut chunk_header_txs = vec![];
    let mut chunk_part_txs = vec![];

    let genesis_hash = hash(&[1u8]);

    let mut join_handles = vec![];

    for me in 0..(num_whales - num_offline) {
        let (finalized_chunk_tx, finalized_chunk_rx) = channel();
        let (chunk_publishing_tx, chunk_publishing_rx) = channel();

        let (chunk_header_request_tx, chunk_header_request_rx) = channel();
        let (chunk_part_request_tx, chunk_part_request_rx) = channel();
        let (chunk_header_tx, chunk_header_rx) = channel();
        let (chunk_part_tx, chunk_part_rx) = channel();

        let (t1, t2, t3, t4) = spawn_whale(
            me as AuthorityId,
            7,
            WhaleCunksTransmittingMode::Broadcast(
                chunk_header_request_tx,
                chunk_part_request_tx,
                chunk_header_rx,
                chunk_part_rx,
            ),
            &orchestrator,
            finalized_chunk_rx,
            chunk_publishing_tx,
            &terminated,
            None,
        );

        finalized_chunk_tx.send((genesis_hash, 0, false)).unwrap();

        join_handles.push(t1);
        join_handles.push(t2);
        join_handles.push(t3);
        join_handles.push(t4);

        chunk_publishing_rxs.push(chunk_publishing_rx);
        finalized_chunk_txs.push(finalized_chunk_tx);

        chunk_header_request_rxs.push(chunk_header_request_rx);
        chunk_part_request_rxs.push(chunk_part_request_rx);
        chunk_header_txs.push(chunk_header_tx);
        chunk_part_txs.push(chunk_part_tx);
    }

    let chunks: Arc<RwLock<HashMap<CryptoHash, EncodedShardChunk>>> =
        Arc::new(RwLock::new(HashMap::new()));

    let terminated2 = terminated.clone();
    let chunks2 = chunks.clone();

    let broadcast_thread = thread::spawn(move || {
        let mut header_requests = vec![];
        let mut part_requests = vec![];
        loop {
            if *terminated2.read().unwrap() {
                break;
            }

            let mut work_done = false;
            {
                for (chunk_header_request_rx, chunk_header_tx) in
                    chunk_header_request_rxs.iter().zip(chunk_header_txs.iter())
                {
                    for hash in chunk_header_request_rx.try_iter() {
                        header_requests.push((chunk_header_tx, hash));
                        work_done = true;
                    }
                }

                for (chunk_part_request_rx, chunk_part_tx) in
                    chunk_part_request_rxs.iter().zip(chunk_part_txs.iter())
                {
                    for (hash, part_id) in chunk_part_request_rx.try_iter() {
                        part_requests.push((chunk_part_tx, hash, part_id));
                        work_done = true;
                    }
                }

                let mut new_header_requests = vec![];
                let chunks2 = chunks2.read().unwrap();
                for (sender, hash) in header_requests.drain(..) {
                    if let Some(chunk) = chunks2.get(&hash) {
                        let _ = sender.send(ChunkHeaderMsg {
                            chunk_hash: hash,
                            header: chunk.header.clone(),
                        });
                        work_done = true;
                    } else {
                        new_header_requests.push((sender, hash));
                    }
                }
                header_requests = new_header_requests;

                let mut new_part_requests = vec![];
                for (sender, hash, part_id) in part_requests.drain(..) {
                    if let Some(chunk) = chunks2.get(&hash) {
                        if part_id < num_whales - num_offline {
                            let _ = sender.send(ChunkPartMsg {
                                chunk_hash: hash,
                                part_id,
                                part: chunk.content.parts[part_id as usize]
                                    .as_ref()
                                    .unwrap()
                                    .clone(),
                            });
                        }
                        work_done = true;
                    } else {
                        new_part_requests.push((sender, hash, part_id));
                    }
                }
                part_requests = new_part_requests;
            }

            if !work_done {
                std::thread::yield_now();
            }
        }
    });

    let mut height = 1;
    // TODO: reintroduce the last_hash_and_height once optimistic chunk creation works
    //let mut last_hash_and_height = None;
    for chunk_publishing_rx in chunk_publishing_rxs.iter() {
        println!("!!!");
        let mut published_chunks = wait_for_n(chunk_publishing_rx, 1, 1000);
        let chunk_hash = published_chunks[0].chunk_hash();
        chunks.write().unwrap().insert(chunk_hash, published_chunks.pop().unwrap());

        for finalized_chunk_tx in finalized_chunk_txs.iter() {
            finalized_chunk_tx.send((chunk_hash, height, false)).unwrap();
        }
        /*last_hash_and_height = match last_hash_and_height {
            None => Some((chunk_hash, height)),
            Some((last_hash, last_height)) => {
                for finalized_chunk_tx in finalized_chunk_txs.iter() {
                    println!("Inserted chunk {:?}", last_hash);
                    finalized_chunk_tx.send((last_hash, last_height, false)).unwrap();
                }
                for finalized_chunk_tx in finalized_chunk_txs.iter() {
                    finalized_chunk_tx.send((chunk_hash, height, false)).unwrap();
                }
                None
            }
        };*/
        height += 1;
    }

    *terminated.write().unwrap() = true;

    for handle in join_handles {
        handle.join().unwrap();
    }
    broadcast_thread.join().unwrap();
}

fn test_multiple_workers_with_serving_common(num_whales: u64, num_offline: u64) {
    let terminated = Arc::new(RwLock::new(false));
    let orchestrator = Arc::new(RwLock::new(TestOrchestrator::new(num_whales)));

    let mut chunk_publishing_rxs = vec![];
    let mut chunk_publishing_txs = vec![];
    let mut finalized_chunk_txs = vec![];

    let mut chunk_header_request_rxs = vec![];
    let mut chunk_part_request_rxs = vec![];
    let mut chunk_header_txs = vec![];
    let mut chunk_part_txs = vec![];
    let mut chunk_header_and_part_txs = vec![];

    let mut targeted_chunk_header_request_txs = vec![];
    let mut targeted_chunk_part_request_txs = vec![];
    let mut targeted_chunk_header_rxs = vec![];
    let mut targeted_chunk_part_rxs = vec![];
    let mut targeted_chunk_header_and_part_rxs = vec![];

    let genesis_hash = hash(&[1u8]);

    let mut join_handles = vec![];

    for me in 0..(num_whales - num_offline) {
        let (finalized_chunk_tx, finalized_chunk_rx) = channel();
        let (out_chunk_publishing_tx, out_chunk_publishing_rx) = channel();
        let (in_chunk_publishing_tx, in_chunk_publishing_rx) = channel();

        let (chunk_header_request_tx, chunk_header_request_rx) = channel();
        let (chunk_part_request_tx, chunk_part_request_rx) = channel();
        let (chunk_header_tx, chunk_header_rx) = channel();
        let (chunk_part_tx, chunk_part_rx) = channel();
        let (chunk_header_and_part_tx, chunk_header_and_part_rx) = channel();

        let (targeted_chunk_header_request_tx, targeted_chunk_header_request_rx) = channel();
        let (targeted_chunk_part_request_tx, targeted_chunk_part_request_rx) = channel();
        let (targeted_chunk_header_tx, targeted_chunk_header_rx) = channel();
        let (targeted_chunk_part_tx, targeted_chunk_part_rx) = channel();
        let (targeted_chunk_header_and_part_tx, targeted_chunk_header_and_part_rx) = channel();

        let (t1, t2, t3, t4) = spawn_whale(
            me as AuthorityId,
            7,
            WhaleCunksTransmittingMode::Serve(
                chunk_header_request_tx,
                chunk_part_request_tx,
                targeted_chunk_header_tx,
                targeted_chunk_part_tx,
                targeted_chunk_header_and_part_tx,
                targeted_chunk_header_request_rx,
                targeted_chunk_part_request_rx,
                chunk_header_rx,
                chunk_part_rx,
                chunk_header_and_part_rx,
                in_chunk_publishing_rx,
            ),
            &orchestrator,
            finalized_chunk_rx,
            out_chunk_publishing_tx,
            &terminated,
            None,
        );

        finalized_chunk_tx.send((genesis_hash, 0, false)).unwrap();

        join_handles.push(t1);
        join_handles.push(t2);
        join_handles.push(t3);
        join_handles.push(t4);

        chunk_publishing_rxs.push(out_chunk_publishing_rx);
        chunk_publishing_txs.push(in_chunk_publishing_tx);
        finalized_chunk_txs.push(finalized_chunk_tx);

        chunk_header_request_rxs.push(chunk_header_request_rx);
        chunk_part_request_rxs.push(chunk_part_request_rx);
        chunk_header_txs.push(chunk_header_tx);
        chunk_part_txs.push(chunk_part_tx);
        chunk_header_and_part_txs.push(chunk_header_and_part_tx);

        targeted_chunk_header_request_txs.push(targeted_chunk_header_request_tx);
        targeted_chunk_part_request_txs.push(targeted_chunk_part_request_tx);
        targeted_chunk_header_rxs.push(targeted_chunk_header_rx);
        targeted_chunk_part_rxs.push(targeted_chunk_part_rx);
        targeted_chunk_header_and_part_rxs.push(targeted_chunk_header_and_part_rx);
    }

    let terminated2 = terminated.clone();

    let broadcast_thread = thread::spawn(move || loop {
        if *terminated2.read().unwrap() {
            break;
        }

        let mut work_done = false;
        {
            for (who, chunk_header_request_rx) in chunk_header_request_rxs.iter().enumerate() {
                for hash in chunk_header_request_rx.try_iter() {
                    for whom in 0..num_whales - num_offline {
                        let _ = targeted_chunk_header_request_txs[whom as usize].send((who, hash));
                    }
                    work_done = true;
                }
            }

            for (who, chunk_part_request_rx) in chunk_part_request_rxs.iter().enumerate() {
                for (hash, part_id) in chunk_part_request_rx.try_iter() {
                    let orchestrator = &*orchestrator.read().unwrap();
                    let whom = orchestrator.get_authority_id_for_part(part_id as usize);
                    if (whom as u64) < num_whales - num_offline {
                        let _ = targeted_chunk_part_request_txs[whom as usize]
                            .send((who, hash, part_id));
                    }
                    work_done = true;
                }
            }

            for targeted_chunk_header_rx in targeted_chunk_header_rxs.iter() {
                for (whom, header) in targeted_chunk_header_rx.try_iter() {
                    if (whom as u64) < num_whales - num_offline {
                        let _ = chunk_header_txs[whom as usize].send(header);
                    }
                    work_done = true;
                }
            }

            for targeted_chunk_part_rx in targeted_chunk_part_rxs.iter() {
                for (whom, part) in targeted_chunk_part_rx.try_iter() {
                    if (whom as u64) < num_whales - num_offline {
                        let _ = chunk_part_txs[whom as usize].send(part);
                    }
                    work_done = true;
                }
            }

            for targeted_chunk_header_and_part_rx in targeted_chunk_header_and_part_rxs.iter() {
                for (whom, header_and_part) in targeted_chunk_header_and_part_rx.try_iter() {
                    if (whom as u64) < num_whales - num_offline {
                        let _ = chunk_header_and_part_txs[whom as usize].send(header_and_part);
                    }
                    work_done = true;
                }
            }
        }

        if !work_done {
            std::thread::yield_now();
        }
    });

    let mut height = 1;
    for _iter in 0..3 {
        let mut last_chunk_hash = Default::default();
        for (chunk_publishing_rx, chunk_publishing_tx) in
            chunk_publishing_rxs.iter().zip(chunk_publishing_txs.iter())
        {
            println!("!!!");
            let mut published_chunks = wait_for_n(chunk_publishing_rx, 1, 1000);
            let chunk_hash = published_chunks[0].chunk_hash();
            last_chunk_hash = chunk_hash;
            chunk_publishing_tx.send(published_chunks.pop().unwrap()).unwrap();

            //std::thread::sleep(Duration::from_millis(50));
            for finalized_chunk_tx in finalized_chunk_txs.iter() {
                finalized_chunk_tx.send((chunk_hash, height, false)).unwrap();
            }
            height += 1;
        }

        for _i in 0..num_offline {
            for finalized_chunk_tx in finalized_chunk_txs.iter() {
                finalized_chunk_tx.send((last_chunk_hash, height, false)).unwrap();
            }
            height += 1;
        }
    }

    *terminated.write().unwrap() = true;

    for handle in join_handles {
        handle.join().unwrap();
    }
    broadcast_thread.join().unwrap();
}

#[test]
fn test_single_worker() {
    let (finalized_chunk_tx, finalized_chunk_rx) = channel();
    let (chunk_publishing_tx, chunk_publishing_rx) = channel();

    let terminated = Arc::new(RwLock::new(false));
    let chunks_mgr = Arc::new(RwLock::new(ShardChainChunksManager::default()));
    let orchestrator = Arc::new(RwLock::new(TestOrchestrator::new(1)));

    let genesis_hash = hash(&[1u8]);

    let (t1, t2, t3, t4) = spawn_whale(
        0,
        7,
        WhaleCunksTransmittingMode::NoBroadcast,
        &orchestrator,
        finalized_chunk_rx,
        chunk_publishing_tx,
        &terminated,
        Some(chunks_mgr.clone()),
    );

    finalized_chunk_tx.send((genesis_hash, 0, false)).unwrap();

    let mut blocks = wait_for_n(&chunk_publishing_rx, 1, 1000);
    assert_eq!(blocks[0].header.prev_chunk_hash, genesis_hash);

    let first_block_hash = blocks[0].chunk_hash();
    chunks_mgr.write().unwrap().insert_chunk(first_block_hash, blocks.pop().unwrap());
    finalized_chunk_tx.send((first_block_hash, 1, false)).unwrap();

    let blocks = wait_for_n(&chunk_publishing_rx, 1, 1000);
    assert_eq!(blocks[0].header.prev_chunk_hash, first_block_hash);

    *terminated.write().unwrap() = true;

    t1.join().unwrap();
    t2.join().unwrap();
    t3.join().unwrap();
    t4.join().unwrap();
}

#[test]
fn test_multiple_workers_no_serving() {
    for i in 0..10 {
        println!("==== ITERATION {:?}.1 ====", i);
        test_multiple_workers_no_serving_common(10, 0);
        println!("==== ITERATION {:?}.2 ====", i);
        test_multiple_workers_no_serving_common(10, 5);
    }
}

#[test]
#[should_panic]
fn test_multiple_workers_no_serving_no_data_availability() {
    test_multiple_workers_no_serving_common(10, 6);
}

#[test]
fn test_multiple_workers_with_serving() {
    for i in 0..10 {
        println!("==== ITERATION {:?}.1 ====", i);
        test_multiple_workers_with_serving_common(10, 0);
        println!("==== ITERATION {:?}.2 ====", i);
        test_multiple_workers_with_serving_common(10, 5);
    }
}

#[test]
#[should_panic]
fn test_multiple_workers_with_serving_no_data_availability() {
    test_multiple_workers_with_serving_common(10, 7);
}
