use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use actix::System;
use futures::{future, Future};

use near_chain::{Block, BlockApproval};
use near_client::test_utils::setup_mock;
use near_client::GetBlock;
use near_network::test_utils::wait_or_panic;
use near_network::types::{FullPeerInfo, PeerChainInfo};
use near_network::{NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo};
use primitives::crypto::signer::InMemorySigner;
use primitives::hash::hash;
use primitives::test_utils::init_test_logger;
use primitives::transaction::SignedTransaction;
use primitives::types::MerkleHash;

/// Runs block producing client and stops after network mock received two blocks.
#[test]
fn produce_two_blocks() {
    init_test_logger();
    System::run(|| {
        let count = Arc::new(AtomicUsize::new(0));
        setup_mock(
            vec!["test"],
            "test",
            true,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::BlockAnnounce { .. } = msg {
                    count.fetch_add(1, Ordering::Relaxed);
                    if count.load(Ordering::Relaxed) >= 2 {
                        System::current().stop();
                    }
                }
                NetworkResponses::NoResponse
            }),
        );
    })
    .unwrap();
}

/// Runs block producing client and sends it a transaction.
#[test]
fn produce_blocks_with_tx() {
    let count = Arc::new(AtomicUsize::new(0));
    init_test_logger();
    System::run(|| {
        let client = setup_mock(
            vec!["test"],
            "test",
            true,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::BlockAnnounce { block } = msg {
                    count.fetch_add(block.transactions.len(), Ordering::Relaxed);
                    if count.load(Ordering::Relaxed) >= 1 {
                        System::current().stop();
                    }
                }
                NetworkResponses::NoResponse
            }),
        );
        client.do_send(NetworkClientMessages::Transaction(SignedTransaction::empty()));
    })
    .unwrap();
}

/// Runs client that receives a block from network and announces header to the network with approval.
/// Need 3 block producers, to receive approval.
#[test]
fn receive_network_block() {
    init_test_logger();
    System::run(|| {
        let client = setup_mock(
            vec!["test2", "test1", "test3"],
            "test2",
            true,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::BlockHeaderAnnounce { approval, .. } = msg {
                    assert!(approval.is_some());
                    System::current().stop();
                }
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer = Arc::new(InMemorySigner::from_seed("test1", "test1"));
            let block = Block::produce(
                &last_block.header,
                last_block.header.height + 1,
                MerkleHash::default(),
                vec![],
                HashMap::default(),
                signer,
            );
            client.do_send(NetworkClientMessages::Block(block, PeerInfo::random(), false));
            future::result(Ok(()))
        }));
    })
    .unwrap();
}

/// Runs client that receives a block from network and announces header to the network.
#[test]
fn receive_network_block_header() {
    let block_holder: Arc<RwLock<Option<Block>>> = Arc::new(RwLock::new(None));
    init_test_logger();
    System::run(|| {
        let block_holder1 = block_holder.clone();
        let client = setup_mock(
            vec!["test"],
            "other",
            true,
            Box::new(move |msg, _ctx, client_addr| match msg {
                NetworkRequests::BlockRequest { hash, peer_info } => {
                    let block = block_holder1.read().unwrap().clone().unwrap();
                    assert_eq!(hash.clone(), block.hash());
                    actix::spawn(
                        client_addr
                            .send(NetworkClientMessages::Block(block, peer_info.clone(), false))
                            .then(|_| futures::future::ok(())),
                    );
                    NetworkResponses::NoResponse
                }
                NetworkRequests::BlockHeaderAnnounce { .. } => {
                    System::current().stop();
                    NetworkResponses::NoResponse
                }
                _ => NetworkResponses::NoResponse,
            }),
        );
        actix::spawn(client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer = Arc::new(InMemorySigner::from_seed("test", "test"));
            let block = Block::produce(
                &last_block.header,
                last_block.header.height + 1,
                MerkleHash::default(),
                vec![],
                HashMap::default(),
                signer,
            );
            client.do_send(NetworkClientMessages::BlockHeader(
                block.header.clone(),
                PeerInfo::random(),
            ));
            *block_holder.write().unwrap() = Some(block);
            future::result(Ok(()))
        }));
    })
    .unwrap();
}

/// Include approvals to the next block in newly produced block.
#[test]
fn produce_block_with_approvals() {
    init_test_logger();
    System::run(|| {
        let client = setup_mock(
            vec!["test3", "test1", "test2"],
            "test2",
            true,
            Box::new(move |msg, _ctx, _| {
                if let NetworkRequests::BlockAnnounce { block } = msg {
                    assert!(block.header.approval_sigs.len() > 0);
                    System::current().stop();
                }
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer1 = Arc::new(InMemorySigner::from_seed("test1", "test1"));
            let signer3 = Arc::new(InMemorySigner::from_seed("test3", "test3"));
            let block = Block::produce(
                &last_block.header,
                last_block.header.height + 1,
                MerkleHash::default(),
                vec![],
                HashMap::default(),
                signer1,
            );
            let block_approval = BlockApproval::new(block.hash(), &*signer3, "test2".to_string());
            client.do_send(NetworkClientMessages::Block(block, PeerInfo::random(), false));
            client.do_send(NetworkClientMessages::BlockApproval(
                "test3".to_string(),
                block_approval.hash,
                block_approval.signature,
            ));
            future::result(Ok(()))
        }));
    })
    .unwrap();
}

/// Sends 2 invalid blocks followed by valid block, and checks that client announces only valid block.
#[test]
fn invalid_blocks() {
    init_test_logger();
    System::run(|| {
        let client = setup_mock(
            vec!["test"],
            "other",
            false,
            Box::new(move |msg, _ctx, _client_actor| {
                match msg {
                    NetworkRequests::BlockHeaderAnnounce { header, approval } => {
                        assert_eq!(header.height, 1);
                        assert_eq!(header.prev_state_root, MerkleHash::default());
                        assert_eq!(*approval, None);
                        System::current().stop();
                    }
                    _ => {}
                };
                NetworkResponses::NoResponse
            }),
        );
        actix::spawn(client.send(GetBlock::Best).then(move |res| {
            let last_block = res.unwrap().unwrap();
            let signer = Arc::new(InMemorySigner::from_seed("test", "test"));
            // Send invalid state root.
            let block = Block::produce(
                &last_block.header,
                last_block.header.height + 1,
                hash(&[0]),
                vec![],
                HashMap::default(),
                signer.clone(),
            );
            client.do_send(NetworkClientMessages::Block(block.clone(), PeerInfo::random(), false));
            // Send block that builds on invalid one.
            let block2 = Block::produce(
                &block.header,
                block.header.height + 1,
                hash(&[1]),
                vec![],
                HashMap::default(),
                signer.clone(),
            );
            client.do_send(NetworkClientMessages::Block(block2, PeerInfo::random(), false));
            // Send proper block.
            let block3 = Block::produce(
                &last_block.header,
                last_block.header.height + 1,
                MerkleHash::default(),
                vec![],
                HashMap::default(),
                signer,
            );
            client.do_send(NetworkClientMessages::Block(block3, PeerInfo::random(), false));
            future::result(Ok(()))
        }));
        near_network::test_utils::wait_or_panic(5000);
    })
    .unwrap();
}

/// Runs two authority runtime with only one authority online.
/// Present authority produces blocks on it's height after deadline.
#[test]
fn skip_block_production() {
    init_test_logger();
    System::run(|| {
        setup_mock(
            vec!["test1", "test2"],
            "test2",
            false,
            Box::new(move |msg, _ctx, _client_actor| {
                match msg {
                    NetworkRequests::BlockAnnounce { block } => {
                        if block.header.height > 3 {
                            System::current().stop();
                        }
                    }
                    _ => {}
                };
                NetworkResponses::NoResponse
            }),
        );
        wait_or_panic(10000);
    })
    .unwrap();
}

/// Runs client that syncs with peers.
#[test]
fn client_sync() {
    init_test_logger();
    System::run(|| {
        let _client = setup_mock(
            vec!["test"],
            "other",
            false,
            Box::new(move |msg, _ctx, _client_actor| match msg {
                NetworkRequests::FetchInfo => {
//                    System::current().stop();
                    NetworkResponses::Info {
                        num_active_peers: 1,
                        peer_max_count: 1,
                        most_weight_peers: vec![FullPeerInfo {
                            peer_info: PeerInfo::random(),
                            chain_info: PeerChainInfo { height: 5, total_weight: 100.into() },
                        }],
                    }
                }
                // NetworkRequests::
                _ => NetworkResponses::NoResponse,
            }),
        );
        wait_or_panic(1000);
    })
    .unwrap();
}
