use actix::{Actor, System};
use base64;
use futures::future;
use futures::future::Future;
use protobuf::Message;

use near_client::test_utils::setup_no_network;
use near_client::GetBlock;
use near_jsonrpc::client::new_client;
use near_jsonrpc::{start_http, RpcConfig};
use near_network::test_utils::{open_port, WaitOrTimeout};
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::test_utils::init_test_logger;
use near_primitives::transaction::TransactionBody;
use near_protos::signed_transaction as transaction_proto;

// TODO: move to another file
/// Test sending transaction via json rpc without waiting.
#[test]
fn test_send_tx() {
    init_test_logger();

    System::run(|| {
        let (client_addr, view_client_addr) =
            setup_no_network(vec!["test1", "test2"], "test1", true);

        let addr = format!("127.0.0.1:{}", open_port());
        start_http(RpcConfig::new(&addr), client_addr.clone(), view_client_addr.clone());

        let mut client = new_client(&format!("http://{}", addr));
        let signer = InMemorySigner::from_seed("test1", "test1");
        let tx = TransactionBody::send_money(1, "test1", "test2", 100).sign(&signer);
        let tx: transaction_proto::SignedTransaction = tx.into();
        actix::spawn(
            client
                .broadcast_tx_async(base64::encode(&tx.write_to_bytes().unwrap()))
                .map_err(|_| ())
                .map(|_| ()),
        );
        WaitOrTimeout::new(
            Box::new(move |_| {
                actix::spawn(view_client_addr.send(GetBlock::Best).then(move |res| {
                    let last_block = res.unwrap().unwrap();
                    // TODO: something better then checking that recent blocks have tx.
                    // if last_block.transactions.len() != 0 {
                    if last_block.header.height > 3 {
                        System::current().stop();
                    }
                    future::result(Ok(()))
                }))
            }),
            100,
            1000,
        )
        .start();
    })
    .unwrap();
}

/// Connect to json rpc and query it.
#[test]
fn test_query() {
    init_test_logger();

    System::run(|| {
        let (client_addr, view_client_addr) = setup_no_network(vec!["test"], "test1", true);

        let addr = format!("127.0.0.1:{}", open_port());
        start_http(RpcConfig::new(&addr), client_addr, view_client_addr);

        let mut client = new_client(&format!("http://{}", addr));
        actix::spawn(client.query("account/test".to_string(), vec![]).then(|res| {
            println!("{:?}", res);
            assert!(res.is_ok());
            System::current().stop();
            future::result(Ok(()))
        }));
    })
    .unwrap();
}