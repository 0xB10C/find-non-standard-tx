use bitcoin_pool_identification::{parse_json, PoolIdentification, DEFAULT_MAINNET_POOL_LIST};
use bitcoincore_rpc::bitcoin::{Amount, Block, Network, Txid};
use bitcoincore_rpc::{Auth, Client, RpcApi};
use config::Config;
use csv::Writer;
use std::thread;
use std::time::Duration;

const DUPLICATE_BLOCK_ERROR: &str = "\"duplicate\"";
const TX_ALREADY_IN_MEMPOOL_REJECTION_REASON: &str = "txn-already-in-mempool";
const RPC_RETRY_TIME: Duration = Duration::from_secs(5);

fn rpc_client(settings: &Config, node: &str) -> Client {
    Client::new(
        &format!(
            "{}:{}",
            settings
                .get::<String>(&format!("nodes.{}.rpc_host", node))
                .expect(&format!("need a rpc_host for the {} node", node)),
            settings
                .get::<u16>(&format!("nodes.{}.rpc_port", node))
                .expect(&format!("need a rpc_port for the {} node", node)),
        ),
        Auth::UserPass(
            settings
                .get::<String>(&format!("nodes.{}.rpc_user", node))
                .expect(&format!("need a rpc_user for the {} node", node)),
            settings
                .get::<String>(&format!("nodes.{}.rpc_pass", node))
                .expect(&format!("need a rpc_pass for the {} node", node)),
        ),
    )
    .unwrap()
}

#[derive(Debug, serde::Serialize)]
struct ResultRow {
    height: u64,
    miner: String,
    reject_reason: String,
    txid: Txid,
    vsize: usize,
    inputs: usize,
    outputs: usize,
}

fn main() {
    let settings = Config::builder()
        .add_source(config::File::with_name("config.toml"))
        .build()
        .unwrap();

    // We need two nodes. One node that can give us data about blocks (could
    // also be a block explorer API) and a node that we submit transactions
    // to and which tells us if the transaction is standard or is being
    // rejected as non-standard.
    // The data node and the test node.
    let data_node = rpc_client(&settings, "data");
    let test_node = rpc_client(&settings, "test");

    let test_node_height = test_node.get_block_count().unwrap();
    println!("The test node is at height {}", test_node_height);
    let start_height = test_node_height + 1;
    println!(
        "Starting to collect non-standard transactions at height {}",
        start_height
    );

    let output_file = settings
        .get::<String>("output")
        .expect("No 'output' defined in the configuration");
    let mut wtr = Writer::from_path(output_file.clone())
        .expect(&format!("Can't open output file {}", output_file));

    let pools = parse_json(DEFAULT_MAINNET_POOL_LIST);

    let mut current_height = start_height;
    while current_height <= data_node.get_block_count().unwrap() {
        let block_hash = data_node.get_block_hash(current_height).unwrap();
        let block = data_node.get_block(&block_hash).unwrap();

        let pool_name = match block.identify_pool(Network::Bitcoin, &pools) {
            Some(result) => result.pool.name,
            None => "Unknown".to_string(),
        };

        let mut csv_rows = vec![];
        for tx in block.txdata.iter() {
            if tx.is_coinbase() {
                continue;
            }

            let results = test_node.test_mempool_accept(&[tx]).unwrap();
            let result = results.first().unwrap();

            if !result.allowed {
                // If a previously aborted run left transactions in the mempool,
                // a transaction will be rejected for already being in the mempool.
                // We don't care about these cases.
                let reject_reason = result.reject_reason.clone().unwrap();
                if reject_reason == TX_ALREADY_IN_MEMPOOL_REJECTION_REASON {
                    continue;
                }

                // When using -stopatheight=X, Bitcoin Core might already know
                // about blocks at a height >X. In this case, transactions are
                // rejected because they are "already known" (as the blocks
                // are already known). We don't care about these cases and
                // filter them out when we receive an error on submitblock.
                csv_rows.push(ResultRow {
                    height: current_height,
                    miner: pool_name.clone(),
                    txid: tx.txid(),
                    reject_reason,
                    vsize: tx.vsize(),
                    inputs: tx.input.len(),
                    outputs: tx.output.len(),
                });
            } else {
                loop {
                    match test_node.send_raw_transaction(
                        tx,
                        Some(Amount::MAX_MONEY),
                        Some(Amount::MAX_MONEY),
                    ) {
                        Ok(_) => break,
                        Err(e) => match e {
                            bitcoincore_rpc::Error::JsonRpc(e) => {
                                // If we are sending blocks and transactions too fast, Bitcoin Core
                                // RPC server receive buffer might fill up and we receive a transport
                                // error: Resource temporarily unavailable.
                                match e {
                                    bitcoincore_rpc::jsonrpc::Error::Transport(e) => {
                                        println!(
                                            "Transport error while sending raw transaction: {}",
                                            e
                                        );
                                        println!(
                                            "Waiting for {:?} before retrying...",
                                            RPC_RETRY_TIME
                                        );
                                        thread::sleep(RPC_RETRY_TIME);
                                    }
                                    _ => panic!("{}", e),
                                }
                            }
                            _ => panic!("{}", e),
                        },
                    }
                }
            }
        }

        let block_was_unknown = submit_block(&test_node, &block, current_height);
        if block_was_unknown {
            for row in csv_rows.iter() {
                wtr.serialize(&row).unwrap();
                println!(
                    "Transaction rejected in block {}: txid: {} reason: {:?} pool: {}",
                    row.height, row.txid, row.reject_reason, row.miner,
                );
            }
        }
        csv_rows.clear();
        current_height += 1;
        wtr.flush().unwrap();
    }
}

// Either submits the block (if needed by retrying) or panics on an unhandled error
// returns true if the node didn't know about the block; false if the node already knew about it
fn submit_block(node: &Client, block: &Block, current_height: u64) -> bool {
    loop {
        match node.submit_block(&block) {
            Ok(_) => return true,
            Err(e) => {
                match e {
                    // The submitblock RPC returns an error DUPLICATE_BLOCK_ERROR, when
                    // the block is already known by Bitcoin Core. A few of these are
                    // expected.
                    bitcoincore_rpc::Error::ReturnedError(s) => {
                        if s == DUPLICATE_BLOCK_ERROR {
                            println!("Block {} is already known by the 'test' Bitcoin Core node. Skipping..", current_height);
                            return false;
                        } else {
                            panic!("ReturnedError({})", s);
                        }
                    }
                    bitcoincore_rpc::Error::JsonRpc(e) => {
                        match e {
                            // If we are sending blocks and transactions too fast, Bitcoin Core
                            // RPC server receive buffer might fill up and we receive a transport
                            // error: Resource temporarily unavailable.
                            bitcoincore_rpc::jsonrpc::Error::Transport(e) => {
                                println!("Transport error while submitting block: {}", e);
                                println!(
                                    "Waiting for {:?} before retrying...",
                                    RPC_RETRY_TIME
                                );
                                thread::sleep(RPC_RETRY_TIME);
                            }
                            _ => panic!("{}", e),
                        }
                    }
                    _ => panic!("{}", e),
                }
            }
        }
    }
}
