use bitcoin_pool_identification::{default_data, PoolIdentification};
use bitcoincore_rpc::bitcoin::{Amount, Block, Network, Txid};
use bitcoincore_rpc::jsonrpc;
use bitcoincore_rpc::{Client, RpcApi};
use config::Config;
use csv::Writer;
use env_logger::Env;
use log::info;
use std::fs::OpenOptions;
use std::time;

const DUPLICATE_BLOCK_ERROR: &str = "\"duplicate\"";
const TX_ALREADY_IN_MEMPOOL_REJECTION_REASON: &str = "txn-already-in-mempool";
const RPC_TIMEOUT: time::Duration = time::Duration::from_secs(60 * 5); // 5 minutes

// Bitcoin Core won't allow anything larger or equal to 1 BTC here
// since https://github.com/bitcoin/bitcoin/pull/29434. So use 1 BTC - 1 sat.
// The burn amount can be higher.
const MAX_FEE: Amount = Amount::from_sat(99_999_999);
const MAX_BURN: Amount = Amount::from_sat(999_999_999);

fn rpc_client(settings: &Config, node: &str) -> Client {
    let rpc_url = &format!(
        "{}:{}",
        settings
            .get::<String>(&format!("nodes.{}.rpc_host", node))
            .expect(&format!("need a rpc_host for the {} node", node)),
        settings
            .get::<u16>(&format!("nodes.{}.rpc_port", node))
            .expect(&format!("need a rpc_port for the {} node", node)),
    );

    // Build a custom transport to be able to configure the timeout.
    let custom_timeout_transport = jsonrpc::simple_http::Builder::new()
        .url(rpc_url)
        .expect("invalid rpc url")
        .auth(
            settings
                .get::<String>(&format!("nodes.{}.rpc_user", node))
                .expect(&format!("need a rpc_user for the {} node", node)),
            Some(
                settings
                    .get::<String>(&format!("nodes.{}.rpc_pass", node))
                    .expect(&format!("need a rpc_pass for the {} node", node)),
            ),
        )
        .timeout(RPC_TIMEOUT)
        .build();
    Client::from_jsonrpc(jsonrpc::client::Client::with_transport(
        custom_timeout_transport,
    ))
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
    fee: u64,
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

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

    let output_filename = settings
        .get::<String>("output")
        .expect("No 'output' defined in the configuration");

    let output_file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(output_filename.clone())
        .expect(&format!("Can't open output file {}", output_filename));

    let mut wtr = Writer::from_writer(output_file);

    let pools = default_data(Network::Bitcoin);

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

            let results = test_node.test_mempool_accept(&[tx], Some(MAX_FEE)).unwrap();
            let result = results.first().unwrap();

            if !result.allowed {
                // If a previously aborted run left transactions in the mempool,
                // a transaction will be rejected for already being in the mempool.
                // We don't care about these cases.
                let reject_reason = result.reject_reason.clone().unwrap();
                if reject_reason == TX_ALREADY_IN_MEMPOOL_REJECTION_REASON {
                    continue;
                }

                let info = data_node
                    .get_raw_transaction_info_with_fee(&tx.txid(), Some(&block_hash))
                    .unwrap();
                let fee = info.fee.unwrap_or_default();

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
                    fee: fee.to_sat(),
                });
            } else {
                test_node
                    .send_raw_transaction(tx, Some(MAX_FEE), Some(MAX_BURN))
                    .expect(&format!("Could not send raw transaction {}", tx.txid()));
            }
        }

        let block_was_unknown = submit_block(&test_node, &block, current_height);
        if block_was_unknown {
            for row in csv_rows.iter() {
                wtr.serialize(&row).unwrap();
                info!(
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
                            info!("Block {} is already known by the 'test' Bitcoin Core node. Skipping..", current_height);
                            return false;
                        } else {
                            panic!("ReturnedError({})", s);
                        }
                    }
                    _ => panic!("{}", e),
                }
            }
        }
    }
}
