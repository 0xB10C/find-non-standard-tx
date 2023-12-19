use bitcoincore_rpc::{Auth, Client, RpcApi};
use config::Config;
use csv::Writer;

const DUPLICATE_BLOCK_ERROR: &str = "\"duplicate\"";
const TX_ALREADY_IN_MEMPOOL_REJECTION_REASON: &str = "txn-already-in-mempool";

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
    wtr.write_record(&[
        "height",
        "txid",
        "reject_reason",
        "vsize",
        "inputs",
        "outputs",
    ])
    .unwrap();

    let mut current_height = start_height;
    while current_height <= data_node.get_block_count().unwrap() {
        let block_hash = data_node.get_block_hash(current_height).unwrap();
        let block = data_node.get_block(&block_hash).unwrap();

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
                csv_rows.push([
                    current_height.to_string(),
                    tx.txid().to_string(),
                    reject_reason,
                    tx.vsize().to_string(),
                    tx.input.len().to_string(),
                    tx.output.len().to_string(),
                ]);
            } else {
                test_node.send_raw_transaction(tx).unwrap();
            }
        }

        match test_node.submit_block(&block) {
            Ok(_) => {
                for row in csv_rows.iter() {
                    wtr.write_record(row.iter()).unwrap();
                    println!(
                        "Transaction rejected in block {}: txid={} reason={:?}",
                        row[0], row[1], row[2]
                    );
                }
                csv_rows.clear();
            }
            Err(e) => {
                match e {
                    // The submitblock RPC returns an error DUPLICATE_BLOCK_ERROR, when
                    // the block is already known by Bitcoin Core. A few of these are
                    // expected.
                    bitcoincore_rpc::Error::ReturnedError(s) => {
                        if s == DUPLICATE_BLOCK_ERROR {
                            println!("Block {} was already known to the 'test' Bitcoin Core node. Skipping..", current_height);
                        } else {
                            panic!("ReturnedError({})", s);
                        }
                    }
                    _ => panic!("{}", e),
                }
            }
        }
        current_height += 1;
    }
    wtr.flush().unwrap();
}
