use anyhow::Result;
use core::pin::Pin;
use futures;
use futures::{pin_mut, TryStreamExt};
use std::borrow::Borrow;
use std::collections::HashMap;
use tokio_postgres::{Row, RowStream};

mod cli;
mod dsn;
mod generic;
mod pg_hasher;

async fn next_hash(mut rows: Pin<&mut RowStream>, first: bool) -> Result<(Row, u64)> {
    match rows.try_next().await {
        Ok(or) => match or {
            Some(r) => {
                let hash = pg_hasher::row_hasher(r.borrow(), first);
                Ok((r, hash))
            }
            None => Err(anyhow::Error::msg("We reached the end of the RowStream")),
        },
        Err(e) => Err(anyhow::Error::from(e)),
    }
}


#[tokio::main] // By default, tokio_postgres uses the tokio crate as its runtime.
async fn main() -> Result<()> {
    // We need to pass in params, and we need to define params for async operations
    // Lets define as array of 32 bit integer with 0 elements
    let params: &[i32] = &[];
    let args = cli::Params::get_args();

    // Connect to the source database.
    let source_dsn = dsn::Dsn::from_string(args.source_dsn.as_str())
        .merge(dsn::Dsn::from_defaults());
    let source = source_dsn.client().await;
    // And run the query on the source connection
    let source_rows = source.query_raw(&args.source_query, params).await?;

    // Connect to the dest database.
    let dest_dsn = dsn::Dsn::from_string(args.dest_dsn.as_str())
        .merge(dsn::Dsn::from_defaults());
    let dest = dest_dsn.client().await;
    // And run the query on the dest connection
    let dest_rows = dest.query_raw(&args.dest_query, params).await?;

    // And then check that we got back the same string we sent over.

    pin_mut!(source_rows);
    let mut source_done: bool = false;
    pin_mut!(dest_rows);
    let mut dest_done: bool = false;
    let mut i: u32 = 0;
    let mut _of: bool = false;
    let mut source_distinct_rows: HashMap<u64, Row> = HashMap::new();
    let mut dest_distinct_rows: HashMap<u64, Row> = HashMap::new();
    loop {
        if source_done && dest_done {
            break;
        } else if dest_distinct_rows.len() + source_distinct_rows.len() > args.max_unmatched {
            break;
        }
        if i % 2 == 0 {
            if source_done {
                // Add one, don't care about overflow
                (i, _of) = i.overflowing_add(1);
            } else {
                match next_hash(source_rows.as_mut(), false).await {
                    Ok((r, h)) => {
                        if dest_distinct_rows.contains_key(&h) {
                            dest_distinct_rows.remove(&h);
                        } else {
                            source_distinct_rows.insert(h, r);
                            (i, _of) = i.overflowing_add(1);
                        }
                    }
                    Err(e) => {
                        if e.to_string() == "We reached the end of the RowStream" {
                            source_done = true
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        } else {
            if dest_done {
                // Add one, don't care about overflow
                (i, _of) = i.overflowing_add(1);
            } else {
                match next_hash(dest_rows.as_mut(), false).await {
                    Ok((r, h)) => {
                        if source_distinct_rows.contains_key(&h) {
                            source_distinct_rows.remove(&h);
                        } else {
                            dest_distinct_rows.insert(h, r);
                            (i, _of) = i.overflowing_add(1);
                        }
                    }
                    Err(e) => {
                        if e.to_string() == "We reached the end of the RowStream" {
                            dest_done = true
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }
    }
    println!("Processed: {}", i + 1);
    match args.output_format.as_str() {
        "hashmap" => {
            for (_h, r) in source_distinct_rows {
                println!("< {}", pg_hasher::row_as_string(r.borrow(), false));
            }
            for (_h, r) in dest_distinct_rows {
                println!("> {}", pg_hasher::row_as_string(r.borrow(), false));
            }
        }
        "insert" => {
            for (_h, r) in source_distinct_rows {
                println!(
                    "< {}",
                    pg_hasher::row_as_insert(args.dest_table_name.as_str(), r.borrow(), false)
                );
            }
            for (_h, r) in dest_distinct_rows {
                println!(
                    "> {}",
                    pg_hasher::row_as_insert(args.source_table_name.as_str(), r.borrow(), false)
                );
            }
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Invalid output format {}",
                args.output_format
            ));
        }
    }

    Ok(())
}
