use tokio_postgres::{NoTls, Error};

mod cli;
mod pg_hasher;

#[tokio::main] // By default, tokio_postgres uses the tokio crate as its runtime.
async fn main() -> Result<(), Error> {
    let args = cli::Params::get_args();
    // Connect to the database.
    let (client, connection) =
        tokio_postgres::connect(&*args.source_dsn, NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Now we can execute a simple statement that just returns its parameter.
    let rows = client
        .query(&args.source_query, &[])
        .await?;
    // And then check that we got back the same string we sent over.
    let mut first: bool = true;
    for row in &rows {
        let h = pg_hasher::row_hasher(row, first);
        first = false;
        println!("Checksum: {}", h);
    }

    Ok(())
}
