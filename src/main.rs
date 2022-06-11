use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use chrono;
use tokio_postgres::{NoTls, Error};
use tokio_postgres::types::Type;
use ordered_float;

#[tokio::main] // By default, tokio_postgres uses the tokio crate as its runtime.
async fn main() -> Result<(), Error> {
    // Connect to the database.
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=postgres port=49304 password=supassword", NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Now we can execute a simple statement that just returns its parameter.
    let rows = client
        .query("SELECT * from t4 order by 1, 2, 3, 4", &[])
        .await?;

    // And then check that we got back the same string we sent over.
    for row in &rows {
        let mut s = DefaultHasher::new();

        let cols = row.columns();
        for i in 0..cols.len() {
            match *cols[i].type_() {
                Type::BOOL => row.get::<usize, Option<bool>>(i).hash(&mut s),
                Type::CHAR => row.get::<usize, Option<i8>>(i).hash(&mut s),
                Type::INT2 => row.get::<usize, Option<i16>>(i).hash(&mut s),
                Type::INT4 => row.get::<usize, Option<i32>>(i).hash(&mut s),
                Type::INT8 => row.get::<usize, Option<i64>>(i).hash(&mut s),
                Type::OID => row.get::<usize, Option<u32>>(i).hash(&mut s),
                Type::FLOAT4 | Type::FLOAT8 => {
                    match row.get::<usize, Option<f64>>(i){
                        Some(f) => {
                            let of = ordered_float::OrderedFloat(f);
                            of.hash(&mut s)
                        }
                        None => None::<ordered_float::OrderedFloat::<f64>>.hash(&mut s),
                    }
                },
                Type::DATE => row.get::<usize, Option<chrono::NaiveDate>>(i).hash(&mut s),
                Type::INET => row.get::<usize, Option<IpAddr>>(i).hash(&mut s),
                Type::VARCHAR | Type::BYTEA => row.get::<usize, Option<String>>(i).hash(&mut s),
                _ => println!("missing type conversion for {}", cols[i].type_())
            }
        }
        println!("Checksum: {}", s.finish());
    }

    Ok(())
}
