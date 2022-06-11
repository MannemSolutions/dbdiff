use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use chrono;
use tokio_postgres::{NoTls, Error};
use tokio_postgres::types::Type;
use ordered_float;
use bit_vec::BitVec;
use chrono::Utc;
use cidr;

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
                Type::BIT => row.get::<usize, Option<BitVec>>(i).hash(&mut s),
                Type::FLOAT4 | Type::FLOAT8 => {
                    match row.get::<usize, Option<f64>>(i){
                        Some(f) => {
                            let of = ordered_float::OrderedFloat(f);
                            of.hash(&mut s)
                        }
                        None => None::<ordered_float::OrderedFloat::<f64>>.hash(&mut s),
                    }
                },
                Type::CIDR => row.get::<usize, Option<cidr::IpCidr>>(i).hash(&mut s),
                Type::INET => row.get::<usize, Option<cidr::IpInet>>(i).hash(&mut s),
                Type::MACADDR => row.get::<usize, Option<eui48::MacAddress>>(i).hash(&mut s),
                Type::POINT => {
                    match row.get::<usize, Option<geo_types::Point<f64>>>(i){
                        Some(p) => {
                            let x = ordered_float::OrderedFloat(p.x());
                            x.hash(&mut s);
                            let y = ordered_float::OrderedFloat(p.y());
                            y.hash(&mut s)
                        },
                        None => None::<ordered_float::OrderedFloat::<f64>>.hash(&mut s),
                    }
                },
                Type::BOX => {
                    match row.get::<usize, Option<geo_types::Rect<f64>>>(i){
                        Some(r) => {
                            let c = r.center();
                            ordered_float::OrderedFloat(c.x).hash(&mut s);
                            ordered_float::OrderedFloat(c.y).hash(&mut s);
                            ordered_float::OrderedFloat(r.height()).hash(&mut s);
                            ordered_float::OrderedFloat(r.width()).hash(&mut s);
                        },
                        None => None::<ordered_float::OrderedFloat::<f64>>.hash(&mut s),
                    }
                },
                Type::PATH => match row.get::<usize, Option<geo_types::LineString<f64>>>(i){
                    Some(ls) => {
                        for c in ls.coords() {
                            ordered_float::OrderedFloat(c.x).hash(&mut s);
                            ordered_float::OrderedFloat(c.y).hash(&mut s);
                        }
                    },
                    None => None::<ordered_float::OrderedFloat::<f64>>.hash(&mut s),
                },
                Type::JSON | Type::JSONB => match row.get::<usize, Option<serde_json::Value>>(i) {
                    Some(j) => {
                        j.as_str().hash(&mut s);
                    },
                    None => None::<ordered_float::OrderedFloat::<f64>>.hash(&mut s),
                },
                Type::UUID => row.get::<usize, Option<uuid::Uuid>>(i).hash(&mut s),
                Type::TIMESTAMP => row.get::<usize, Option<chrono::NaiveDateTime>>(i).hash(&mut s),
                Type::TIMESTAMPTZ => row.get::<usize, Option<chrono::DateTime<Utc>>>(i).hash(&mut s),
                Type::DATE => row.get::<usize, Option<chrono::NaiveDate>>(i).hash(&mut s),
                Type::TIME => row.get::<usize, Option<chrono::NaiveTime>>(i).hash(&mut s),
                Type::VARCHAR | Type::BYTEA => row.get::<usize, Option<String>>(i).hash(&mut s),
                _ => println!("missing type conversion for {}", cols[i].type_())
            }
        }
        println!("Checksum: {}", s.finish());
    }

    Ok(())
}
