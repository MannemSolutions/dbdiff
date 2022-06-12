use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use chrono;
use tokio_postgres::{NoTls, Error};
use tokio_postgres::types::Type;
use ordered_float;
use bit_vec::BitVec;
use chrono::Utc;
use cidr;

fn float_hasher(of: Option<f64>, mut s: DefaultHasher) -> DefaultHasher {
    match of {
        Some(f) => {
            let of = ordered_float::OrderedFloat(f);
            of.hash(&mut s)
        }
        None => None::<ordered_float::OrderedFloat<f64>>.hash(&mut s),
    }
    s
}

fn point_hasher(point: Option<geo_types::Point<f64>>, mut s: DefaultHasher) -> DefaultHasher {
    match point {
        Some(p) => {
            let x = ordered_float::OrderedFloat(p.x());
            x.hash(&mut s);
            let y = ordered_float::OrderedFloat(p.y());
            y.hash(&mut s)
        },
        None => None::<ordered_float::OrderedFloat<f64>>.hash(&mut s),
    }
    s
}

fn rect_hasher(rect: Option<geo_types::Rect<f64>>, mut s: DefaultHasher) -> DefaultHasher {
    match rect {
        Some(r) => {
            let c = r.center();
            ordered_float::OrderedFloat(c.x).hash(&mut s);
            ordered_float::OrderedFloat(c.y).hash(&mut s);
            ordered_float::OrderedFloat(r.height()).hash(&mut s);
            ordered_float::OrderedFloat(r.width()).hash(&mut s);
        },
        None => None::<ordered_float::OrderedFloat<f64>>.hash(&mut s),
    }
    s
}

fn linestring_hasher(ls: Option<geo_types::LineString<f64>>, mut s: DefaultHasher) -> DefaultHasher {
    match ls {
        Some(ls) => {
            for c in ls.coords() {
                ordered_float::OrderedFloat(c.x).hash(&mut s);
                ordered_float::OrderedFloat(c.y).hash(&mut s);
            }
        },
        None => None::<ordered_float::OrderedFloat<f64>>.hash(&mut s),
    }
    s
}

fn json_hasher(sj: Option<serde_json::Value>, mut s: DefaultHasher) -> DefaultHasher {
    match sj {
        Some(j) => {
            j.as_str().hash(&mut s);
        },
        None => None::<ordered_float::OrderedFloat<f64>>.hash(&mut s),
    }
    s
}


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
                Type::BIT => row.get::<usize, Option<BitVec>>(i).hash(&mut s),
                Type::BIT_ARRAY => {
                   for b in row.get::<usize, Vec<Option<BitVec>>>(i) {
                       b.hash(&mut s)
                   }
                },
                Type::BOOL => row.get::<usize, Option<bool>>(i).hash(&mut s),
                Type::BOOL_ARRAY => {
                    for b in row.get::<usize, Vec<Option<bool>>>(i) {
                        b.hash(&mut s)
                    }
                },
                Type::CHAR => row.get::<usize, Option<i8>>(i).hash(&mut s),
                Type::CHAR_ARRAY => {
                    for c in row.get::<usize, Vec<Option<i8>>>(i) {
                        c.hash(&mut s)
                    }
                },
                Type::INT2 => row.get::<usize, Option<i16>>(i).hash(&mut s),
                Type::INT2_ARRAY => {
                    for i2 in row.get::<usize, Vec<Option<i16>>>(i) {
                        i2.hash(&mut s)
                    }
                },
                Type::INT4 => row.get::<usize, Option<i32>>(i).hash(&mut s),
                Type::INT4_ARRAY => {
                    for i4 in row.get::<usize, Vec<Option<i32>>>(i) {
                        i4.hash(&mut s)
                    }
                },
                Type::INT8 => row.get::<usize, Option<i64>>(i).hash(&mut s),
                Type::INT8_ARRAY => {
                    for i8 in row.get::<usize, Vec<Option<i64>>>(i) {
                        i8.hash(&mut s)
                    }
                },
                Type::OID => row.get::<usize, Option<u32>>(i).hash(&mut s),
                Type::OID_ARRAY => {
                    for o in row.get::<usize, Vec<Option<u32>>>(i) {
                        o.hash(&mut s)
                    }
                },
                Type::FLOAT4 | Type::FLOAT8 => s = float_hasher(row.get::<usize, Option<f64>>(i), s),
                Type::FLOAT4_ARRAY | Type::FLOAT8_ARRAY => {
                    for f in row.get::<usize, Vec<Option<f64>>>(i) {
                        s = float_hasher(f, s);
                    }
                },
                Type::CIDR => row.get::<usize, Option<cidr::IpCidr>>(i).hash(&mut s),
                Type::CIDR_ARRAY => {
                    for c in row.get::<usize, Vec<Option<cidr::IpCidr>>>(i) {
                        c.hash(&mut s)
                    }
                },
                Type::INET => row.get::<usize, Option<cidr::IpInet>>(i).hash(&mut s),
                Type::INET_ARRAY => {
                    for inet in row.get::<usize, Vec<Option<cidr::IpInet>>>(i) {
                        inet.hash(&mut s)
                    }
                },
                Type::MACADDR | Type::MACADDR8 => row.get::<usize, Option<eui48::MacAddress>>(i).hash(&mut s),
                Type::MACADDR_ARRAY | Type::MACADDR8_ARRAY=> {
                    for m in row.get::<usize, Vec<Option<cidr::IpCidr>>>(i) {
                        m.hash(&mut s)
                    }
                },
                Type::POINT => s = point_hasher(row.get::<usize, Option<geo_types::Point<f64>>>(i), s),
                Type::POINT_ARRAY => {
                    for p in row.get::<usize, Vec<Option<geo_types::Point<f64>>>>(i) {
                        s = point_hasher(p, s)
                    }
                },
                Type::BOX => s = rect_hasher(row.get::<usize, Option<geo_types::Rect<f64>>>(i), s),
                Type::BOX_ARRAY=> {
                    for ob in row.get::<usize, Vec<Option<geo_types::Rect<f64>>>>(i) {
                        s = rect_hasher(ob, s);
                    }
                },
                Type::PATH => s = linestring_hasher(row.get::<usize, Option<geo_types::LineString<f64>>>(i), s),
                Type::PATH_ARRAY => for ls in row.get::<usize, Vec<Option<geo_types::LineString<f64>>>>(i) {
                    s = linestring_hasher(ls, s);
                },
                Type::JSON | Type::JSONB => s = json_hasher(row.get::<usize, Option<serde_json::Value>>(i), s),
                Type::JSON_ARRAY | Type::JSONB_ARRAY =>
                    for sj in row.get::<usize, Vec<Option<serde_json::Value>>>(i) {
                        s = json_hasher(sj, s);
                    }
                Type::UUID => row.get::<usize, Option<uuid::Uuid>>(i).hash(&mut s),
                Type::UUID_ARRAY =>
                    for u in row.get::<usize, Vec<Option<uuid::Uuid>>>(i) {
                        u.hash(&mut s)
                    }
                Type::TIMESTAMP => row.get::<usize, Option<chrono::NaiveDateTime>>(i).hash(&mut s),
                Type::TIMESTAMP_ARRAY =>
                    for t in row.get::<usize, Vec<Option<chrono::NaiveDateTime>>>(i) {
                        t.hash(&mut s)
                    }
                Type::TIMESTAMPTZ => row.get::<usize, Option<chrono::DateTime<Utc>>>(i).hash(&mut s),
                Type::TIMESTAMPTZ_ARRAY =>
                    for t in row.get::<usize, Vec<Option<chrono::DateTime<Utc>>>>(i) {
                        t.hash(&mut s)
                    }
                Type::DATE => row.get::<usize, Option<chrono::NaiveDate>>(i).hash(&mut s),
                Type::DATE_ARRAY =>
                    for d in row.get::<usize, Vec<Option<chrono::NaiveDate>>>(i) {
                        d.hash(&mut s)
                    }
                Type::TIME => row.get::<usize, Option<chrono::NaiveTime>>(i).hash(&mut s),
                Type::TIME_ARRAY =>
                    for d in row.get::<usize, Vec<Option<chrono::NaiveTime>>>(i) {
                        d.hash(&mut s)
                    }
                Type::VARCHAR | Type::BYTEA => row.get::<usize, Option<String>>(i).hash(&mut s),
                Type::VARCHAR_ARRAY | Type::BYTEA_ARRAY =>
                    for v in row.get::<usize, Vec<Option<String>>>(i) {
                        v.hash(&mut s)
                    }
                _ => println!("missing type conversion for {}", cols[i].type_())
            }
        }
        println!("Checksum: {}", s.finish());
    }

    Ok(())
}
