use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use chrono;
use tokio_postgres::{Row, types::Type};
use bit_vec::BitVec;
use chrono::Utc;
use cidr;
use std::collections::HashMap;

pub const NULL: &'static str = "Null";

fn str_as_name(name: &str) -> String {
    format!("\"{}\"", name.replace("'", "''"))
}

fn varchar_as_sql_str(os: Option<String>) -> String {
    match os {
        Some(s) => format!("'{}'", s.replace("'", "''")),
        None => String::from(NULL),
    }
}

fn float_as_sql_str(of: Option<f64>) -> String {
    match of {
        Some(f) => {
            f.to_string()
        }
        None => String::from(NULL),
    }
}

fn point_as_sql_str(point: Option<geo_types::Point<f64>>) -> String {
    match point {
        Some(p) => format!("{} {}", p.x(), p.y()),
        None => String::from(NULL),
    }
}

fn rect_as_sql_str(rect: Option<geo_types::Rect<f64>>) -> String {
    match rect {
        Some(r) => {
            let c = r.center();
            format!("{} {} {} {}", c.x, c.y, r.height(), r.width())
        },
        None => String::from(NULL),
    }
}

fn linestring_as_sql_str(ls: Option<geo_types::LineString<f64>>) -> String {
    let mut vec: Vec<String> = Vec::new();
    match ls {
        Some(ls) => {
            for c in ls.coords() {
                vec.push(format!("{} {}", c.x, c.y))
            }
            vec.join(" ")
        },
        None => String::from(NULL),
    }
}

fn json_as_sql_str(sj: Option<serde_json::Value>) -> String {
    match sj {
        Some(j) => j.to_string(),
        None => String::from(NULL),
    }
}

fn col_as_sql_str(row: &Row, i: usize, display: bool) -> String {
    let mut val_array: Vec<String>;
    let col = &row.columns()[i];
    match *col.type_() {
        Type::BIT => {
            match row.get::<usize, Option<BitVec>>(i) {
                Some(b) => b.any().to_string(),
                None => String::from(NULL),
            }
        },
        Type::BIT_ARRAY => {
            val_array = Vec::new();
            for ob in row.get::<usize, Vec<Option<BitVec>>>(i) {
                match ob {
                    Some(b) => val_array.push(b.any().to_string()),
                    None => val_array.push(String::from(NULL))
                }
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::BOOL => {
            match row.get::<usize, Option<bool>>(i) {
                Some(b) => b.to_string(),
                None => String::from(NULL),
            }
        },
        Type::BOOL_ARRAY => {
            val_array = Vec::new();
            for ob in row.get::<usize, Vec<Option<bool>>>(i) {
                match ob {
                    Some(b) => val_array.push(b.to_string()),
                    None => val_array.push(String::from(NULL))
                }
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::CHAR =>
            match row.get::<usize, Option<i8>>(i) {
                Some(i) => i.to_string(),
                None => String::from(NULL),
            }
        Type::CHAR_ARRAY => {
            val_array = Vec::new();
            for oi in row.get::<usize, Vec<Option<i8>>>(i) {
                match oi {
                    Some(i) => val_array.push(i.to_string()),
                    None => val_array.push(String::from(NULL))
                }
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::INT2 =>
            match row.get::<usize, Option<i16>>(i) {
                Some(i) => i.to_string(),
                None => String::from(NULL),
            }
        Type::INT2_ARRAY => {
            val_array = Vec::new();
            for oi in row.get::<usize, Vec<Option<i16>>>(i) {
                match oi {
                    Some(i) => val_array.push(i.to_string()),
                    None => val_array.push(String::from(NULL))
                }
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::INT4 =>
            match row.get::<usize, Option<i32>>(i) {
                Some(i) => i.to_string(),
                None => String::from(NULL),
            }
        Type::INT4_ARRAY => {
            val_array = Vec::new();
            for oi in row.get::<usize, Vec<Option<i32>>>(i) {
                match oi {
                    Some(i) => val_array.push(i.to_string()),
                    None => val_array.push(String::from(NULL))
                }
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::INT8 =>
            match row.get::<usize, Option<i64>>(i) {
                Some(i) => i.to_string(),
                None => String::from(NULL),
            }
        Type::INT8_ARRAY => {
            val_array = Vec::new();
            for oi in row.get::<usize, Vec<Option<i64>>>(i) {
                match oi {
                    Some(i) => val_array.push(i.to_string()),
                    None => val_array.push(String::from(NULL))
                }
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::OID =>
            match row.get::<usize, Option<u32>>(i) {
                Some(i) => i.to_string(),
                None => String::from(NULL),
            }
        Type::OID_ARRAY => {
            val_array = Vec::new();
            for oi in row.get::<usize, Vec<Option<u32>>>(i) {
                match oi {
                    Some(i) => val_array.push(i.to_string()),
                    None => val_array.push(String::from(NULL))
                }
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::FLOAT4 | Type::FLOAT8 => float_as_sql_str(row.get::<usize, Option<f64>>(i)),
        Type::FLOAT4_ARRAY | Type::FLOAT8_ARRAY => {
            val_array = Vec::new();
            for f in row.get::<usize, Vec<Option<f64>>>(i) {
                val_array.push(float_as_sql_str(f));
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::CIDR =>
            match row.get::<usize, Option<cidr::IpCidr>>(i) {
                Some(ic) => ic.to_string(),
                None => String::from(NULL),
            }
        Type::CIDR_ARRAY => {
            val_array = Vec::new();
            for c in row.get::<usize, Vec<Option<cidr::IpCidr>>>(i) {
                match c {
                    Some(ic) => val_array.push(ic.to_string()),
                    None => val_array.push(String::from(NULL)),
                }
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::INET =>
            match row.get::<usize, Option<cidr::IpInet>>(i) {
                Some(inet) => inet.to_string(),
                None => String::from(NULL),
            }
        Type::INET_ARRAY => {
            val_array = Vec::new();
            for oinet in row.get::<usize, Vec<Option<cidr::IpInet>>>(i) {
                match oinet {
                    Some(inet) => val_array.push(inet.to_string()),
                    None => val_array.push(String::from(NULL)),
                }
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::MACADDR | Type::MACADDR8 =>
            match row.get::<usize, Option<eui48::MacAddress>>(i) {
                Some(mac) => mac.to_string(eui48::MacAddressFormat::HexString),
                None => String::from(NULL),
            }
        Type::MACADDR_ARRAY | Type::MACADDR8_ARRAY => {
            val_array = Vec::new();
            for m in row.get::<usize, Vec<Option<eui48::MacAddress>>>(i) {
                match m {
                    Some(mac) => val_array.push(mac.to_string(eui48::MacAddressFormat::HexString)),
                    None => val_array.push(String::from(NULL)),
                }
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::POINT => point_as_sql_str(row.get::<usize, Option<geo_types::Point<f64>>>(i)),
        Type::POINT_ARRAY => {
            val_array = Vec::new();
            for p in row.get::<usize, Vec<Option<geo_types::Point<f64>>>>(i) {
                val_array.push(point_as_sql_str(p))
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::BOX => rect_as_sql_str(row.get::<usize, Option<geo_types::Rect<f64>>>(i)),
        Type::BOX_ARRAY => {
            val_array = Vec::new();
            for ob in row.get::<usize, Vec<Option<geo_types::Rect<f64>>>>(i) {
                val_array.push(rect_as_sql_str(ob));
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::PATH => linestring_as_sql_str(row.get::<usize, Option<geo_types::LineString<f64>>>(i)),
        Type::PATH_ARRAY => {
            val_array = Vec::new();
            for ls in row.get::<usize, Vec<Option<geo_types::LineString<f64>>>>(i) {
                val_array.push(linestring_as_sql_str(ls));
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::JSON | Type::JSONB => json_as_sql_str(row.get::<usize, Option<serde_json::Value>>(i)),
        Type::JSON_ARRAY | Type::JSONB_ARRAY => {
            val_array = Vec::new();
            for sj in row.get::<usize, Vec<Option<serde_json::Value>>>(i) {
                val_array.push(json_as_sql_str(sj));
            }
            format!("[ {} ]", val_array.join(", "))
        }

        Type::UUID =>
            match row.get::<usize, Option<uuid::Uuid>>(i) {
                Some(mac) => mac.to_string(),
                None => String::from(NULL),
            }
        Type::UUID_ARRAY => {
            val_array = Vec::new();
            for ou in row.get::<usize, Vec<Option<uuid::Uuid>>>(i) {
                match ou {
                    Some(u) => val_array.push(u.to_string()),
                    None => val_array.push(String::from(NULL)),
                }
            }
            format!("[ {} ]", val_array.join(", "))
        }
        Type::TIMESTAMP =>
            match row.get::<usize, Option<chrono::NaiveDateTime>>(i) {
                Some(dt) => dt.to_string(),
                None => String::from(NULL),
            }
        Type::TIMESTAMP_ARRAY => {
            val_array = Vec::new();
            for odt in row.get::<usize, Vec<Option<chrono::NaiveDateTime>>>(i) {
                match odt {
                    Some(dt) => val_array.push(dt.to_string()),
                    None => val_array.push(String::from(NULL)),
                }
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::TIMESTAMPTZ =>
            match row.get::<usize, Option<chrono::DateTime<Utc>>>(i) {
                Some(dt) => dt.to_string(),
                None => String::from(NULL),
            }
        Type::TIMESTAMPTZ_ARRAY => {
            val_array = Vec::new();
            for t in row.get::<usize, Vec<Option<chrono::DateTime<Utc>>>>(i) {
                match t {
                    Some(dt) => val_array.push(dt.to_string()),
                    None => val_array.push(String::from(NULL)),
                }
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::DATE =>
            match row.get::<usize, Option<chrono::NaiveDate>>(i) {
                Some(dt) => dt.to_string(),
                None => String::from(NULL),
            }
        Type::DATE_ARRAY => {
            val_array = Vec::new();
            for d in row.get::<usize, Vec<Option<chrono::NaiveDate>>>(i) {
                match d {
                    Some(dt) => val_array.push(dt.to_string()),
                    None => val_array.push(String::from(NULL)),
                }
            }
            format!("[ {} ]", val_array.join(", "))
        },
        Type::TIME =>
            match row.get::<usize, Option<chrono::NaiveTime>>(i) {
                Some(dt) => dt.to_string(),
                None => String::from(NULL),
            }
        Type::TIME_ARRAY => {
            val_array = Vec::new();
            for d in row.get::<usize, Vec<Option<chrono::NaiveTime>>>(i) {
                match d {
                    Some(dt) => val_array.push(dt.to_string()),
                    None => val_array.push(String::from(NULL)),
                }
            }
            format!("[ {} ]", val_array.join(", "))

        },
        Type::VARCHAR | Type::BYTEA | Type::NAME | Type::TEXT => varchar_as_sql_str(row.get::<usize, Option<String>>(i)),
        Type::VARCHAR_ARRAY | Type::BYTEA_ARRAY | Type::NAME_ARRAY | Type::TEXT_ARRAY => {
            val_array = Vec::new();
            for os in row.get::<usize, Vec<Option<String>>>(i) {
                val_array.push(varchar_as_sql_str(os));
            }
            format!("[ {} ]", val_array.join(", "))
        }
        _ => {
            if display {
                println!("missing type conversion for {}, use {}::TEXT if you want to take it into account", *col.type_(), col.name());
            }
            String::from(NULL)
        }
    }
}

pub fn row_hasher(row: &Row, display: bool) -> u64 {
    let mut s = DefaultHasher::new();

    let cols = row.columns();
    for i in 0..cols.len() {
        col_as_sql_str(row, i, display).hash(&mut s);
        }
    s.finish()
}

pub fn row_map(row: &Row, display: bool) -> HashMap<String, String> {
    let mut row_map: HashMap<String, String> = HashMap::new();
    let cols = row.columns();
    for i in 0..cols.len() {
        row_map.insert(String::from(cols[i].name()), col_as_sql_str(row, i, display));
    }
    row_map
}
pub fn row_as_string(row: &Row, display: bool) -> String {
    let mut col_vals: Vec<String> = Vec::new();
    let cols = row.columns();
    for i in 0..cols.len() {
        let col_val = format!("{}: {}", cols[i].name(), col_as_sql_str(row, i, display));
        col_vals.push(col_val);
    }
    format!("[ {} ]", col_vals.join(", "))
}
pub fn row_as_insert(table_name: &str, row: &Row, display: bool) -> String {
    let mut col_names: Vec<String> = Vec::new();
    let mut col_vals: Vec<String> = Vec::new();
    let cols = row.columns();
    for i in 0..cols.len() {
        col_names.push(str_as_name(cols[i].name()));
        col_vals.push(col_as_sql_str(row, i, display));
    }
    format!("insert into {} ({}) VALUES({});", str_as_name(table_name),
            col_names.join(", "), col_vals.join(", "))
}
