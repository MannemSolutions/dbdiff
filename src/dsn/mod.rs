use crate::generic;
use std::collections::HashMap;
use users::{get_current_uid, get_user_by_uid};

pub struct Dsn {
    kv: HashMap<String, String>,
}

impl Dsn {
    // fn new() -> Dsn {
    //     Dsn{
    //         kv: HashMap::new(),
    //     }
    // }
    pub fn from_string(from: &str) -> Dsn {
        let mut kv: HashMap<String, String> = HashMap::new();
        let split = from.split(" ");
        for s in split {
            if let Some((k, v)) = s.split_once("=") {
                kv.insert(k.to_string(), v.to_string());
            }
        }
        Dsn { kv }
    }
    pub fn merge(self, with: Dsn) -> Dsn {
        let mut kv: HashMap<String, String> = HashMap::new();
        kv.extend(with.kv);
        kv.extend(self.kv);
        Dsn { kv }
    }
    pub fn from_defaults() -> Dsn {
        let mut kv: HashMap<String, String> = HashMap::new();

        let mut user = generic::get_env_str("", "PGUSER", "").to_string();
        if user.is_empty() {
            user = match get_user_by_uid(get_current_uid()).unwrap().name().to_str() {
                Some(osuser) => osuser.to_string(),
                None => "".to_string(),
            };
        }
        kv.insert("user".to_string(), user.to_string());
        kv.insert(
            "dbname".to_string(),
            generic::get_env_str("", "PGDATABASE", user.as_str()),
        );
        kv.insert(
            "host".to_string(),
            generic::get_env_str("", "PGHOST", "/tmp"),
        );
        kv.insert(
            "sslmode".to_string(),
            generic::get_env_str("", "PGSSLMODE", "prefer"),
        );
        kv.insert(
            "sslcert".to_string(),
            generic::get_env_str("", "PGSSLCERT", "~/.postgresql/postgresql.crt"),
        );
        kv.insert(
            "sslkey".to_string(),
            generic::get_env_str("", "PGSSLKEY", "~/.postgresql/postgresql.key"),
        );
        kv.insert(
            "sslrootcert".to_string(),
            generic::get_env_str("", "PGSSLKEY", "~/.postgresql/root.crt"),
        );
        kv.insert(
            "sslcrl".to_string(),
            generic::get_env_str("", "PGSSLKEY", "~/.postgresql/root.crl"),
        );
        Dsn { kv }
    }
    pub fn as_string(self) -> String {
        let mut vec = Vec::new();
        for (k, v) in self.kv {
            vec.push(format!("{0}={1}", k, v))
        }
        vec.join(" ")
    }
}
