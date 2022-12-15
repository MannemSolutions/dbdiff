use openssl::ssl::{SslConnector, SslFiletype, SslMethod};
use postgres_openssl::MakeTlsConnector;
use serde::{Deserialize, Serialize};
use whoami;

#[derive(Serialize, Deserialize)]
struct DatabaseConfig {
    client_cert_path: String,
    client_key_path: String,
    server_ca_path: String,
    host: String,
    dbname: String,
    user: String,
}

impl ::std::default::Default for DatabaseConfig {
    fn default() -> Self {
        let user_name = whoami::username();
        Self {
            client_cert_path: "~/.postgresql/postgresql.crt".into(),
            client_key_path: "~/.postgresql/postgresql.key".into(),
            server_ca_path: "~/.postgresql/root.crt".into(),
            host: "/tmp".into(),
            dbname: "".into(),
            user: user_name.into(),
        }
    }
}

fn shellexpand(path: &str) -> String {
    shellexpand::tilde(path).to_string()
}

#[tokio::main]
async fn main() -> Result<(), tokio_postgres::Error> {
    let conf_file = match confy::get_configuration_file_path("postgres-ssl", None) {
        Ok(s) => s.to_str().unwrap().to_string(),
        Err(error) => panic!("confy-load error: {:?}", error),
    };
    eprintln!("config file: {}", conf_file);
    let config: DatabaseConfig = match confy::load("postgres-ssl", None) {
        Ok(value) => value,
        Err(error) => panic!("confy-load error: {:?}", error),
    };

    let client_cert_path = shellexpand(config.client_cert_path.as_str());
    let client_key_path = shellexpand(config.client_key_path.as_str());
    let server_ca_path = shellexpand(config.server_ca_path.as_str());
    let host = config.host;
    let mut dbname = config.dbname;
    let user = config.user;
    if dbname == "" {
        dbname = user.to_string();
    }
    let connection_string = format!(
        "sslmode=require host={} dbname={} user={}",
        host, dbname, user
    );
    eprintln!("connection string: {}", connection_string);

    let mut builder = match SslConnector::builder(SslMethod::tls()) {
        Ok(value) => value,
        Err(error) => panic!("connector error: {}", error),
    };

    if let Err(error) = builder.set_certificate_chain_file(&client_cert_path) {
        eprintln!("set_certificate_file: {}", error);
    }
    if let Err(error) = builder.set_private_key_file(&client_key_path, SslFiletype::PEM) {
        eprintln!("set_client_key_file: {}", error);
    }
    if let Err(error) = builder.set_ca_file(&server_ca_path) {
        eprintln!("set_ca_file: {}", error);
    }

    let mut connector = MakeTlsConnector::new(builder.build());

    connector.set_callback(|config, _| {
        config.set_verify_hostname(false);

        Ok(())
    });

    let connect = tokio_postgres::connect(&connection_string, connector);

    let (client, connection) = match connect.await {
        Ok(value) => value,
        Err(error) => panic!("connect error: {}", error),
    };

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("connection error: {}", error);
        }

        println!("client:{:?}", client);
    });

    Ok(())
}
