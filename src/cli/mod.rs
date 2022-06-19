use std::env;
use structopt::StructOpt;

/// Search for a pattern in a file and display the lines that contain it.
#[derive(StructOpt)]
pub struct Params {
    /// Source connection string
    #[structopt(long = "source_dsn")]
    #[structopt(default_value, long)]
    pub source_dsn: String,

    /// Destination connection string
    #[structopt(long = "dest_dsn")]
    #[structopt(default_value, long)]
    pub dest_dsn: String,

    /// Query to run on the source database
    #[structopt(short = "q", long = "source_query")]
    #[structopt(default_value, long)]
    pub source_query: String,

    /// Query to run on the source database, defaults to source query
    #[structopt(long = "dest_query")]
    #[structopt(default_value, long)]
    pub dest_query: String,

    /// Max number of rows to not match
    #[structopt(long = "max_unmatched")]
    #[structopt(default_value, long)]
    pub max_unmatched: usize,
}

fn get_str_default(val: &str, env_key: &str, default: &str) -> String {
    if !val.is_empty() {
        return format!("{}", val);
    }
    match env::var(env_key) {
        Ok(env_val) => env_val,
        Err(_e) => format!("{}", default),
    }
}

fn get_int_default(val: u32, env_key: &str, default: u32) -> u32 {
    if val > 0 {
        return val;
    }
    if let Ok(env_val) = env::var(env_key) {
        if let Ok(env_int_val) = env_val.parse::<u32>() {
            return env_int_val;
        }
    }
    default
}

// fn get_bool_default(val: bool, env_key: String) -> bool {
//     if val {
//         return val;
//     }
//     if let Ok(mut env_val) = env::var(env_key) {
//         env_val.make_ascii_lowercase();
//         if let Ok(env_bool_val) = env_val.parse::<bool>() {
//             return env_bool_val;
//         }
//     }
//     false
// }

impl Params {
    fn from_args() -> Params {
        <Params as StructOpt>::from_args()
    }
    pub fn get_args() -> Params {
        let mut args = Params::from_args();
        args.max_unmatched = get_int_default(args.max_unmatched as u32, &String::from("DBDIFF_MAX_UNMATCHED"), 1048576) as usize;
        args.source_query = get_str_default(&args.source_query, &String::from("DBDIFF_SOURCE_QUERY"), &String::from("select * from pg_tables"));
        args.dest_query = get_str_default(&args.dest_query, &String::from("DBDIFF_DESTINATION_QUERY"), &args.source_query);
        args.source_dsn = get_str_default(
            &args.source_dsn,
            &String::from("DBDIFF_SOURCE"),
            &String::from("host=/tmp"),
        );
        args.dest_dsn = get_str_default(
            &args.dest_dsn,
            &String::from("DBDIFF_DESTINATION"),
            &args.source_dsn[..],
        );
        args
    }
}
