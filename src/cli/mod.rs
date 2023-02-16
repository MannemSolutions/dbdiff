use crate::generic;
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

    /// Table name for insert queries into the source
    #[structopt(short = "t", long = "sourcetable")]
    #[structopt(default_value, long)]
    pub source_table_name: String,

    /// Table name for insert queries into the dest
    #[structopt(long = "desttable")]
    #[structopt(default_value, long)]
    pub dest_table_name: String,

    /// Output format
    #[structopt(short = "f", long = "format")]
    #[structopt(default_value, long)]
    pub output_format: String,

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

impl Params {
    fn from_args() -> Params {
        <Params as StructOpt>::from_args()
    }
    pub fn get_args() -> Params {
        let mut args = Params::from_args();
        args.max_unmatched = generic::get_env_int(
            args.max_unmatched as u32,
            &String::from("DBDIFF_MAX_UNMATCHED"),
            1048576,
        ) as usize;
        args.output_format = generic::get_env_str(
            &args.output_format,
            &String::from("DBDIFF_OUTPUT_FORMAT"),
            &String::from("hashmap"),
        );
        args.source_table_name = generic::get_env_str(
            &args.source_table_name,
            &String::from("DBDIFF_SOURCE_TABLE_NAME"),
            &String::from("t1"),
        );
        args.dest_table_name = generic::get_env_str(
            &args.dest_table_name,
            &String::from("DBDIFF_DESTINATION_TABLE_NAME"),
            &args.source_table_name,
        );
        args.source_query = generic::get_env_str(
            &args.source_query,
            &String::from("DBDIFF_SOURCE_QUERY"),
            &String::from("select * from pg_tables"),
        );
        args.dest_query = generic::get_env_str(
            &args.dest_query,
            &String::from("DBDIFF_DESTINATION_QUERY"),
            &args.source_query,
        );
        args.source_dsn = generic::get_env_str(
            &args.source_dsn,
            &String::from("DBDIFF_SOURCE"),
            &String::from(""),
        );
        args.dest_dsn = generic::get_env_str(
            &args.dest_dsn,
            &String::from("DBDIFF_DESTINATION"),
            &args.source_dsn[..],
        );
        args
    }
}
