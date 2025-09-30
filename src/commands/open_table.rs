use std::{collections::HashMap, sync::Arc};

use clap::{Parser, arg, command};
use deltalake::open_table_with_storage_options;

use crate::program_context::ProgramContext;

#[derive(Parser, Debug)]
#[command(name = "open", about = "Open a Delta table")]
struct OpenArgs {
    /// Logical name you want to assign
    table_name: String,

    /// s3://bucket/path
    table_path: String,

    /// http://host:port for MinIO / S3 endpoint
    #[arg(long)]
    endpoint_url: Option<String>,

    /// Access key
    #[arg(long)]
    access_key_id: Option<String>,

    /// Secret key
    #[arg(long)]
    secret_access_key: Option<String>,

    /// true/false
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    allow_http: bool,

    /// S3 addressing style: "path" or "virtual"
    #[arg(long, value_parser = ["path", "virtual"], default_value = "path")]
    addressing_style: String,

    /// Conditional write mechanism: "etag" (MinIO) or "dynamodb" (AWS)
    #[arg(long, value_parser = ["etag", "dynamodb"], default_value = "etag")]
    conditional_put: String,
}

pub async fn open_table_command(ctx: &mut ProgramContext, line: &str) {
    let args = match OpenArgs::try_parse_from(shell_words::split(line).expect("parse failed")) {
        Ok(args) => args,
        Err(e) => {
            e.print().expect("error writing to stderr");
            return;
        }
    };

    let mut storage_options: HashMap<String, String> = HashMap::new();

    if let Some(v) = args.endpoint_url {
        storage_options.insert("AWS_ENDPOINT_URL".into(), v);
    }
    if let Some(v) = args.access_key_id {
        storage_options.insert("AWS_ACCESS_KEY_ID".into(), v);
    }
    if let Some(v) = args.secret_access_key {
        storage_options.insert("AWS_SECRET_ACCESS_KEY".into(), v);
    }

    storage_options.insert("AWS_ALLOW_HTTP".into(), args.allow_http.to_string());
    storage_options.insert("AWS_S3_ADDRESSING_STYLE".into(), args.addressing_style);
    storage_options.insert("aws_conditional_put".into(), args.conditional_put);

    let table = match open_table_with_storage_options(args.table_path, storage_options).await {
        Ok(table) => Arc::new(table),
        Err(e) => {
            println!("{}", e);
            return;
        }
    };

    ctx.tables.insert(args.table_name.clone(), table.clone());
    ctx.df_ctx.register_table(args.table_name, table).unwrap();
}
