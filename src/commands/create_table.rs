use std::sync::Arc;

use clap::{Parser, arg, command};
use deltalake::{DeltaOps, datafusion::prelude::SessionContext};

use crate::schema;

#[derive(Parser, Debug)]
#[command(name = "open", about = "Open a Delta table")]
struct CreateArgs {
    /// Logical name you want to assign
    table_name: String,

    /// s3://bucket/path
    table_path: String,

    /// Schema
    #[arg(long)]
    schema: String,
}

pub async fn create_table_command(ctx: &SessionContext, line: &str) {
    let args = match CreateArgs::try_parse_from(shell_words::split(line).expect("parse failed")) {
        Ok(args) => args,
        Err(e) => {
            e.print().expect("error writing to stderr");
            return;
        }
    };

    let schema = match schema::parser::parse_schema(args.schema.as_str()) {
        Ok(schema) => schema,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };

    let table = DeltaOps::try_from_uri(&args.table_name)
        .await
        .unwrap()
        .create()
        .with_table_name(&args.table_name)
        .with_columns(schema)
        .await
        .unwrap();

    ctx.register_table(args.table_name, Arc::new(table)).unwrap();
    // TODO remove all unwraps
}
