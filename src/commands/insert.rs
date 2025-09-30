use clap::Parser;
use deltalake::{writer::{DeltaWriter, JsonWriter, WriteMode}, DeltaTable};
use serde_json::Value;

use crate::program_context::ProgramContext;

#[derive(Parser, Debug)]
#[command(name = "open", about = "Open a Delta table")]
struct InsertArgs {
    /// Table name
    table_name: String,

    /// The values to be added in json format. Can be an object, representing
    /// a single row, or an array
    #[arg(long)]
    json: String,
}

pub async fn insert_command(ctx: &mut ProgramContext, line: &str) {
    let args = match InsertArgs::try_parse_from(shell_words::split(line).expect("parse failed")) {
        Ok(args) => args,
        Err(e) => {
            e.print().expect("error writing to stderr");
            return;
        }
    };

    let Some(table) = ctx.tables.get(&args.table_name) else {
        eprintln!("Error: table '{}' not found", args.table_name);
        return;
    };

    let json: Value = match serde_json::from_str(&args.json) {
        Ok(json) => json,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };

    let values: Vec<_> = if let Some(arr) = json.as_array() {
        arr.iter().cloned().collect()
    } else {
        vec![json]
    };

    let len = values.len();

    // This clone is sloppy
    let mut table: DeltaTable = table.as_ref().clone();

    let mut writer = JsonWriter::for_table(&table).unwrap();

    writer
        .write_with_mode(values, WriteMode::Default)
        .await
        .unwrap();

    writer.flush_and_commit(&mut table).await.unwrap();
    ctx.refresh_table(&args.table_name, table).await;

    println!("Written {} records", len);
}
