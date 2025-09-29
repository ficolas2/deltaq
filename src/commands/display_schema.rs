use deltalake::datafusion::prelude::SessionContext;

use clap::Parser;

use crate::utils::data_type::arrow_type_to_delta_str;

#[derive(Parser, Debug)]
#[command(name = "open", about = "Open a Delta table")]
struct SchemaArgs {
    /// Name for the loaded table
    table_name: String,
}

pub async fn display_schema_command(ctx: &SessionContext, line: &str) {
    let args = match SchemaArgs::try_parse_from(shell_words::split(line).expect("parse failed")) {
        Ok(args) => args,
        Err(e) => {
            e.print().expect("error writing to stderr");
            return;
        }
    };

    let df = ctx.table(args.table_name).await.unwrap().limit(0, Some(0));
    let schema = match &df {
        Ok(df) => df.schema(),
        Err(e) => { 
            println!("{}", e);
            return;
        },
    };

    for f in schema.fields() {
        match arrow_type_to_delta_str(f.data_type()) {
            Ok(t) => println!("  {}: {}", f.name(), t),
            Err(e) => println!("Error on column {}: {}", f.name(), e),
        }
    }
    let metadata = schema.metadata();
    if !metadata.is_empty() {
        println!("Metadata: ");
        for (k, v) in metadata.iter() {
            println!("  {}: {}", k, v);
        }
    }
}

