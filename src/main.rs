use commands::create_table::create_table_command;
use commands::open_table::open_table_command;
use deltalake::datafusion::error::DataFusionError;
use deltalake::datafusion::prelude::SessionContext;
use indoc::indoc;
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};

pub mod commands {
    pub mod create_table;
    pub mod open_table;
}

pub mod schema {
    pub mod parser;
    pub mod tokenizer;
}

#[tokio::main]
async fn main() -> Result<()> {
    deltalake::aws::register_handlers(None);
    let ctx = SessionContext::new();

    let mut sb = "".to_string();

    let mut rl = DefaultEditor::new()?;
    loop {
        let prompt = if sb.is_empty() {
            "deltaq> "
        } else {
            "   ...> "
        };
        let readline = rl.readline(prompt);
        match readline {
            Ok(line) if matches!(line.trim(), "quit" | "exit" | "\\q") => break,
            Ok(line) if line.starts_with(".") => {
                rl.add_history_entry(line.as_str())?;
                run_command(&ctx, &line).await;
            }
            Ok(line) => {
                if sb.is_empty() {
                    sb = line
                } else {
                    sb = format!("{}\n{}", sb, &line);
                }
                if sb.trim_end().ends_with(";") {
                    let query = sb;
                    sb = String::new();

                    rl.add_history_entry(&query)?;
                    let res = ctx.sql(&query).await;
                    match res {
                        Ok(df) => println!("{}", df.to_string().await.unwrap()),
                        Err(e) => match e {
                            DataFusionError::SQL(e, _) => println!("{}", e),
                            _ => println!("{}", e),
                        },
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
            }
            Err(ReadlineError::Eof) => {
                println!("");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    Ok(())
}

async fn run_command(ctx: &SessionContext, line: &str) {
    let args = shell_words::split(line).expect("parse failed");

    match args[0].as_str() {
        ".help" => {
            print!(indoc!(r#"
                Commands: (type any command for more help about the specific command)
                .open <TABLE_NAME> <TABLE_PATH>
                    Open a table at a path, and give it a name.
                    For more info, like specifying S3 see the .open help
                .create --schema <schema> <TABLE_NAME> <TABLE_PATH> 
                    Create a table from a given schema
                .tables
                    Display all opened tables
                .schema <TABLE_NAME>
                    Display the schema for a given table
            "#))
        }
        ".open" => {
            open_table_command(ctx, line).await;
        }
        ".create" => {
            create_table_command(ctx, line).await;
        }
        ".tables" => {
            for table_name in &ctx
                .catalog("datafusion")
                .unwrap()
                .schema("public")
                .unwrap()
                .table_names()
            {
                println!("{}", table_name);
            }
        }
        _ => {}
    }
}
