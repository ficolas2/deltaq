use commands::open_table::open_table_command;
use deltalake::datafusion::error::DataFusionError;
use deltalake::datafusion::prelude::SessionContext;
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};
pub mod commands {
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
        let prompt = if sb.is_empty() { "deltaq> " } else { "   ...> " };
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
        ".open" => {
            open_table_command(ctx, line).await;
        }
        _ => {}
    }
}
