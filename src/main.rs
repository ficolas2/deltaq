use deltalake::datafusion::prelude::SessionContext;
use open_table::open_table_command;
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};

pub mod open_table;

#[tokio::main]
async fn main() -> Result<()> {
    deltalake::aws::register_handlers(None);
    let ctx = SessionContext::new();
    
    let mut rl = DefaultEditor::new()?;
    loop {
        let readline = rl.readline("deltaq> ");
        match readline {
            Ok(line) if matches!(line.trim(), "quit" | "exit" | "\\q") => break,
            Ok(line) if line.starts_with(".") => {
                rl.add_history_entry(line.as_str())?;
                run_command(&ctx, &line).await;
            }
            Ok(line) => {
                rl.add_history_entry(line.as_str())?;
                let df = ctx.sql(&line).await.unwrap();
                println!("{}", df.to_string().await.unwrap());
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
