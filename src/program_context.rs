use std::sync::Arc;

use deltalake::{datafusion::{common::HashMap, prelude::SessionContext}, DeltaTable};

pub struct ProgramContext {
    pub df_ctx: SessionContext,
    pub tables: HashMap<String, Arc<DeltaTable>>,
}

impl ProgramContext {
    pub fn new() -> ProgramContext {
        ProgramContext {
            df_ctx: SessionContext::new(),
            tables: HashMap::new(),
        }
    }

    pub async fn refresh_table(&mut self, table_name: &str, table: DeltaTable) {
        let table = Arc::new(table);
        self.tables.insert(table_name.to_string(), table.clone());

        let _ = self.df_ctx.deregister_table(table_name);
        // TODO remove unwarp
        self.df_ctx.register_table(table_name, table).unwrap();
    }
}

impl Default for ProgramContext {
    fn default() -> Self {
        Self::new()
    }
}
