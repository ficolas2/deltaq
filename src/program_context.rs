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
}

impl Default for ProgramContext {
    fn default() -> Self {
        Self::new()
    }
}
