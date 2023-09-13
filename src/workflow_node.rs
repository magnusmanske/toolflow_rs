use anyhow::{anyhow, Result};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::{mapping::{HeaderMapping, SourceId}, adapter::{QuarryAdapter, Adapter, SparqlAdapter, PetScanAdapter}, APP};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkflowNodeKind {
    Quarry,
    Sparql,
    PetScan,
    Join,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowNode {
    pub kind: WorkflowNodeKind,
    pub parameters: HashMap<String,String>,
    pub header_mapping: HeaderMapping,
}

impl WorkflowNode {
    pub async fn run(&self, input: &HashMap<usize,String>) -> Result<String> {
        match self.kind {
            WorkflowNodeKind::Quarry => {
                let id = self.param_u64("query_id")?;
                QuarryAdapter::default().source2file(&SourceId::QuarryQueryLatest(id), &self.header_mapping).await
            },
            WorkflowNodeKind::Sparql => {
                let sparql = self.param_string("sparql")?;
                SparqlAdapter::default().source2file(&SourceId::Sparql(sparql), &self.header_mapping).await
            },
            WorkflowNodeKind::PetScan => {
                let id = self.param_u64("psid")?;
                PetScanAdapter::default().source2file(&&SourceId::PetScan(id), &self.header_mapping).await
            },
            WorkflowNodeKind::Join => {
                let mode = self.param_string("mode")?;
                match mode.as_str() {
                    "inner_join_on_key" => {
                        let join_key = self.param_string("join_key")?;
                        let uuids: Vec<&str> = input.iter().map(|(_slot,uuid)|uuid.as_str()).collect();
                        APP.inner_join_on_key(uuids,&join_key)
                    }
                    other => Err(anyhow!("Unknown join mode '{other}'"))
                }
            }
        }
    }

    fn param_string(&self, key: &str) -> Result<String> {
        self.parameters.get(key).map(|s|s.to_owned()).ok_or_else(||anyhow!("Parameter '{key}' not found"))
    }

    fn param_u64(&self, key: &str) -> Result<u64> {
        Ok(self.parameters.get(key).ok_or_else(||anyhow!("Parameter '{key}' not found"))?.parse::<u64>()?)
    }
}
