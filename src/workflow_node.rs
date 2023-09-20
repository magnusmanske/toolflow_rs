use anyhow::{anyhow, Result};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::{filter::Filter, mapping::{HeaderMapping, SourceId}, adapter::{QuarryQueryAdapter, Adapter, SparqlAdapter, PetScanAdapter, PagePileAdapter, AListBuildingToolAdapter}, APP, data_file::DataFileDetails};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkflowNodeKind {
    QuarryQueryRun,
    // QuarryQueryLatest,
    Sparql,
    PetScan,
    PagePile,
    AListBuildingTool,
    Join,
    Filter,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowNode {
    pub kind: WorkflowNodeKind,
    pub parameters: HashMap<String,String>,
    pub header_mapping: HeaderMapping,
}

impl WorkflowNode {
    pub async fn run(&self, input: &HashMap<usize,String>) -> Result<DataFileDetails> {
        match self.kind {
            WorkflowNodeKind::QuarryQueryRun => {
                let id = self.param_u64("quarry_query_id")?;
                QuarryQueryAdapter::default().source2file(&SourceId::QuarryQueryLatest(id), &self.header_mapping).await
            },
            WorkflowNodeKind::Sparql => {
                let sparql = self.param_string("sparql")?;
                SparqlAdapter::default().source2file(&SourceId::Sparql(sparql), &self.header_mapping).await
            },
            WorkflowNodeKind::PetScan => {
                let id = self.param_u64("psid")?;
                PetScanAdapter::default().source2file(&&SourceId::PetScan(id), &self.header_mapping).await
            },
            WorkflowNodeKind::PagePile => {
                let id = self.param_u64("pagepile_id")?;
                PagePileAdapter::default().source2file(&&SourceId::PagePile(id), &self.header_mapping).await
            },
            WorkflowNodeKind::AListBuildingTool => {
                let wiki = self.param_string("wiki")?;
                let qid = self.param_string("qid")?;
                let id = (wiki,qid);
                AListBuildingToolAdapter::default().source2file(&&SourceId::AListBuildingTool(id), &self.header_mapping).await
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
            },
            WorkflowNodeKind::Filter => {
                let operator = self.param_string("value").unwrap_or_default();
                let filter = Filter {
                    key: self.param_string("key")?,
                    subkey: self.param_string("join_key").ok(),
                    operator: serde_json::from_str(&operator)?,
                    value: self.param_string("value")?,
                    remove_matching: self.param_bool("remove_matching")?,
                };
                let uuids: Vec<&str> = input.iter().map(|(_slot,uuid)|uuid.as_str()).collect();
                match uuids.len() {
                    0 => Err(anyhow!("Filter has no input")),
                    1 => filter.process(&uuids[0]).await,
                    other => Err(anyhow!("Filter has {other} inputs, should only have one")),
                }
            },
        }
    }

    fn param_string(&self, key: &str) -> Result<String> {
        self.parameters.get(key).map(|s|s.to_owned()).ok_or_else(||anyhow!("Parameter '{key}' not found"))
    }

    fn param_u64(&self, key: &str) -> Result<u64> {
        Ok(self.parameters.get(key).ok_or_else(||anyhow!("Parameter '{key}' not found"))?.parse::<u64>()?)
    }

    fn param_bool(&self, key: &str) -> Result<bool> {
        Ok(self.parameters.get(key).ok_or_else(||anyhow!("Parameter '{key}' not found"))?.parse::<bool>()?)
    }
}
