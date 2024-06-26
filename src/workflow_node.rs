use crate::{
    adapter::*,
    data_file::DataFileDetails,
    filter::{Filter, FilterPetScan, FilterSort},
    generator::Generator,
    join::Join,
    mapping::{HeaderMapping, SourceId},
    renderer::{Renderer, RendererWikitext},
};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkflowNodeKind {
    // QuarryQueryRun,
    QuarryQueryLatest,
    Sparql,
    PetScan,
    PagePile,
    AListBuildingTool,
    UserEdits,
    WdFist,
    Join,
    Filter,
    FilterPetScan,
    FilterSort,
    Generator,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowNode {
    pub kind: WorkflowNodeKind,
    pub parameters: HashMap<String, Value>,
    pub header_mapping: HeaderMapping,
}

impl WorkflowNode {
    pub async fn run(
        &self,
        input: &HashMap<usize, String>,
        user_id: usize,
    ) -> Result<DataFileDetails> {
        match self.kind {
            WorkflowNodeKind::QuarryQueryLatest => {
                let id = self.param_u64("quarry_query_id")?;
                QuarryQueryAdapter::default()
                    .source2file(&SourceId::QuarryQueryLatest(id), &self.header_mapping)
                    .await
            }
            WorkflowNodeKind::Sparql => {
                let sparql = self.param_string("sparql")?;
                SparqlAdapter::default()
                    .source2file(&SourceId::Sparql(sparql), &self.header_mapping)
                    .await
            }
            WorkflowNodeKind::PetScan => {
                let id = self.param_u64("psid")?;
                PetScanAdapter::default()
                    .source2file(&&SourceId::PetScan(id), &self.header_mapping)
                    .await
            }
            WorkflowNodeKind::PagePile => {
                let id = self.param_u64("pagepile_id")?;
                PagePileAdapter::default()
                    .source2file(&&SourceId::PagePile(id), &self.header_mapping)
                    .await
            }
            WorkflowNodeKind::WdFist => {
                let url = self.param_string("wdfist_url")?;
                WdFistAdapter::default()
                    .source2file(&&SourceId::WdFist(url), &self.header_mapping)
                    .await
            }
            WorkflowNodeKind::AListBuildingTool => {
                let wiki = self.param_string("wiki")?;
                let qid = self.param_string("qid")?;
                let id = (wiki, qid);
                AListBuildingToolAdapter::default()
                    .source2file(&&SourceId::AListBuildingTool(id), &self.header_mapping)
                    .await
            }
            WorkflowNodeKind::UserEdits => {
                let url = self.param_string("user_edits_url")?;
                UserEditsAdapter::default()
                    .source2file(&&SourceId::UserEdits(url), &self.header_mapping)
                    .await
            }
            WorkflowNodeKind::Join => {
                let mode = self.param_string("mode")?;
                match mode.as_str() {
                    "inner_join_on_key" => {
                        let join_key = self.param_string("join_key")?;
                        let uuids: Vec<&str> =
                            input.iter().map(|(_slot, uuid)| uuid.as_str()).collect();
                        Join::default().inner_join_on_key(uuids, &join_key)
                    }
                    "merge_unique" => {
                        let join_key = self.param_string("join_key")?;
                        let uuids: Vec<&str> =
                            input.iter().map(|(_slot, uuid)| uuid.as_str()).collect();
                        Join::default().merge_unique(uuids, &join_key)
                    }
                    other => Err(anyhow!("Unknown join mode '{other}'")),
                }
            }
            WorkflowNodeKind::Filter => {
                let operator = self.param("operator")?;
                let filter = Filter {
                    key: self.param_string("key")?,
                    subkey: self.param_string("subkey").ok(),
                    operator: serde_json::from_str(&operator.to_string())
                        .map_err(|_| anyhow!("Invaid operator {operator}"))?,
                    value: self.param_string("value")?,
                    remove_matching: self.param_bool("remove_matching").unwrap_or(false),
                };
                let uuids: Vec<&str> = input.iter().map(|(_slot, uuid)| uuid.as_str()).collect();
                match uuids.len() {
                    0 => Err(anyhow!("Filter has no input")),
                    1 => filter.process(&uuids[0]).await,
                    other => Err(anyhow!("Filter has {other} inputs, should only have one")),
                }
            }
            WorkflowNodeKind::FilterSort => {
                let filter = FilterSort {
                    key: self.param_string("key")?,
                    reverse: self.param_bool("reverse")?, //self.param_u64("reverse")?>0,
                };
                let uuids: Vec<&str> = input.iter().map(|(_slot, uuid)| uuid.as_str()).collect();
                match uuids.len() {
                    0 => Err(anyhow!("FilterSort has no input")),
                    1 => filter.process(&uuids[0]).await,
                    other => Err(anyhow!(
                        "FilterSort has {other} inputs, should only have one"
                    )),
                }
            }
            WorkflowNodeKind::FilterPetScan => {
                let filter = FilterPetScan {
                    key: self.param_string("key")?,
                    psid: self.param_u64("psid")?,
                };
                let uuids: Vec<&str> = input.iter().map(|(_slot, uuid)| uuid.as_str()).collect();
                match uuids.len() {
                    0 => Err(anyhow!("FilterPetScan has no input")),
                    1 => filter.process(&uuids[0]).await,
                    other => Err(anyhow!(
                        "FilterPetScan has {other} inputs, should only have one"
                    )),
                }
            }
            WorkflowNodeKind::Generator => {
                let mode = self.param_string("mode")?;
                match mode.as_str() {
                    "wikipage" => {
                        let uuid = input
                            .iter()
                            .map(|(_slot, uuid)| uuid.as_str())
                            .next()
                            .ok_or_else(|| anyhow!("No inputs for this node"))?;
                        let wiki = self.param_string("wiki")?;
                        let page = self.param_string("page")?;
                        let wikitext = RendererWikitext::default().render_from_uuid(&uuid)?;
                        Generator::wikipage(&wikitext, &wiki, &page, user_id).await
                    }
                    other => Err(anyhow!("Unknown join mode '{other}'")),
                }
            }
        }
    }

    fn param(&self, key: &str) -> Result<&Value> {
        self.parameters
            .get(key)
            .ok_or_else(|| anyhow!("Parameter '{key}' not found"))
    }

    fn param_string(&self, key: &str) -> Result<String> {
        self.param(key)?
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("Parameter '{key}' not found"))
    }

    fn param_u64(&self, key: &str) -> Result<u64> {
        if let Some(ret) = self.param(key)?.as_u64() {
            return Ok(ret);
        }
        let ret = self.param(key)?.as_str().map(|s| s.parse::<u64>().ok());
        let ret = ret.ok_or_else(|| anyhow!("Parameter '{key}' not a str"))?;
        ret.ok_or_else(|| anyhow!("Parameter '{key}' not a u64"))
    }

    fn param_bool(&self, key: &str) -> Result<bool> {
        if let Some(ret) = self.param(key)?.as_bool() {
            return Ok(ret);
        }
        if let Ok(ret) = self.param_u64(key) {
            return Ok(ret > 0);
        }
        Err(anyhow!("Parameter '{key}' not a boolean or u64"))
    }
}
