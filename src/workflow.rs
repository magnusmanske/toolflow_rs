use anyhow::{anyhow, Result};
use std::collections::HashMap;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use crate::{mapping::{HeaderMapping, SourceId}, adapter::{QuarryAdapter, Adapter, SparqlAdapter}, APP};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkflowNodeKind {
    Quarry,
    Sparql,
    Join,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowNode {
    pub kind: WorkflowNodeKind,
    pub parameters: HashMap<String,String>,
    pub header_mapping: HeaderMapping,
}

impl WorkflowNode {
    async fn run(&self, input: &HashMap<usize,String>) -> Result<String> {
        match self.kind {
            WorkflowNodeKind::Quarry => {
                let id = self.param_u64("query_id")?;
                QuarryAdapter::default().source2file(&SourceId::QuarryQueryLatest(id), &self.header_mapping).await
            },
            WorkflowNodeKind::Sparql => {
                let sparql = self.param_string("sparql")?;
                SparqlAdapter::default().source2file(&SourceId::Sparql(sparql), &self.header_mapping).await
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum WorkflowNodeStatusValue {
    WAITING,
    RUNNING,
    DONE,
    FAILED,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInput {
    node_id: usize,
    slot: usize,
    uuid: String,
}



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowNodeStatus {
    node_id: usize,
    status: WorkflowNodeStatusValue,
    uuid: String,
}

impl WorkflowNodeStatus {
    fn new(node_id: usize) -> Self {
        Self  { node_id, status: WorkflowNodeStatusValue::WAITING, uuid: String::new() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowEdge {
    pub source_node: usize,
    pub target_node: usize,
    pub target_slot: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub nodes: Vec<WorkflowNode>,
    pub edges: Vec<WorkflowEdge>,
    pub node_status: Vec<WorkflowNodeStatus>,
}

impl Workflow {
    pub fn new(nodes: Vec<WorkflowNode>, edges: Vec<WorkflowEdge>) -> Self {
        let mut ret = Self {
            nodes,
            edges,
            node_status: vec![],
        };
        ret.reset();
        ret
    }

    pub fn reset(&mut self) {
        self.node_status = self.nodes.iter()
            .enumerate()
            .map(|(node_id,_node)| WorkflowNodeStatus::new(node_id) )
            .collect();
    }

    pub fn has_finished(&self) -> bool {
        self.has_completed_succesfully() || self.has_failed()
    }

    pub fn has_failed(&self) -> bool {
        self.node_status.iter().any(|node_status| node_status.status==WorkflowNodeStatusValue::FAILED)
    }

    pub fn has_completed_succesfully(&self) -> bool {
        self.node_status.iter().any(|node_status| node_status.status==WorkflowNodeStatusValue::DONE)
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            println!("{self:?}");
            let nodes_to_run = self.get_next_nodes_to_run();
            if nodes_to_run.is_empty() {
                break;
            }

            let inputs_tmp: Vec<_> = self.edges.iter()
                .filter(|edge|nodes_to_run.contains(&edge.target_node))
                .map(|edge|NodeInput{node_id: edge.target_node, uuid: self.node_status[edge.source_node].uuid.to_owned(), slot:edge.target_slot})
                .collect();
            let mut inputs: HashMap<usize,HashMap<usize,String>> = HashMap::new();
            for node_id in &nodes_to_run {
                inputs.insert(*node_id,HashMap::new());
            }
            for i in inputs_tmp {
                inputs.entry(i.node_id).or_default().insert(i.slot,i.uuid.to_owned());                
            }

            let mut futures = vec![];
            for node_id in &nodes_to_run {
                let future = self.nodes[*node_id].run(inputs.get(node_id).unwrap());
                futures.push(future);
            }
            let results = join_all(futures).await;
            match results.iter().filter(|r|r.is_err()).next() {
                Some(error_result) => match error_result {
                    Ok(_) => {},
                    Err(e) => return Err(anyhow!(e.to_string())),
                }
                None => {},
            }

            let node_file: Vec<_> = results.into_iter()
                .filter_map(|r|r.ok()) // Already checked they are all OK
                .enumerate()
                .map(|(num,uuid)|(nodes_to_run[num],uuid))
                .collect();
            for (node_id,uuid) in node_file {
                self.node_status[node_id].uuid = uuid;
                self.node_status[node_id].status = WorkflowNodeStatusValue::DONE;
            }
        }
        Ok(())
    }

    fn node_open_dependencies(&self, node_id: usize) -> usize {
        self.edges.iter()
            .filter(|edge| edge.target_node==node_id)
            .filter(|edge| self.node_status[edge.source_node].status!=WorkflowNodeStatusValue::DONE)
            .count()
    }

    pub fn get_next_nodes_to_run(&self) -> Vec<usize> {
        self.node_status.iter()
            .filter(|node_status| node_status.status==WorkflowNodeStatusValue::WAITING)
            .filter(|node_status| self.node_open_dependencies(node_status.node_id)==0)
            .map(|node_status|node_status.node_id)
            .collect()
    }
}