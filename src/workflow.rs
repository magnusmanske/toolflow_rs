use toolforge::pool::mysql_async::prelude::*;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use crate::{APP, workflow_run::{WorkflowRun, WorkflowNodeStatusValue}, workflow_node::WorkflowNode, data_file::DataFileDetails};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInput {
    node_id: usize,
    slot: usize,
    uuid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum WorkflowState {
    #[default]
    DRAFT,
    PUBLISHED,
}

impl WorkflowState {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "DRAFT" => Some(WorkflowState::DRAFT),
            "PUBLISHED" => Some(WorkflowState::PUBLISHED),
            _ => None,
        }
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

    #[serde(skip)]
    pub state: WorkflowState,

    #[serde(skip)]
    pub id: usize,

    #[serde(skip)]
    pub user_id: usize,

    #[serde(skip)]
    pub run: WorkflowRun,

    #[serde(skip)]
    name: String,
}

impl Workflow {
    pub fn new(nodes: Vec<WorkflowNode>, edges: Vec<WorkflowEdge>, user_id: usize) -> Self {
        let mut ret = Self {
            id: 0,
            user_id,
            nodes,
            edges,
            state: WorkflowState::default(),
            run: WorkflowRun::default(),
            name: String::default(),
        };
        ret.run = WorkflowRun::new(&ret);
        ret
    }

    pub async fn from_id(workflow_id: usize) -> Result<Self> {
        let mut conn = APP.get_db_connection().await?;
        let (name, mut ret,state) = format!("SELECT `name`,`json`,`state` FROM `workflow` WHERE `id`={workflow_id}")
            .with(())
            .map(&mut conn, |x:(String,String,String)| (
                    x.0.to_owned(),
                    serde_json::from_str::<Self>(&x.1).unwrap(),
                    WorkflowState::from_str(&x.2).unwrap_or_default()
                ) )
            .await?
            .pop()
            .ok_or_else(||anyhow!("No workflow with id {workflow_id}"))?;
        ret.id = workflow_id;
        ret.name = name;
        ret.state = state;
        ret.run = WorkflowRun::new(&ret);
        Ok(ret)
    }

    pub async fn run(&mut self) -> Result<()> {
        let run_id = self.run.get_or_create_id().await?;
        let _ = self.run.load_status().await?;
        loop {
            let nodes_to_run = self.get_next_nodes_to_run();
            if nodes_to_run.is_empty() {
                break;
            }

            let mut inputs: HashMap<usize,HashMap<usize,String>> = nodes_to_run.iter().map(|node_id| (*node_id,HashMap::new())).collect();
            self.edges.iter()
                .filter(|edge|nodes_to_run.contains(&edge.target_node))
                .map(|edge|NodeInput{node_id: edge.target_node, uuid: self.run.get_node_status(edge.source_node).uuid().to_string(), slot:edge.target_slot})
                .for_each(|i| { let _ = inputs.entry(i.node_id).or_default().insert(i.slot,i.uuid.to_owned()); } );

            let futures: Vec<_> = nodes_to_run.iter().map(|node_id|self.nodes[*node_id].run(inputs.get(node_id).unwrap(), self.user_id)).collect();
            let results = join_all(futures).await;

            // Set error for all nodes
            results.iter()
                .zip(nodes_to_run.iter())
                .for_each(|(result,node_id)| {
                    if let Err(e) = result {
                        self.run.get_node_status_mut(*node_id).set_status(WorkflowNodeStatusValue::FAILED,Some(e.to_string()));
                    } else {
                        self.run.get_node_status_mut(*node_id).set_status(WorkflowNodeStatusValue::DONE,None);
                    }
                });

            // Fail on first error
            if let Some(error_result) = results.iter().filter(|r|r.is_err()).next() {
                if let Err(e) = error_result {
                    self.run.update_status(WorkflowNodeStatusValue::FAILED, &mut APP.get_db_connection().await?).await?;
                    return Err(anyhow!(e.to_string()));
                }
            }

            let node_file: Vec<(usize,DataFileDetails)> = results.into_iter()
                .filter_map(|r|r.ok()) // Already checked they are all OK
                .enumerate()
                .map(|(num,dfd)|(nodes_to_run[num],dfd)) // TODO FIXME
                .collect();
            
            let mut conn = APP.get_db_connection().await?;
            if self.run.is_cancelled(&mut conn).await? {
                return Err(anyhow!("User cancelled run"));
            }
            for (node_id,dfd) in node_file {
                if !dfd.is_valid() {
                    continue; // TODO is this the right thing to do?
                }
                let is_output_node = self.run.is_output_node(node_id);
                let end_time = if is_output_node { "null" } else { "NOW() + INTERVAL 1 HOUR" };
                format!("INSERT INTO `file` (`uuid`,`expires`,`run_id`,`node_id`,`is_output`,`rows`) VALUES (?,{end_time},?,?,?,?)")
                    .with((dfd.uuid.to_owned(),run_id,node_id,is_output_node,dfd.rows))
                    .run(&mut conn)
                    .await?;
                self.run.get_node_status_mut(node_id).done_with_uuid(&dfd.uuid);
            }
            self.run.update_status(WorkflowNodeStatusValue::RUNNING, &mut conn).await?;
        }

        self.run.update_status(WorkflowNodeStatusValue::DONE, &mut APP.get_db_connection().await?).await?;

        Ok(())
    }

    fn node_open_dependencies(&self, node_id: usize) -> usize {
        self.edges.iter()
            .filter(|edge| edge.target_node==node_id)
            .filter(|edge| !self.run.get_node_status(edge.source_node).is_done())
            .count()
    }

    pub fn get_next_nodes_to_run(&self) -> Vec<usize> {
        self.nodes.iter()
            .enumerate()
            .map(|(node_id,_)| self.run.get_node_status(node_id))
            .filter(|node_status| node_status.is_waiting())
            .filter(|node_status| self.node_open_dependencies(node_status.node_id)==0)
            .map(|node_status|node_status.node_id)
            .collect()
    }
}