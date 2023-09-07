use toolforge::pool::mysql_async::{prelude::*, Conn};
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
    is_output_node: bool,
}

impl WorkflowNodeStatus {
    fn new(node_id: usize) -> Self {
        Self  { node_id, status: WorkflowNodeStatusValue::WAITING, uuid: String::new() , is_output_node: false }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowEdge {
    pub source_node: usize,
    pub target_node: usize,
    pub target_slot: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkflowRun {
    workflow_id: usize,
    nodes_total: usize,
    id: Option<u64>,
    node_status: Vec<WorkflowNodeStatus>,
}

impl WorkflowRun {
    pub fn new(workflow_id: usize, node_ids: Vec<usize>, output_node_ids: Vec<usize>) -> Self {
        let mut ret = Self::default();
        ret.workflow_id = workflow_id;
        ret.nodes_total = node_ids.len();
        ret.node_ids2status(&node_ids, &output_node_ids);
        ret
    }

    pub fn set_id(&mut self, id: u64) {
        self.id = Some(id);
    }

    fn node_ids2status(&mut self, node_ids: &Vec<usize>, output_node_ids: &Vec<usize>) {
        self.node_status = node_ids.iter()
            .map(|node_id| WorkflowNodeStatus::new(*node_id) )
            .collect();

        for node_id in output_node_ids {
            self.node_status[*node_id].is_output_node = true;
        }
    }

    async fn create_new_id(&mut self) -> Result<()> {
        let id = format!("INSERT INTO `run` (`status`,`workflow_id`,`ts_created`,`ts_last`,`nodes_total`) VALUES ('RUN',{},NOW(),NOW(),{})",self.workflow_id,self.nodes_total)
            .with(())
            .run(APP.get_db_connection().await?)
            .await?
            .last_insert_id()
            .ok_or_else(||anyhow!("Can't create a run in the database for workflow {}",self.workflow_id))?;
        self.id = Some(id);
        Ok(())
    }

    pub async fn get_or_create_id(&mut self) -> Result<u64> {
        match self.id {
            Some(id) => Ok(id),
            None => {
                let _ = self.create_new_id().await?;
                let id = self.id.ok_or_else(||anyhow!("Could not create a run ID for workflow {}",self.workflow_id))?;
                Ok(id)
            }
        }
    }

    pub async fn load_status(&mut self, edges: Vec<WorkflowEdge>) -> Result<()> {
        let run_id = self.get_or_create_id().await?;
        let mut conn = APP.get_db_connection().await?;
        let results: Vec<(usize,String)> = "SELECT `uuid`,`node_id` FROM `file` WHERE `run_id`=?"
            .with((run_id,))
            .map(&mut conn, |(uuid,node_id)|(node_id,uuid))
            .await?;
        for (node_id,uuid) in results {
            let ns = self.node_status.iter_mut().filter(|ns|ns.node_id==node_id).next().ok_or_else(||anyhow!("More nodes in files that in node_status for run {run_id}"))?;
            ns.uuid = uuid;
            ns.status = WorkflowNodeStatusValue::DONE;
        }

        // Delete files and reset status of nodes that are dependent on unfinished nodes
        let mut remove_uuids = vec![];
        loop {
            let done: Vec<usize> = self.node_status.iter().filter(|ns|ns.status==WorkflowNodeStatusValue::DONE).map(|ns|ns.node_id).collect();
            let todo: Vec<usize> = self.node_status.iter().filter(|ns|ns.status!=WorkflowNodeStatusValue::DONE).map(|ns|ns.node_id).collect();
            let redo: Vec<usize> = edges.iter() // Edges
                .filter(|edge|done.contains(&edge.target_node)) // where the target is done
                .filter(|edge|todo.contains(&edge.source_node)) // but the source is nopt
                .map(|edge|edge.target_node) // so the target needs to be re-done after the source was
                .collect();
            if redo.is_empty() {
                break;
            }
            for ns in self.node_status.iter_mut().filter(|ns|redo.contains(&ns.node_id)) {
                ns.status = WorkflowNodeStatusValue::WAITING;
                remove_uuids.push(ns.uuid.to_owned());
                ns.uuid = String::new();
            }
        }
        if !remove_uuids.is_empty() {
            for uuid in &remove_uuids {
                let _ = APP.remove_uuid_file(uuid);
            }
            let mut conn = APP.get_db_connection().await?;
            format!("DELETE FROM `file` WHERE `uuid` IN ('{}')",remove_uuids.join("','")).with(()).run(&mut conn).await?;
        }

        Ok(())
    }

    pub fn has_ended(&self) -> bool {
        self.has_completed_succesfully() || self.has_failed()
    }

    pub fn has_failed(&self) -> bool {
        self.node_status.iter().any(|node_status| node_status.status==WorkflowNodeStatusValue::FAILED)
    }

    pub fn has_completed_succesfully(&self) -> bool {
        self.node_status.iter().any(|node_status| node_status.status==WorkflowNodeStatusValue::DONE)
    }

    pub async fn is_cancelled(&mut self, conn: &mut Conn) -> Result<bool> {
        let run_id = self.id.ok_or_else(||anyhow!("WorkflowRun::is_cancelled: No ID set"))?;
        Ok(!"SELECT `id` FROM `run` WHERE `id`=? AND `status`='CANCEL'"
            .with((run_id,))
            .map(conn, |id: u64| id)
            .await?
            .is_empty())
    }

    pub async fn update_status(&self, status: &str, conn: &mut Conn) -> Result<()> {
        let run_id = self.id.ok_or_else(||anyhow!("WorkflowRun::is_cancelled: No ID set"))?;
        let nodes_done = self.node_status.iter().filter(|ns|ns.status==WorkflowNodeStatusValue::DONE).count();
        "UPDATE `run` SET `status`=?,`nodes_done`=? WHERE `id`=?"
            .with((status,nodes_done,run_id))
            .run(conn)
            .await?;
        Ok(())
    }

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    #[serde(skip)]
    pub id: usize,

    #[serde(skip)]
    pub user_id: usize,

    pub nodes: Vec<WorkflowNode>,
    pub edges: Vec<WorkflowEdge>,

    #[serde(skip)]
    pub run: WorkflowRun,
}

impl Workflow {
    pub fn new(nodes: Vec<WorkflowNode>, edges: Vec<WorkflowEdge>, user_id: usize) -> Self {
        let mut ret = Self {
            id: 0,
            user_id,
            nodes,
            edges,
            run: WorkflowRun::default(),
            // node_status: vec![],
        };
        ret.create_run();
        ret
    }

    pub async fn from_id(workflow_id: usize) -> Result<Self> {
        let mut conn = APP.get_db_connection().await?;
        let mut ret = format!("SELECT `json` FROM `workflow` WHERE `id`={workflow_id}")
            .with(())
            .map(&mut conn, |j:String| serde_json::from_str::<Self>(&j).unwrap())
            .await?
            .pop()
            .ok_or_else(||anyhow!("No workflow with id {workflow_id}"))?;
        ret.id = workflow_id;
        ret.create_run();
        Ok(ret)
    }

    fn create_run(&mut self) {
        let node_ids = self.get_all_node_ids();
        let output_node_ids = self.get_output_nodes();
        self.run = WorkflowRun::new(0,node_ids, output_node_ids);
    }

    pub async fn run(&mut self) -> Result<()> {
        let run_id = self.run.get_or_create_id().await?;
        let _ = self.run.load_status(self.edges.to_owned()).await?;
        loop {
            // println!("{self:?}");
            let nodes_to_run = self.get_next_nodes_to_run();
            if nodes_to_run.is_empty() {
                break;
            }

            let inputs_tmp: Vec<_> = self.edges.iter()
                .filter(|edge|nodes_to_run.contains(&edge.target_node))
                .map(|edge|NodeInput{node_id: edge.target_node, uuid: self.run.node_status[edge.source_node].uuid.to_owned(), slot:edge.target_slot})
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
            if let Some(error_result) = results.iter().filter(|r|r.is_err()).next() {
                match error_result {
                    Ok(_) => {},
                    Err(e) => {
                        self.run.update_status("FAIL", &mut APP.get_db_connection().await?).await?;
                        return Err(anyhow!(e.to_string()));
                    }
                }
            }

            let node_file: Vec<_> = results.into_iter()
                .filter_map(|r|r.ok()) // Already checked they are all OK
                .enumerate()
                .map(|(num,uuid)|(nodes_to_run[num],uuid))
                .collect();
            
            let mut conn = APP.get_db_connection().await?;
            if self.run.is_cancelled(&mut conn).await? {
                return Err(anyhow!("User cancelled run"));
            }
            for (node_id,uuid) in node_file {
                let is_output_node = self.run.node_status.iter().filter(|ns|ns.node_id==node_id).map(|ns|ns.is_output_node).next().unwrap_or(false);
                let end_time = if is_output_node { "null" } else { "NOW() + INTERVAL 1 HOUR" };
                format!("INSERT INTO `file` (`uuid`,`expires`,`run_id`,`node_id`,`is_output`) VALUES (?,{end_time},?,?,?)")
                    .with((uuid.to_owned(),run_id,node_id,is_output_node))
                    .run(&mut conn)
                    .await?;
                self.run.node_status[node_id].uuid = uuid;
                self.run.node_status[node_id].status = WorkflowNodeStatusValue::DONE;
            }
            self.run.update_status("RUN", &mut conn).await?;
        }

        self.run.update_status("DONE", &mut APP.get_db_connection().await?).await?;

        Ok(())
    }

    fn get_all_node_ids(&self) -> Vec<usize> {
        self.nodes.iter()
            .enumerate()
            .map(|(id,_)|id)
            .collect()
    }

    fn get_output_nodes(&self) -> Vec<usize> {
        self.nodes.iter()
            .enumerate()
            .map(|(id,_)|id)
            .filter(|node_id| !self.edges.iter().any(|edge|edge.source_node==*node_id))
            .collect()
    }

    fn node_open_dependencies(&self, node_id: usize) -> usize {
        self.edges.iter()
            .filter(|edge| edge.target_node==node_id)
            .filter(|edge| self.run.node_status[edge.source_node].status!=WorkflowNodeStatusValue::DONE)
            .count()
    }

    pub fn get_next_nodes_to_run(&self) -> Vec<usize> {
        self.run.node_status.iter()
            .filter(|node_status| node_status.status==WorkflowNodeStatusValue::WAITING)
            .filter(|node_status| self.node_open_dependencies(node_status.node_id)==0)
            .map(|node_status|node_status.node_id)
            .collect()
    }
}