use crate::{workflow::*, APP};
use anyhow::{anyhow, Result};
use mysql_async::{from_row, params, prelude::*, Conn};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WorkflowNodeStatusValue {
    WAITING,
    RUNNING,
    DONE,
    FAILED,
    CANCEL,
}

impl WorkflowNodeStatusValue {
    pub fn as_str(&self) -> &str {
        match self {
            WorkflowNodeStatusValue::WAITING => "WAIT",
            WorkflowNodeStatusValue::RUNNING => "RUN",
            WorkflowNodeStatusValue::DONE => "DONE",
            WorkflowNodeStatusValue::FAILED => "FAIL",
            WorkflowNodeStatusValue::CANCEL => "CANCEL",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowNodeStatus {
    pub node_id: usize,
    status: WorkflowNodeStatusValue,
    uuid: String,
    is_output_node: bool,
    #[serde(default)]
    error: Option<String>,
}

impl WorkflowNodeStatus {
    fn new(node_id: usize) -> Self {
        Self {
            node_id,
            status: WorkflowNodeStatusValue::WAITING,
            uuid: String::new(),
            is_output_node: false,
            error: None,
        }
    }

    pub fn done_with_uuid(&mut self, uuid: &str) {
        self.uuid = uuid.to_string();
        self.status = WorkflowNodeStatusValue::DONE;
    }

    pub fn uuid(&self) -> &str {
        &self.uuid
    }

    pub fn set_status(&mut self, status: WorkflowNodeStatusValue, error: Option<String>) {
        self.status = status;
        self.error = error;
    }

    pub fn is_done(&self) -> bool {
        self.status == WorkflowNodeStatusValue::DONE
    }

    pub fn is_waiting(&self) -> bool {
        self.status == WorkflowNodeStatusValue::WAITING
    }

    pub fn is_failed(&self) -> bool {
        self.status == WorkflowNodeStatusValue::FAILED
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkflowRun {
    workflow_id: usize,
    nodes_total: usize,
    id: Option<u64>,
    node_status: Vec<WorkflowNodeStatus>,
    edges: Vec<WorkflowEdge>,
}

impl WorkflowRun {
    pub fn new(workflow: &Workflow) -> Self {
        let mut ret = Self::default();
        ret.workflow_id = workflow.id;
        ret.nodes_total = workflow.nodes.len();
        ret.edges = workflow.edges.to_owned();
        let node_ids = ret.get_all_node_ids(workflow);
        let output_node_ids = ret.get_output_nodes(workflow);
        ret.node_ids2status(&node_ids, &output_node_ids);
        ret
    }

    pub fn set_id(&mut self, id: u64) {
        self.id = Some(id);
    }

    fn get_all_node_ids(&self, workflow: &Workflow) -> Vec<usize> {
        workflow
            .nodes
            .iter()
            .enumerate()
            .map(|(id, _)| id)
            .collect()
    }

    pub fn get_node_status(&self, node_id: usize) -> &WorkflowNodeStatus {
        &self.node_status[node_id]
    }

    pub fn get_node_status_mut(&mut self, node_id: usize) -> &mut WorkflowNodeStatus {
        &mut self.node_status[node_id]
    }

    pub fn is_output_node(&self, node_id: usize) -> bool {
        match self.node_status.get(node_id) {
            Some(ns) => ns.is_output_node,
            None => false,
        }
    }

    fn get_output_nodes(&self, workflow: &Workflow) -> Vec<usize> {
        workflow
            .nodes
            .iter()
            .enumerate()
            .map(|(id, _)| id)
            .filter(|node_id| {
                !workflow
                    .edges
                    .iter()
                    .any(|edge| edge.source_node == *node_id)
            })
            .collect()
    }

    fn node_ids2status(&mut self, node_ids: &Vec<usize>, output_node_ids: &Vec<usize>) {
        self.node_status = node_ids
            .iter()
            .map(|node_id| WorkflowNodeStatus::new(*node_id))
            .collect();

        for node_id in output_node_ids {
            self.node_status[*node_id].is_output_node = true;
        }
    }

    async fn create_new_id(&mut self) -> Result<()> {
        let details = json!(self.node_status).to_string();
        let sql = "INSERT INTO `run` (`status`,`workflow_id`,`ts_created`,`ts_last`,`nodes_total`,`details`) VALUES (':status',:workflow_id,NOW(),NOW(),:nodes_total,:details)";
        let params = params! {"status" => WorkflowNodeStatusValue::RUNNING.as_str(), "workflow_id" => self.workflow_id , "nodes_total" => self.nodes_total, details};
        let mut conn = APP.get_db_connection().await?;
        conn.exec_drop(&sql, params)
            .await
            .map_err(|e| format!("{:?}", e))
            .map_err(|e| anyhow!("{e}"))?;
        let id = conn.last_insert_id().ok_or_else(|| {
            anyhow!(
                "Can't create a run in the database for workflow {}",
                self.workflow_id
            )
        })?;
        self.id = Some(id);
        Ok(())
    }

    pub async fn get_or_create_id(&mut self) -> Result<u64> {
        match self.id {
            Some(id) => Ok(id),
            None => {
                let _ = self.create_new_id().await?;
                let id = self.id.ok_or_else(|| {
                    anyhow!(
                        "Could not create a run ID for workflow {}",
                        self.workflow_id
                    )
                })?;
                Ok(id)
            }
        }
    }

    pub async fn load_status(&mut self) -> Result<()> {
        let run_id = self.get_or_create_id().await?;
        let sql = "SELECT `uuid`,`node_id` FROM `file` WHERE `run_id`=:run_id";
        let results: Vec<(String, usize)> = APP
            .get_db_connection()
            .await?
            .exec_iter(sql, params!(run_id))
            .await?
            .map_and_drop(from_row::<(String, usize)>)
            .await?;
        for (uuid, node_id) in results {
            let ns = self
                .node_status
                .iter_mut()
                .filter(|ns| ns.node_id == node_id)
                .next()
                .ok_or_else(|| {
                    anyhow!("More nodes in files that in node_status for run {run_id}")
                })?;
            ns.uuid = uuid;
            ns.set_status(WorkflowNodeStatusValue::DONE, None);
        }

        // Delete files and reset status of nodes that are dependent on unfinished nodes
        let mut remove_uuids = vec![];
        loop {
            let done: Vec<usize> = self
                .node_status
                .iter()
                .filter(|ns| ns.is_done())
                .map(|ns| ns.node_id)
                .collect();
            let todo: Vec<usize> = self
                .node_status
                .iter()
                .filter(|ns| !ns.is_done())
                .map(|ns| ns.node_id)
                .collect();
            let redo: Vec<usize> = self
                .edges
                .iter() // Edges
                .filter(|edge| done.contains(&edge.target_node)) // where the target is done
                .filter(|edge| todo.contains(&edge.source_node)) // but the source is not
                .map(|edge| edge.target_node) // so the target needs to be re-done after the source was
                .collect();
            if redo.is_empty() {
                break;
            }
            for ns in self
                .node_status
                .iter_mut()
                .filter(|ns| redo.contains(&ns.node_id))
            {
                ns.set_status(WorkflowNodeStatusValue::WAITING, None);
                remove_uuids.push(ns.uuid.to_owned());
                ns.uuid = String::new();
            }
        }
        if !remove_uuids.is_empty() {
            for uuid in &remove_uuids {
                let _ = APP.remove_uuid_file(uuid);
            }
            let mut conn = APP.get_db_connection().await?;
            format!(
                "DELETE FROM `file` WHERE `uuid` IN ('{}')",
                remove_uuids.join("','")
            )
            .with(())
            .run(&mut conn)
            .await?;
        }

        Ok(())
    }

    pub fn has_ended(&self) -> bool {
        self.has_completed_succesfully() || self.has_failed()
    }

    pub fn has_failed(&self) -> bool {
        self.node_status
            .iter()
            .any(|node_status| node_status.is_failed())
    }

    pub fn has_completed_succesfully(&self) -> bool {
        self.node_status
            .iter()
            .any(|node_status| node_status.is_done())
    }

    pub async fn is_cancelled(&mut self, conn: &mut Conn) -> Result<bool> {
        let run_id = self
            .id
            .ok_or_else(|| anyhow!("WorkflowRun::is_cancelled: No ID set"))?;
        Ok(!"SELECT `id` FROM `run` WHERE `id`=? AND `status`=?"
            .with((run_id, WorkflowNodeStatusValue::CANCEL.as_str()))
            .map(conn, |id: u64| id)
            .await?
            .is_empty())
    }

    pub async fn update_status(
        &self,
        status: WorkflowNodeStatusValue,
        conn: &mut Conn,
    ) -> Result<()> {
        let run_id = self
            .id
            .ok_or_else(|| anyhow!("WorkflowRun::is_cancelled: No ID set"))?;
        let details = json!(self.node_status).to_string();
        let nodes_done = self.node_status.iter().filter(|ns| ns.is_done()).count();
        let sql ="UPDATE `run` SET `status`=:status,`nodes_done`=:nodes_done,`details`=:details WHERE `id`=:run_id";
        conn.exec_drop(
            sql,
            params!("status" => status.as_str(), nodes_done, "details" => &details, run_id),
        )
        .await?;
        println!("Workflow {} Run {:?}: {details}", self.workflow_id, self.id);
        Ok(())
    }
}
