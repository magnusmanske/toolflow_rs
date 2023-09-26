use std::{collections::HashMap, thread, time::{self, SystemTime}};
use anyhow::{Result, anyhow};
use mediawiki::api::Api;
use regex::Regex;
use serde_json::Value;
use toolforge::pool::mysql_async::{prelude::*, Pool, Conn};
use tokio::sync::RwLock;
use lazy_static::lazy_static;

use crate::{data_file::DataFile, workflow_run::WorkflowNodeStatusValue, workflow::Workflow};

pub const USER_AGENT: &'static str = toolforge::user_agent!("toolflow");
const REQWEST_TIMEOUT: u64 = 60*5;

lazy_static!{
    static ref RE_WEBSERVER_WIKIPEDIA: Regex = Regex::new(r"^(.+)wiki$").expect("Regex error");
    static ref RE_WEBSERVER_WIKI: Regex = Regex::new(r"^(.+)(wik.+)$").expect("Regex error");
}

pub struct App {
    pool: Pool,
    site_matrix: RwLock<HashMap<String,Api>>,
    runs_on_toolforge: bool,
}

impl App {
    pub fn new() -> Self {
        Self {
            pool: Pool::new(toolforge::db::toolsdb("s53704__toolflow".to_string())
                .expect("unable to load db config")
                .to_string()
                .as_str(),),
            site_matrix: RwLock::new(HashMap::new()),
            runs_on_toolforge: std::path::Path::new("~/public_html").exists(),
        }
    }

    pub async fn get_db_connection(&self) -> Result<Conn> {
        Ok(self.pool.get_conn().await?)
    }

    pub fn hold_on(&self) {
        thread::sleep(time::Duration::from_millis(500));
    }

    fn to_compare(&self, s: &str) -> String {
        s.to_lowercase().replace(' ',"_")
    }

    pub async fn get_namespace_id(&self, wiki: &str, ns: &str) -> Option<i64> {
        let ns_to_compare = self.to_compare(ns);
        let site_info = self.get_site_info(wiki).await.ok()?;
        let si = site_info["query"]["namespaces"].as_object()?;
        Some(si.iter()
            .map(|(_ns_id,v)| (v["id"].as_i64(),v["*"].as_str())) // Local namesapces
            .chain(si.iter().map(|(_ns_id,v)| (v["id"].as_i64(),v["canonical"].as_str()))) // Adding canonical namespaces
            .filter(|(ns_id,ns_name)|ns_id.is_some()&&ns_name.is_some())
            .map(|(ns_id,ns_name)|(ns_id.unwrap(),ns_name.unwrap()))
            .filter(|(_ns_id,ns_name)| self.to_compare(ns_name)==ns_to_compare)
            .map(|(ns_id,_ns_name)|ns_id)
            .next()
            .unwrap_or(0))
    }

    pub async fn get_namespace_name(&self, wiki: &str, nsid: i64) -> Option<String> {
        let key = format!("{nsid}");
        let site_info = self.get_site_info(wiki).await.ok()?;
        Some(site_info["query"]["namespaces"][key]["*"].as_str()?.to_string())
    }

    async fn get_site_info(&self, wiki: &str) -> Result<Value> {
        match self.site_matrix.read().await.get(wiki) {
            Some(v) => return Ok(v.get_site_info().to_owned()),
            None => {}
        }
        let mut sm = self.site_matrix.write().await;
        let server = self.get_webserver_for_wiki(wiki).ok_or_else(||anyhow!("Could not find web server for {wiki}"))?;
        let url = format!("https://{server}/w/api.php");
        let api = Api::new(&url).await?;
        let entry = sm.entry(wiki.to_string()).or_insert(api);
        let ret = entry.get_site_info().to_owned();
        Ok(ret)
    }

    pub fn get_webserver_for_wiki(&self, wiki: &str) -> Option<String> {
        match wiki {
            "commonswiki" => Some("commons.wikimedia.org".to_string()),
            "wikidatawiki" => Some("www.wikidata.org".to_string()),
            "specieswiki" => Some("species.wikimedia.org".to_string()),
            "metawiki" => Some("meta.wikimedia.org".to_string()),
            wiki => {
                let wiki = wiki.replace("_","-");
                if let Some(cap1) = RE_WEBSERVER_WIKIPEDIA.captures(&wiki) {
                    if let Some(name) = cap1.get(1) {
                        return Some(format!("{}.wikipedia.org",name.as_str()));
                    }                    
                }
                if let Some(cap2) = RE_WEBSERVER_WIKI.captures(&wiki) {
                    if let (Some(name),Some(domain)) = (cap2.get(1),cap2.get(2)) {
                        return Some(format!("{}.{}.org",name.as_str(),domain.as_str()));
                    }
                }
                None
            }
        }
    }

    pub async fn find_next_waiting_run(&self, conn: &mut Conn) -> Option<(u64,usize)> { // (run_id,workflow_id)
        if let Err(e) = self.activate_scheduled_runs(conn).await {
            eprintln!("{e}");
        }
        "SELECT `id`,`workflow_id` FROM `run` WHERE `status`='WAIT' LIMIT 1"
            .with(())
            .map(conn, |(run_id,workflow_id)| (run_id,workflow_id) )
            .await.ok()?
            .pop()
    }

    async fn activate_scheduled_runs(&self, conn: &mut Conn) -> Result<()> {
        let run_ids = "SELECT `run_id` FROM `scheduler` WHERE `is_active`=1 AND `next_event`<now()"
            .with(())
            .map(&mut (*conn), |run_id: usize| run_id)
            .await?;
        for run_id in run_ids.iter() {
            let _ = self.clear_all_run_results(*run_id, &mut (*conn)).await;
            conn.exec_drop("UPDATE `run` SET `status`='WAIT' WHERE `status`!='RUN' AND `id`=?", (run_id,)).await?;
            conn.exec_drop("UPDATE `scheduler` SET `next_event`=DATE_ADD(now(), INTERVAL 1 DAY) WHERE `interval`='DAILY' AND `is_active`=1 AND `run_id`=?", (run_id,)).await?;
            conn.exec_drop("UPDATE `scheduler` SET `next_event`=DATE_ADD(now(), INTERVAL 1 WEEK) WHERE `interval`='WEEKLY' AND `is_active`=1 AND `run_id`=?", (run_id,)).await?;
            conn.exec_drop("UPDATE `scheduler` SET `next_event`=DATE_ADD(now(), INTERVAL 1 MONTH) WHERE `interval`='MONTHLY' AND `is_active`=1 AND `run_id`=?", (run_id,)).await?;
        }
        Ok(())
    }

    pub async fn clear_old_files(&self, conn: &mut Conn) -> Result<()> {
        let results: Vec<(usize,String)> = "SELECT `id`,`uuid` FROM `file` WHERE `expires`<=NOW()"
            .with(())
            .map(&mut (*conn), |(id,uuid)| (id,uuid) )
            .await?;
        self.remove_files(results, conn).await
    }

    async fn clear_all_run_results(&self, run_id: usize, conn: &mut Conn) -> Result<()> {
        let results: Vec<(usize,String)> = "SELECT `id`,`uuid` FROM `file` WHERE `run_id`=?"
            .with((run_id,))
            .map(&mut (*conn), |(id,uuid)| (id,uuid) )
            .await?;
        self.remove_files(results, conn).await
    }

    async fn remove_files(&self, results: Vec<(usize,String)>, conn: &mut Conn) -> Result<()> {
        let mut ids_to_delete = vec![];
        for (id,uuid) in results {
            match self.remove_uuid_file(&uuid) {
                Ok(_) => ids_to_delete.push(format!("{id}")),
                Err(e) => eprintln!("{e}"),
            }
        }
        if !ids_to_delete.is_empty() {
            format!("DELETE FROM `file` WHERE `id` IN ({})",ids_to_delete.join(",")).with(()).run(conn).await?;
        }
        Ok(())
    }

    pub async fn reset_running_jobs(&self) -> Result<()> {
        let conn = self.get_db_connection().await?;
        match "UPDATE `run` SET `status`='WAIT' WHERE `status`='RUN'".with(()).run(conn).await {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!("{e}")),
        }
    }

    pub fn remove_uuid_file(&self, uuid: &str) -> Result<()> {
        let df = DataFile::new_from_uuid(uuid);
        if let Some(path) = df.path() {
            if let Err(error) = std::fs::remove_file(&path) {
                return Err(anyhow!("Could not delete file {path}: {error}"));
            }
        }
        Ok(())
    }

    pub fn data_path(&self) -> &str {
        if cfg!(test) {
            return "./test_data" // Testing
        } else if self.runs_on_toolforge {
            "/data/project/toolflow/data"
        } else {
            "./tmp" // Local box
        }
    }

    pub fn reqwest_client() -> Result<reqwest::Client> {
        Ok(reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(core::time::Duration::from_secs(REQWEST_TIMEOUT))
            .connection_verbose(true)
            .gzip(true)
            .deflate(true)
            .brotli(true)
            .build()?)
    }

    pub async fn add_user_oauth_to_api(&self, api: &mut Api, user_id: usize) -> Result<()> {
        let conn = self.get_db_connection().await?;
        let oauth = "SELECT `oauth` FROM `user` WHERE `id`=?"
            .with((user_id,))
            .map(conn, |oauth: String| oauth)
            .await?
            .iter()
            .next()
            .ok_or_else(||anyhow!("User {user_id} does not have OAuth information stored"))?
            .to_owned();
        let j: Value = serde_json::from_str(&oauth)?;
        let oauth_params = mediawiki::api::OAuthParams::new_from_json(&j);
        api.set_oauth(Some(oauth_params));
        Ok(())
    }

    pub async fn server(&self) -> Result<()> {
        let _ = self.clear_old_files(&mut self.get_db_connection().await?).await;
        let _ = self.reset_running_jobs().await.expect("Could not reset RUN-state runs to WAIT");
        let mut last_clear_time = SystemTime::now();
    
    
        loop {
            match last_clear_time.elapsed() {
                Ok(elapsed) => {
                    if elapsed.as_secs()>5*60 { // Every 5 minutes
                        let _ = self.clear_old_files(&mut self.get_db_connection().await?).await;
                        last_clear_time = SystemTime::now();
                    }
                }
                Err(_) => {},
            }
    
            let mut conn = self.get_db_connection().await?;
            match self.find_next_waiting_run(&mut conn).await {
                Some((run_id,workflow_id)) => {
                    let mut workflow = match Workflow::from_id(workflow_id).await {
                        Ok(workflow) => workflow,
                        Err(e) => {
                            eprintln!("Cannot get workflow {workflow_id}: {e}");
                            continue;
                        }
                    };
                    workflow.run.set_id(run_id);
                    if let Err(e) = workflow.run.update_status(WorkflowNodeStatusValue::RUNNING, &mut conn).await {
                        eprintln!("Cannot update initial status: {e}");
                        continue;
                    }
                    println!("Starting workflow {workflow_id} run {run_id}");
                    tokio::spawn(async move {
                        println!("Started workflow {workflow_id} run {run_id}");
                        let result = workflow.run().await;
                        println!("Finished workflow {workflow_id} run {run_id}: {result:?}");
                    });
    
                }
                None => self.hold_on(),
            }
        }
    }

}
