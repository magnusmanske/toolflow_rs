use std::{fs::File, io::{Write, Seek}, collections::HashMap, thread, time, env};
use anyhow::{Result, anyhow};
use mediawiki::api::Api;
use regex::Regex;
use serde_json::Value;
use tempfile::*;
use toolforge::pool::mysql_async::{prelude::*, Pool, Conn};
use tokio::sync::RwLock;
use lazy_static::lazy_static;

use crate::data_file::DataFile;

pub const USER_AGENT: &'static str = toolforge::user_agent!("toolflow");
const REQWEST_TIMEOUT: u64 = 60*5;

lazy_static!{
    static ref RE_WEBSERVER_WIKIPEDIA: Regex = Regex::new(r"^(.+)wiki$").expect("Regex error");
    static ref RE_WEBSERVER_WIKI: Regex = Regex::new(r"^(.+)(wik.+)$").expect("Regex error");
}

pub struct App {
    pool: Pool,
    site_matrix: RwLock<HashMap<String,Api>>,
}

impl App {
    pub fn new() -> Self {
        Self {
            pool: Pool::new(toolforge::db::toolsdb("s53704__toolflow".to_string())
                .expect("unable to load db config")
                .to_string()
                .as_str(),),
            site_matrix: RwLock::new(HashMap::new()),
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

    fn get_webserver_for_wiki(&self, wiki: &str) -> Option<String> {
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
        "SELECT `id`,`workflow_id` FROM `run` WHERE `status`='WAIT' LIMIT 1"
            .with(())
            .map(conn, |(run_id,workflow_id)| (run_id,workflow_id) )
            .await.ok()?
            .pop()
    }

    pub async fn clear_old_files(&self) -> Result<()> {
        let mut conn = self.get_db_connection().await?;
        let results: Vec<(usize,String)> = "SELECT `id`,`uuid` FROM `file` WHERE `expires`<=NOW()"
            .with(())
            .map(&mut conn, |(id,uuid)| (id,uuid) )
            .await?;
        drop(conn);
        let mut ids_to_delete = vec![];
        for (id,uuid) in results {
            match self.remove_uuid_file(&uuid) {
                Ok(_) => ids_to_delete.push(format!("{id}")),
                Err(e) => eprintln!("{e}"),
            }
        }
        if !ids_to_delete.is_empty() {
            let mut conn = self.get_db_connection().await?;
            format!("DELETE FROM `file` WHERE `id` IN ({})",ids_to_delete.join(",")).with(()).run(&mut conn).await?;
        }
        Ok(())
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
        match env::current_dir() {
            Ok(path) => {
                if cfg!(test) {
                    return "./test_data" // Testing
                } else if path.to_string_lossy().contains("/project/") {
                    "/data/project/toolflow/data"
                } else {
                    "./tmp" // Local box
                }
            },
            Err(_) => "./tmp",
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

    /// Queries SPARQL and returns a filename with the result as CSV.
    pub async fn load_sparql_csv(&self, sparql: &str) -> Result<csv::Reader<File>> {
        let url = format!("https://query.wikidata.org/sparql?query={}",sparql);
        let mut f = tempfile()?;
        let mut res = Self::reqwest_client()?
            .get(url)
            .header(reqwest::header::ACCEPT, reqwest::header::HeaderValue::from_str("text/csv")?)
            .send()
            .await?;
        while let Some(chunk) = res.chunk().await? {
            f.write_all(chunk.as_ref())?;
        }
        f.seek(std::io::SeekFrom::Start(0))?;
        Ok(csv::ReaderBuilder::new()
            .flexible(true)
            .has_headers(true)
            .delimiter(b',')
            .from_reader(f))

        /* HOWTO use:
        let mut reader = self.mnm.load_sparql_csv(&sparql).await?;
        for result in reader.records() {
            let record = result.unwrap();
        }*/
    }

}
