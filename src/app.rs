use std::{fs::File, io::{Write, Seek}, collections::HashMap, thread, time, env};
use anyhow::{Result, anyhow};
use serde_json::json;
use tempfile::*;
use toolforge::pool::mysql_async::{prelude::*, Pool, Conn};

use crate::{data_file::{DataFile, DataFileDetails}, data_header::DataCell};

pub const USER_AGENT: &'static str = toolforge::user_agent!("toolflow");
const REQWEST_TIMEOUT: u64 = 60;

pub struct App {
    pool: Pool,
}

impl App {
    pub fn new() -> Self {
        Self {
            pool: Pool::new(toolforge::db::toolsdb("s53704__toolflow_p".to_string())
                .expect("unable to load db config")
                .to_string()
                .as_str(),)
        }
    }

    pub async fn get_db_connection(&self) -> Result<Conn> {
        Ok(self.pool.get_conn().await?)
    }

    pub fn hold_on(&self) {
        thread::sleep(time::Duration::from_millis(500));
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
                if path.to_string_lossy().contains("/project/") {
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

    pub fn inner_join_on_key(&self, uuids: Vec<&str>, key: &str) -> Result<DataFileDetails> {
        if uuids.is_empty() {
            return Err(anyhow!("No UUIDs given to inner_join_on_key"));
        }
        if uuids.len()==1 { // TODO Maybe just duplicate this file? Need to check for key presence first?
            return Err(anyhow!("Only one UUID given to inner_join_on_key"));
        }
        let mut files: Vec<_> = uuids.iter()
            .map(|uuid|(uuid,DataFile::default(),0))
            .collect();
        for (uuid,file,size) in &mut files {
            file.open_input_file(uuid)?;
            *size = file.file_size().ok_or(anyhow!("{} has no file size",file.path().unwrap()))?;
        }
        files.sort_by_key(|k| k.2);

        let mut main_file = files.remove(0).1;
        main_file.load()?;
        let key2row = main_file.key2row(key)?;
        let mut keys_found: HashMap<String,usize> = HashMap::new();
        for (_uuid,file,_size) in files.iter_mut() {
            file.load_header()?;
            let mut new_header = file.header().to_owned();
            let key_col_num = new_header.get_col_num(key).ok_or(anyhow!("No key '{key} in file {}",file.path().unwrap()))?;
            new_header.columns.remove(key_col_num);
            main_file.add_header(new_header);

            loop {
                let row = match file.read_row() {
                    Some(row) => row,
                    None => break,
                };
                let mut row: Vec<DataCell> = serde_json::from_str(&row)?;
                let new_key = match row.get(key_col_num) {
                    Some(new_key) => new_key,
                    None => continue, // Ignore blank key
                }.as_key();
                let row_id = match key2row.get(&new_key) {
                    Some(id) => *id,
                    None => continue , // Not in the first file
                };
                *keys_found.entry(new_key.to_owned()).or_insert(0) += 1;
                row.remove(key_col_num);
                main_file.rows[row_id].append(&mut row);
            }    
        }
        let keys_in_all_files: Vec<&String> = keys_found.iter()
            .filter(|(_,count)|**count==files.len())
            .map(|(key_name,_)|key_name)
            .collect();
        
        let mut output_file = DataFile::default();
        output_file.open_output_file()?;
        output_file.write_json_row(&json!(main_file.header()))?;
        for key in keys_in_all_files {
            let row_id = match key2row.get(key) {
                Some(id) => *id,
                None => continue,
            };
            let row = match main_file.rows.get(row_id) {
                Some(row) => row,
                None => continue,
            };
            output_file.write_json_row(&json!(row))?;
        }
        Ok(output_file.details())
    }
}
