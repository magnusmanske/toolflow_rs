use std::{fs::File, io::{Write, Seek}};
use anyhow::{Result, anyhow};
use tempfile::*;

use crate::data_file::DataFile;

pub const USER_AGENT: &'static str = "ToolFlow/0.1";
const REQWEST_TIMEOUT: u64 = 60;

pub struct App {

}

impl App {
    pub fn data_path(&self) -> &str {
        "./tmp"
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

    pub fn inner_join_on_key(&self, uuids: Vec<&str>, _key: &str) -> Result<String> {
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
        println!("{files:?}");
        files[1].1.load()?;
        Ok("".to_string())
    }
}
