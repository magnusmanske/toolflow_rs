use std::io::{BufWriter, Write};
use std::{fs::File, collections::HashMap};
use async_trait::async_trait;
use anyhow::{anyhow, Result};
use serde_json::{Value, json};

use crate::data_file::DataFile;
use crate::mapping::{HeaderMapping, SourceId};
use crate::{data_header::*, APP};


#[async_trait]
pub trait Adapter {
    async fn source2file(&mut self, source: &SourceId, mapping: &HeaderMapping) -> Result<String>;
    fn writer(&mut self) -> Result<&mut BufWriter<File>>;
    fn add_output_row(&mut self, v: &Value) -> Result<()> {
        let fh = self.writer()?;
        fh.write(v.to_string().as_bytes())?;
        fh.write(b"\n")?;
        Ok(())
    }
}


#[derive(Debug, Default)]
pub struct SparqlAdapter {
    file: DataFile,
}

#[async_trait]
impl Adapter for SparqlAdapter {
    fn writer(&mut self) -> Result<&mut BufWriter<File>> {
        if !self.file.is_output_open() {
            self.file.open_output_file()?;
        }
        self.file.writer()
    }

    async fn source2file(&mut self, source: &SourceId, mapping: &HeaderMapping) -> Result<String> {
        let sparql = match source {
            SourceId::Sparql(sparql) => sparql,
            _ => return Err(anyhow!("Unsuitable source type for SPARQL: {source:?}")),
        };
        let mut reader = APP.load_sparql_csv(&sparql).await?;
        let labels: Vec<String> = reader.headers()?.iter().map(|s|s.to_string()).collect();
        let label2col_num: HashMap<String,usize> = labels.into_iter().enumerate().map(|(colnum,header)|(header,colnum)).collect();

        self.add_output_row(&json!{mapping.as_data_header()})?; // Output new header

        for result in reader.records() {
            let row = match result {
                Ok(row) => row,
                Err(_) => continue, // Ignore row
            };

            let mut jsonl_row = vec![];
            for cm in &mapping.data {
                if let Some((source_label,element_name)) = cm.mapping.get(0) {
                    if let Some(col_num) = label2col_num.get(source_label) {
                        if let Some(text) = row.get(*col_num) {
                            let j = json!(text);
                            let dc = DataCell::from_value(&j,&cm.header, &element_name);
                            jsonl_row.push(dc);
                            continue;
                        }
                    }
                }
                jsonl_row.push(None);
            }
            self.add_output_row(&json!{jsonl_row})?; // Output data row
        }
        Ok(self.file.uuid().as_ref().unwrap().to_string())
    }
}


#[derive(Debug, Default)]
pub struct QuarryAdapter {
    file: DataFile,
}

#[async_trait]
impl Adapter for QuarryAdapter {
    fn writer(&mut self) -> Result<&mut BufWriter<File>> {
        if !self.file.is_output_open() {
            self.file.open_output_file()?;
        }
        self.file.writer()
    }

    async fn source2file(&mut self, source: &SourceId, mapping: &HeaderMapping) -> Result<String> {
        let url = match source {
            SourceId::QuarryQueryRun(id) => format!("https://quarry.wmcloud.org/run/{id}/output/0/json"),
            SourceId::QuarryQueryLatest(id) => format!("https://quarry.wmcloud.org/query/{id}/result/latest/0/json"),
            _ => return Err(anyhow!("Unsuitable source type for Quarry: {source:?}")),
        };
        let j: Value = reqwest::get(url).await?.json().await?;
        let labels: Vec<String> = j["headers"].as_array().ok_or(anyhow!("JSON has no header array"))?.iter().map(|s|s.as_str().unwrap_or("").to_string()).collect();
        let label2col_num: HashMap<String,usize> = labels.into_iter().enumerate().map(|(colnum,header)|(header,colnum)).collect();
        
        self.add_output_row(&json!{mapping.as_data_header()})?; // Output new header
        for row in j["rows"].as_array().ok_or(anyhow!("JSON has no rows array"))? {
            let row = match row.as_array() {
                Some(row) => row,
                None => continue, // Skip row
            };
            let mut jsonl_row = vec![];
            for cm in &mapping.data {
                if let Some((source_label,element_name)) = cm.mapping.get(0) {
                    if let Some(col_num) = label2col_num.get(source_label) {
                        if let Some(value) = row.get(*col_num) {
                            let dc = DataCell::from_value(value,&cm.header, &element_name);
                            jsonl_row.push(dc);
                            continue;
                        }
                    }
                }
                jsonl_row.push(None);
            }
            self.add_output_row(&json!{jsonl_row})?; // Output data row
        }
        Ok(self.file.uuid().as_ref().unwrap().to_string())
    }
}






#[derive(Debug, Default)]
pub struct PetScanAdapter {
    file: DataFile,
}

#[async_trait]
impl Adapter for PetScanAdapter {
    fn writer(&mut self) -> Result<&mut BufWriter<File>> {
        if !self.file.is_output_open() {
            self.file.open_output_file()?;
        }
        self.file.writer()
    }

    async fn source2file(&mut self, source: &SourceId, mapping: &HeaderMapping) -> Result<String> {
        let url = match source {
            SourceId::PetScan(id) => format!("https://petscan.wmflabs.org/?psid={id}&format=json&output_compatability=quick_intersection"),
            _ => return Err(anyhow!("Unsuitable source type for PetScan: {source:?}")),
        };
        let j: Value = reqwest::get(url).await?.json().await?;
        
        self.add_output_row(&json!{mapping.as_data_header()})?; // Output new header
        for row in j["pages"].as_array().ok_or(anyhow!("JSON has no rows array"))? {
            let row = match row.as_object() {
                Some(row) => row,
                None => continue, // Skip row
            };
            let mut jsonl_row = vec![];
            for cm in &mapping.data {
                if let Some((source_label,element_name)) = cm.mapping.get(0) {
                    // TODO sub-elements like metadata.defaultsort/metadata.disambiguation
                    if let Some(value) = row.get(source_label) {
                        let dc = DataCell::from_value(value,&cm.header, &element_name);
                        jsonl_row.push(dc);
                        continue;
                    }
                }
                jsonl_row.push(None);
            }
            self.add_output_row(&json!{jsonl_row})?; // Output data row
        }
        Ok(self.file.uuid().as_ref().unwrap().to_string())
    }
}