use std::collections::HashMap;
use async_trait::async_trait;
use anyhow::{anyhow, Result};
use serde_json::{Value, json};

use crate::app::App;
use crate::data_cell::DataCell;
use crate::data_file::{DataFile, DataFileDetails};
use crate::mapping::{HeaderMapping, SourceId};
use crate::APP;

/*
To add a new adapter struct:
- add IDs to SourceId in mapping.rs
- add IDs to get_external_header in api.php
- add HTML to node_editor.html template
- add JS initial values to workflow.html new_node()
*/

/*
Candidate tools: (see also https://tool-watch.toolforge.org/ )
- https://xtools.wmcloud.org/pages (parse wikitext output)
- https://ws-search.toolforge.org/ (needs HTML scraping?)
- https://wp-trending.toolforge.org/
- https://wikinearby.toolforge.org/ (via its API)
- https://wikidata-todo.toolforge.org/user_edits.php
- https://wikidata-todo.toolforge.org/wd_edit_stats.php
- https://wikidata-todo.toolforge.org/wdq_image_feed.php
- https://wikidata-todo.toolforge.org/sparql_rc.php
- https://fist.toolforge.org/wd4wp/#/
- https://wikidata-todo.toolforge.org/duplicity/#/
- https://whattodo.toolforge.org
- https://checkwiki.toolforge.org/checkwiki.cgi
- https://cil2.toolforge.org/
- https://grep.toolforge.org/
- https://nppbrowser.toolforge.org/
- https://searchsbl.toolforge.org/
- https://item-quality-evaluator.toolforge.org (to add scores)
*/


#[async_trait]
pub trait Adapter {
    async fn source2file(&mut self, source: &SourceId, mapping: &HeaderMapping) -> Result<DataFileDetails>;
}


#[derive(Debug, Default)]
pub struct SparqlAdapter {
}

#[async_trait]
impl Adapter for SparqlAdapter {
    async fn source2file(&mut self, source: &SourceId, mapping: &HeaderMapping) -> Result<DataFileDetails> {
        let sparql = match source {
            SourceId::Sparql(sparql) => sparql,
            _ => return Err(anyhow!("Unsuitable source type for SPARQL: {source:?}")),
        };
        let mut reader = APP.load_sparql_csv(&sparql).await?;
        let labels: Vec<String> = reader.headers()?.iter().map(|s|s.to_string()).collect();
        let label2col_num: HashMap<String,usize> = labels.into_iter().enumerate().map(|(colnum,header)|(header,colnum)).collect();

        let mut file = DataFile::new_output_file()?;
        file.write_json_row(&json!{mapping.as_data_header()})?; // Output new header
        
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
                            let dc = DataCell::from_value(&j,&cm.header, &element_name).await;
                            jsonl_row.push(dc);
                            continue;
                        }
                    }
                }
                jsonl_row.push(None);
            }
            file.write_json_row(&json!{jsonl_row})?; // Output data row
        }
        Ok(file.details())
    }
}


// Latest result for a given query ID
#[derive(Debug, Default)]
pub struct QuarryQueryAdapter {
}

#[async_trait]
impl Adapter for QuarryQueryAdapter {
    async fn source2file(&mut self, source: &SourceId, mapping: &HeaderMapping) -> Result<DataFileDetails> {
        let url = match source {
            SourceId::QuarryQueryLatest(id) => format!("https://quarry.wmcloud.org/query/{id}/result/latest/0/json"),
            _ => return Err(anyhow!("Unsuitable source type for Quarry query: {source:?}")),
        };
        let j: Value = App::reqwest_client()?.get(url).send().await?.json().await?;        
        let labels: Vec<String> = j["headers"].as_array().ok_or(anyhow!("JSON has no header array"))?.iter().map(|s|s.as_str().unwrap_or("").to_string()).collect();
        let label2col_num: HashMap<String,usize> = labels.into_iter().enumerate().map(|(colnum,header)|(header,colnum)).collect();
        
        let mut file = DataFile::new_output_file()?;
        file.write_json_row(&json!{mapping.as_data_header()})?; // Output new header
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
                            let dc = DataCell::from_value(value,&cm.header, &element_name).await;
                            jsonl_row.push(dc);
                            continue;
                        }
                    }
                }
                jsonl_row.push(None);
            }
            file.write_json_row(&json!{jsonl_row})?; // Output data row
        }
        Ok(file.details())
    }
}



#[derive(Debug, Default)]
pub struct PetScanAdapter {
}

#[async_trait]
impl Adapter for PetScanAdapter {
    async fn source2file(&mut self, source: &SourceId, mapping: &HeaderMapping) -> Result<DataFileDetails> {
        let url = match source {
            SourceId::PetScan(id) => format!("https://petscan.wmflabs.org/?psid={id}&format=json&output_compatability=quick-intersection"),
            _ => return Err(anyhow!("Unsuitable source type for PetScan: {source:?}")),
        };
        let j: Value = App::reqwest_client()?.get(url).send().await?.json().await?;
        
        let mut file = DataFile::new_output_file()?;
        file.write_json_row(&json!{mapping.as_data_header()})?; // Output new header
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
                        let dc = DataCell::from_value(value,&cm.header, &element_name).await;
                        jsonl_row.push(dc);
                        continue;
                    }
                }
                jsonl_row.push(None);
            }
            file.write_json_row(&json!{jsonl_row})?; // Output data row
        }
        Ok(file.details())
    }
}


#[derive(Debug, Default)]
pub struct PagePileAdapter {
}

#[async_trait]
impl Adapter for PagePileAdapter {
    async fn source2file(&mut self, source: &SourceId, mapping: &HeaderMapping) -> Result<DataFileDetails> {
        let url = match source {
            SourceId::PagePile(id) => format!("https://pagepile.toolforge.org/api.php?id={id}&action=get_data&doit&format=json"),
            _ => return Err(anyhow!("Unsuitable source type for PagePile: {source:?}")),
        };
        let j: Value = App::reqwest_client()?.get(url).send().await?.json().await?;        
        let mut file = DataFile::new_output_file()?;
        file.write_json_row(&json!{mapping.as_data_header()})?; // Output new header

        for page in j["pages"].as_array().ok_or(anyhow!("JSON has no rows array"))? {
            let prefixed_title = match page.as_str() {
                Some(prefixed_title) => prefixed_title,
                None => continue, // Skip row
            };

            let mut jsonl_row = vec![];
            for cm in &mapping.data {
                if let Some((_source_label,element_name)) = cm.mapping.get(0) {
                    let value = json!(prefixed_title);
                    let dc = DataCell::from_value(&value,&cm.header, &element_name).await;
                    jsonl_row.push(dc);
                    continue;
                }
                jsonl_row.push(None);
            }
            file.write_json_row(&json!{jsonl_row})?; // Output data row
        }

        Ok(file.details())
    }
}



#[derive(Debug, Default)]
pub struct AListBuildingToolAdapter {
}

#[async_trait]
impl Adapter for AListBuildingToolAdapter {
    async fn source2file(&mut self, source: &SourceId, mapping: &HeaderMapping) -> Result<DataFileDetails> {
        let url = match source {
            SourceId::AListBuildingTool((wiki,q)) => format!("https://a-list-bulding-tool.toolforge.org/API/?wiki_db={wiki}&QID={q}"),
            _ => return Err(anyhow!("Unsuitable source type for AListBuildingTool: {source:?}")),
        };
        let j: Value = App::reqwest_client()?.get(url).send().await?.json().await?;        
        
        let mut file = DataFile::new_output_file()?;
        file.write_json_row(&json!{mapping.as_data_header()})?; // Output new header

        for entry in j.as_array().ok_or(anyhow!("JSON is not an array"))? {
            let title = match entry.get("title") {
                Some(title) => match title.as_str() {
                    Some(title) => title,
                    None => continue, // Skip row
                }
                None => continue, // Skip row
            };
            let qid = match entry.get("qid") {
                Some(qid) => match qid.as_str() {
                    Some(qid) => qid,
                    None => continue, // Skip row
                }
                None => continue, // Skip row
            };
    
            let mut jsonl_row = vec![];
            for cm in &mapping.data {
                for (source_label,element_name) in &cm.mapping {
                    let text = match source_label.as_str() {
                        "title" => title,
                        "qid" => qid,
                        _ => continue,
                    };
                    let j = json!(text);
                    let dc = DataCell::from_value(&j,&cm.header, &element_name).await;
                    jsonl_row.push(dc);
                }
            }
            file.write_json_row(&json!{jsonl_row})?; // Output data row
        }

        Ok(file.details())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_adapter_pagepile() {
        let hm = "{\"data\":[{\"header\":{\"kind\":{\"WikiPage\":{\"ns_id\":null,\"ns_prefix\":null,\"page_id\":null,\"prefixed_title\":null,\"title\":null,\"wiki\":\"dewiki\"}},\"name\":\"wiki_page\"},\"mapping\":[[\"page\",\"prefixed_title\"]]}]}";
        let header_mapping: HeaderMapping = serde_json::from_str(hm).unwrap();
        let id = 51805;
        let df = PagePileAdapter::default().source2file(&&SourceId::PagePile(id), &header_mapping).await.unwrap();
        assert_eq!(df.rows,1748);
        APP.remove_uuid_file(&df.uuid).unwrap(); // Cleanup
    }


    #[tokio::test]
    async fn test_adapter_petscan() {
        let hm = "{\"data\":[{\"header\":{\"kind\":{\"WikiPage\":{\"ns_id\":null,\"ns_prefix\":null,\"page_id\":null,\"prefixed_title\":null,\"title\":null,\"wiki\":\"enwiki\"}},\"name\":\"wiki_page\"},\"mapping\":[[\"page_title\",\"title\"],[\"page_namespace\",\"ns_id\"]]}]}";
        let header_mapping: HeaderMapping = serde_json::from_str(hm).unwrap();
        let id = 25951472;
        let df = PetScanAdapter::default().source2file(&&&SourceId::PetScan(id), &header_mapping).await.unwrap();
        assert_eq!(df.rows,2);
        APP.remove_uuid_file(&df.uuid).unwrap(); // Cleanup
    }

    #[tokio::test]
    async fn test_adapter_alistbuildingtool() {
        let hm = "{\"data\":[{\"header\":{\"kind\":{\"WikiPage\":{\"ns_id\":null,\"ns_prefix\":null,\"page_id\":null,\"prefixed_title\":null,\"title\":null,\"wiki\":\"enwiki\"}},\"name\":\"wiki_page\"},\"mapping\":[[\"title\",\"prefixed_title\"]]}]}";
        let header_mapping: HeaderMapping = serde_json::from_str(hm).unwrap();
        let id = ("enwiki".to_string(),"Q82069695".to_string());
        let df = AListBuildingToolAdapter::default().source2file(&&&SourceId::AListBuildingTool(id), &header_mapping).await.unwrap();
        assert!(df.rows>1);
        APP.remove_uuid_file(&df.uuid).unwrap(); // Cleanup
    }
}