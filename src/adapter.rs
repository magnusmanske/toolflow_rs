use std::collections::HashMap;
use tempfile::*;
use std::{fs::File, io::{Write, Seek}};
use async_trait::async_trait;
use anyhow::{anyhow, Result};
use serde_json::{Value, json};
use url::Url;

use crate::app::App;
use crate::data_cell::DataCell;
use crate::data_file::{DataFile, DataFileDetails};
use crate::mapping::{HeaderMapping, SourceId};
use crate::wiki_page::WikiPage;

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

impl SparqlAdapter {
    /// Queries SPARQL and returns a filename with the result as CSV.
    pub async fn load_sparql_csv(&self, sparql: &str) -> Result<csv::Reader<File>> {
        let url = format!("https://query.wikidata.org/sparql?query={}",sparql);
        let mut f = tempfile()?;
        let mut res = App::reqwest_client()?
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
    }
}

#[async_trait]
impl Adapter for SparqlAdapter {
    async fn source2file(&mut self, source: &SourceId, mapping: &HeaderMapping) -> Result<DataFileDetails> {
        let sparql = match source {
            SourceId::Sparql(sparql) => sparql,
            _ => return Err(anyhow!("Unsuitable source type for SPARQL: {source:?}")),
        };
        let mut reader = self.load_sparql_csv(&sparql).await?;
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


#[derive(Debug, Default)]
struct WdFistParams {
    sparql: Option<String>,
    language: Option<String>,
    project: Option<String>,
    category: Option<String>,
    depth: Option<i64>,
    pagepile: Option<u64>,
    psid: Option<u64>,
    no_images_only: bool,
    wdf_langlinks: bool,
    wdf_only_files_not_on_wd: bool,
    wdf_max_five_results: bool,
}

impl WdFistParams {
    fn from_url(url: &Url) -> Result<Self> {
        if url.host_str()!=Some("fist.toolforge.org") {
            return Err(anyhow!("Not a WD-FIST URL: {url}"));
        }
        let mut ret = Self::default();
        ret.wdf_langlinks = true; // TODO from params, somehow
        ret.wdf_only_files_not_on_wd = true; // TODO from params, somehow
        ret.wdf_max_five_results = true; // TODO from params, somehow
        url.query_pairs().for_each(|(k,v)|{
            match k.as_ref() {
                "sparql" => ret.sparql = Some(v.to_string()),
                "language" => ret.language = Some(v.to_string()),
                "project" => ret.project = Some(v.to_string()),
                "category" => ret.category = Some(v.to_string()),
                "depth" => ret.depth = v.parse::<i64>().ok(),
                "pagepile" => ret.pagepile = v.parse::<u64>().ok(),
                "psid" => ret.psid = v.parse::<u64>().ok(),
                "no_images_only" => ret.no_images_only = v.parse::<u8>().unwrap_or(0)==1,
                _ => {} // Ignore
            }
        });
        Ok(ret)
    }

    fn to_petscan_url(&self) -> String {
        let mut params : Vec<(String,String)> = vec![];
        params.push(("wdf_main".to_string(),"1".to_string()));
        params.push(("doit".to_string(),"1".to_string()));
        params.push(("format".to_string(),"json".to_string()));
        if let Some(value) = &self.sparql {
            params.push(("sparql".to_string(),value.to_owned()));
        }
        if let Some(value) = &self.language {
            params.push(("language".to_string(),value.to_owned()));
        }
        if let Some(value) = &self.project {
            params.push(("project".to_string(),value.to_owned()));
        }
        if let Some(value) = &self.category {
            params.push(("categories".to_string(),value.to_owned()));
        }
        if let Some(value) = &self.depth {
            params.push(("depth".to_string(),format!("{value}")));
        }
        if let Some(value) = &self.pagepile {
            params.push(("pagepile".to_string(),format!("{value}")));
        }
        if let Some(value) = &self.psid {
            params.push(("psid".to_string(),format!("{value}")));
        }

        if self.no_images_only {
            params.push(("wdf_only_items_without_p18".to_string(),"1".to_string()));
        }
        if self.wdf_langlinks {
            params.push(("wdf_langlinks".to_string(),"1".to_string()));
        }
        if self.wdf_only_files_not_on_wd {
            params.push(("wdf_only_files_not_on_wd".to_string(),"1".to_string()));
        }
        if self.wdf_max_five_results {
            params.push(("wdf_max_five_results".to_string(),"1".to_string()));
        }
        let url = Url::parse_with_params("https://petscan.wmflabs.org",&params).expect("Hardcoded PetScan URL failed");
        url.to_string()
    }
}

#[derive(Debug, Default)]
pub struct WdFistAdapter {
}

#[async_trait]
impl Adapter for WdFistAdapter {
    async fn source2file(&mut self, source: &SourceId, mapping: &HeaderMapping) -> Result<DataFileDetails> {
        let url = match source {
            SourceId::WdFist(url) => Url::parse(url)?,
            _ => return Err(anyhow!("Unsuitable source type for WdFist: {source:?}")),
        };
        let wdfist = WdFistParams::from_url(&url)?;
        let petscan_url = wdfist.to_petscan_url();

        let j: Value = App::reqwest_client()?.get(petscan_url).send().await?.json().await?;        
        
        let mut file = DataFile::new_output_file()?;
        file.write_json_row(&json!{mapping.as_data_header()})?; // Output new header

        for (qid,images) in j["data"].as_object().ok_or(anyhow!("JSON is not an object"))? {
            let images = match images.as_object() {
                Some(images) => images,
                None => continue, // Ignore this
            };
            for (image_name,count) in images.iter() {
                if let Some(count) = count.as_i64() {
                    let mut jsonl_row = vec![];

                    let mut wp = WikiPage::new_wikidata_item();
                    wp.prefixed_title = Some(qid.to_owned());
                    jsonl_row.push(DataCell::WikiPage(wp));

                    let wp = WikiPage{
                            title:Some(image_name.to_owned()),
                            prefixed_title:Some(format!("File:{image_name}")),
                            ns_id:Some(6),
                            page_id:None,
                            ns_prefix:Some("File".to_string()),
                            wiki:Some("commonswiki".to_string())
                        };
                    jsonl_row.push(DataCell::WikiPage(wp));

                    jsonl_row.push(DataCell::Int(count));

                    file.write_json_row(&json!{jsonl_row})?; // Output data row
                }
            }
        }

        Ok(file.details())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::APP;

    #[tokio::test]
    async fn test_adapter_pagepile() {
        let hm = "{\"data\":[{\"header\":{\"kind\":{\"WikiPage\":{\"ns_id\":0,\"ns_prefix\":null,\"page_id\":null,\"prefixed_title\":null,\"title\":null,\"wiki\":\"wikidatawiki\"}},\"name\":\"wikidat_item\"},\"mapping\":[[\"page\",\"prefixed_title\"]]}]}";
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

    #[tokio::test]
    async fn test_adapter_wdfist() {
        let j = json!({"data": [{"header": {"kind": {"WikiPage": {"ns_id": 0,"ns_prefix": null,"page_id": null,"prefixed_title": null,"title": null,"wiki": "wikidatawiki"}},"name": "wikidata_item"},"mapping": []},{"header": {"kind": {"WikiPage": {"ns_id": 6,"ns_prefix": "File","page_id": null,"prefixed_title": null,"title": null,"wiki": "commonswiki"}},"name": "commons_image"},"mapping": []},{"header": {"kind": {"Int": null},"name": "number_of_uses"},"mapping": []}]});
        let header_mapping: HeaderMapping = serde_json::from_str(&j.to_string()).unwrap();
        let id = "https://fist.toolforge.org/wdfist/index.html?depth=3&language=en&project=wikipedia&sparql=SELECT%20?item%20WHERE%20{%20?item%20wdt:P31%20wd:Q5%20;%20wdt:P21%20wd:Q6581072%20;%20wdt:P106/wdt:P279*%20wd:Q901%20}%20LIMIT%2010&remove_used=1&remove_multiple=1&prefilled=1".to_string();
        let df = WdFistAdapter::default().source2file(&SourceId::WdFist(id), &header_mapping).await.unwrap();
        assert!(df.rows>1);
        APP.remove_uuid_file(&df.uuid).unwrap(); // Cleanup
    }
}