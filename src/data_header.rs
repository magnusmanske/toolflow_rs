use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::wiki_page::WikiPage;

lazy_static! {
    static ref RE_WIKIDATA_ITEM: Regex = Regex::new(r"^https?://www.wikidata.org/entity/(Q\d+)$").unwrap();
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnHeaderType {
    PlainText,
    WikiPage(WikiPage),
    Int,
    Float,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataCell {
    PlainText(String),
    WikiPage(WikiPage),
    Int(i64),
    Float(f64),
}

impl DataCell {
    pub fn from_value(value: &Value, col_header: &ColumnHeader) -> Option<Self> {
        match &col_header.kind {
            ColumnHeaderType::PlainText => Some(Self::PlainText(value.as_str()?.to_string())),
            ColumnHeaderType::WikiPage(wiki_page) => {
                let mut wiki_page = wiki_page.clone();
                match value["type"].as_str() {
                    Some("wikidata_item") => {
                        match value["url"].as_str() {
                            Some(url) => {
                                match RE_WIKIDATA_ITEM.captures_iter(url).next() {
                                    Some(cap) => {
                                        wiki_page.title = Some(cap[1].to_string());
                                        wiki_page.prefixed_title = Some(cap[1].to_string());
                                    },
                                    None => return None, // No match
                                }
                            }
                            _ => return None, // Not a str
                        }
                    }
                    _ => {
                        match value.as_str() {
                            Some(title) => {
                                wiki_page.title = Some(title.to_owned());
                            },
                            None => todo!(),
                        }
                    },
                }
                Some(Self::WikiPage(wiki_page))
            },
            ColumnHeaderType::Int => Some(Self::Int(value.as_i64()?)),
            ColumnHeaderType::Float => Some(Self::Float(value.as_f64()?))
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnHeader {
    pub name: String,
    pub kind: ColumnHeaderType,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataHeader {
    pub columns: Vec<ColumnHeader>
}