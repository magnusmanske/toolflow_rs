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

    pub fn as_key(&self) -> String {
        match self {
            DataCell::PlainText(s) => s.to_string(),
            DataCell::WikiPage(wiki_page) => {
                // TODO ugly fixme
                let blank = String::new();
                let title = wiki_page.title.as_ref().unwrap_or(&blank);
                let namespace = wiki_page.ns_prefix.as_ref().unwrap_or(&blank);
                let fallback = format!("{namespace}:{title}");
                let fullname = wiki_page.prefixed_title.as_ref().unwrap_or(&fallback);
                let wiki = wiki_page.wiki.as_ref().unwrap_or(&blank);
                format!("{wiki}::{fullname}")
            },
            DataCell::Int(i) => format!("{i}"),
            DataCell::Float(f) => format!("{f}"),
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

impl DataHeader {
    pub fn get_col_num(&self, key: &str) -> Option<usize> {
        self.columns.iter().enumerate().filter(|(_col_num,ch)|ch.name==key).map(|(col_num,_)|col_num).next()
    }

    pub fn add_header(&mut self, header: DataHeader) {
        // TODO duplicate column name warning/error
        let mut header = header;
        self.columns.append(&mut header.columns);
    }
}