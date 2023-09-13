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
    fn entity_from_url(url: &str) -> Option<(i64,String)> { // namespace_id, page_title
        match RE_WIKIDATA_ITEM.captures_iter(url).next() {
            Some(cap) => {
                let title = cap[1].to_string();
                let ns_id = match title.chars().next() {
                    Some('Q') => 0,
                    Some('P') => 120,
                    _ => return None,
                };
                Some((ns_id,title))
            },
            None => None, // No match
        }
    }

    pub fn from_value(value: &Value, col_header: &ColumnHeader, element_name: &str) -> Option<Self> {
        match &col_header.kind {
            ColumnHeaderType::PlainText => Some(Self::PlainText(value.as_str()?.to_string())),
            ColumnHeaderType::WikiPage(wiki_page) => {
                let mut wiki_page = wiki_page.clone();
                match value.as_str() {
                    Some(s) => {
                        match element_name {
                            "title" => wiki_page.title = Some(s.to_owned()),
                            "prefixed_title" => wiki_page.prefixed_title = Some(s.to_owned()),
                            "ns_prefix" => wiki_page.ns_prefix = Some(s.to_owned()),
                            "ns_id" => wiki_page.ns_id = s.parse::<i64>().ok(),
                            "page_id" => wiki_page.page_id = s.parse::<i64>().ok(),
                            "wiki" => wiki_page.wiki = Some(s.to_owned()),
                            "entity_url" => {
                                if let Some((ns_id,title)) = Self::entity_from_url(s) {
                                    wiki_page.ns_id = Some(ns_id);
                                    wiki_page.title = Some(title.to_owned());
                                    wiki_page.prefixed_title = Some(title.to_owned());
                                }
                            }
                            _ => return None
                        }

                    },
                    None => todo!(),
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