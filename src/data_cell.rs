use std::cmp::Ordering;

use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;


use crate::{wiki_page::WikiPage, data_header::{ColumnHeader, ColumnHeaderType}};

lazy_static! {
    static ref RE_WIKIDATA_ITEM: Regex = Regex::new(r"^https?://www.wikidata.org/entity/(Q\d+)$").expect("RegEx fail");
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataCell {
    PlainText(String),
    WikiPage(WikiPage),
    Int(i64),
    Float(f64),
    Blank,
}

impl PartialEq for DataCell {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::PlainText(l0), Self::PlainText(r0)) => l0 == r0,
            (Self::WikiPage(l0), Self::WikiPage(r0)) => l0 == r0,
            (Self::Int(l0), Self::Int(r0)) => l0 == r0,
            (Self::Float(l0), Self::Float(r0)) => l0 == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl PartialOrd for DataCell {
    fn partial_cmp(&self, other: &DataCell) -> Option<Ordering> {
        // println!("{self:?} <=> {other:?}");
        match (self,other) {
            (DataCell::Blank, DataCell::Blank) => Some(Ordering::Equal),
            (DataCell::Blank, _) => Some(Ordering::Less),
            (_, DataCell::Blank) => Some(Ordering::Greater),
            (DataCell::PlainText(t1), DataCell::PlainText(t2)) => t1.partial_cmp(t2),
            // (DataCell::PlainText(_), DataCell::WikiPage(_)) => todo!(),
            // (DataCell::PlainText(_), DataCell::Int(_)) => todo!(),
            // (DataCell::PlainText(_), DataCell::Float(_)) => todo!(),
            // (DataCell::WikiPage(wp), DataCell::PlainText(t)) => todo!(),
            // (DataCell::WikiPage(_), DataCell::WikiPage(_)) => todo!(),
            // (DataCell::WikiPage(_), DataCell::Int(_)) => todo!(),
            // (DataCell::WikiPage(_), DataCell::Float(_)) => todo!(),
            // (DataCell::Int(_), DataCell::PlainText(_)) => todo!(),
            // (DataCell::Int(_), DataCell::WikiPage(_)) => todo!(),
            (DataCell::Int(i1), DataCell::Int(i2)) => i1.partial_cmp(i2),
            (DataCell::Int(i), DataCell::Float(f)) => (*i as f64).partial_cmp(f),
            // (DataCell::Float(_), DataCell::PlainText(_)) => todo!(),
            // (DataCell::Float(_), DataCell::WikiPage(_)) => todo!(),
            (DataCell::Float(f), DataCell::Int(i)) => f.partial_cmp(&(*i as f64)),
            (DataCell::Float(f1), DataCell::Float(f2)) => f1.partial_cmp(f2),
            _ => None,
        }
    }
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

    pub async fn from_value(value: &Value, col_header: &ColumnHeader, element_name: &str) -> Option<Self> {
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
                wiki_page.fill_missing().await;
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
                let ns_prefix = wiki_page.ns_prefix.as_ref().unwrap_or(&blank);
                let fallback = format!("{ns_prefix}:{title}");
                let fullname = wiki_page.prefixed_title.as_ref().unwrap_or(&fallback);
                let wiki = wiki_page.wiki.as_ref().unwrap_or(&blank);
                format!("{wiki}::{fullname}")
            },
            DataCell::Int(i) => format!("{i}"),
            DataCell::Float(f) => format!("{f}"),
            DataCell::Blank => String::new(),
        }
    }

    pub fn to_sub_key(&self, subkey: &Option<String>) -> Self {
        let wp = match self {
            DataCell::WikiPage(wp) => wp,
            _ => return Self::Blank,
        };
        match subkey {
            Some(subkey) => {
                match subkey.as_str() {
                    "title" => wp.title.as_ref().map(|x|DataCell::PlainText(x.to_owned())),
                    "prefixed_title" => wp.prefixed_title.as_ref().map(|x|DataCell::PlainText(x.to_owned())),
                    "ns_prefix" => wp.ns_prefix.as_ref().map(|x|DataCell::PlainText(x.to_owned())),
                    "wiki" => wp.wiki.as_ref().map(|x|DataCell::PlainText(x.to_owned())),
                    "ns_id" => wp.ns_id.map(|x|DataCell::Int(x)),
                    "page_id" => wp.page_id.map(|x|DataCell::Int(x)),
                    _ => None,
                }.unwrap_or_else(||Self::Blank)
            }
            None => Self::Blank,
        }
    }

}