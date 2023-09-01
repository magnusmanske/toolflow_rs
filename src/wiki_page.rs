use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WikiPage {
    pub title: Option<String>,
    pub prefixed_title: Option<String>,
    pub ns_id: Option<i64>,
    pub page_id: Option<i64>,
    pub ns_prefix: Option<String>,
    pub wiki: Option<String>,
}

impl WikiPage {
    pub fn new_wikidata_item() -> Self {
        Self { title: None, prefixed_title: None, ns_id: Some(0), page_id: None, ns_prefix: None, wiki: Some("wikidatawiki".to_string()) }
    }

    pub fn new_commons_category() -> Self {
        Self { title: None, prefixed_title: None, ns_id: Some(14), page_id: None, ns_prefix: Some("Category".to_string()), wiki: Some("commonswiki".to_string()) }
    }
}
    