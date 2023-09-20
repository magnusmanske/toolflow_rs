use serde::{Deserialize, Serialize};

use crate::APP;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WikiPage {
    pub title: Option<String>,
    pub prefixed_title: Option<String>,
    pub ns_id: Option<i64>,
    pub page_id: Option<i64>,
    pub ns_prefix: Option<String>,
    pub wiki: Option<String>,
}

impl PartialEq for WikiPage {
    fn eq(&self, other: &Self) -> bool {
        // TODO check, make more elegant?
        self.wiki==other.wiki && self.prefixed_title==other.prefixed_title
    }
}

impl WikiPage {
    pub fn new_wikidata_item() -> Self {
        Self { title: None, prefixed_title: None, ns_id: Some(0), page_id: None, ns_prefix: None, wiki: Some("wikidatawiki".to_string()) }
    }

    pub fn new_commons_category() -> Self {
        Self { title: None, prefixed_title: None, ns_id: Some(14), page_id: None, ns_prefix: Some("Category".to_string()), wiki: Some("commonswiki".to_string()) }
    }

    pub async fn fill_missing(&mut self) {
        if let Some(title) = &mut self.title {
            *title = title.replace(' ',"_");
        }
        if let Some(prefixed_title) = &mut self.prefixed_title {
            *prefixed_title = prefixed_title.replace(' ',"_");
        }

        if let Some(wiki) = &self.wiki {
            if !wiki.is_empty() {
                if self.ns_prefix.is_none() {
                    if let Some(ns_id) = self.ns_id {
                        if let Some(ns) = APP.get_namespace_name(wiki, ns_id).await {
                            self.ns_prefix = Some(ns)
                        }
                    }
                }
            }
        };
        
        if self.prefixed_title.is_none() {
            if let Some(title) = &self.title {
                if let Some(ns_prefix) = &self.ns_prefix {
                    if !title.is_empty() {
                        if ns_prefix.is_empty() {
                            self.prefixed_title = self.title.to_owned();
                        } else {
                            self.prefixed_title = Some(format!("{ns_prefix}:{title}"));
                        }
                    }
                }
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_wikidata_item() {
        let item = WikiPage::new_wikidata_item();
        assert_eq!(item.wiki,Some("wikidatawiki".to_string()));
        assert_eq!(item.ns_id,Some(0));
    }

    #[test]
    fn test_new_commons_category() {
        let item = WikiPage::new_commons_category();
        assert_eq!(item.wiki,Some("commonswiki".to_string()));
        assert_eq!(item.ns_prefix,Some("Category".to_string()));
        assert_eq!(item.ns_id,Some(14));
    }

    #[tokio::test]
    async fn test_fill_missing() {
        let mut wp = WikiPage::default();
        wp.wiki = Some("wikidatawiki".to_string());
        wp.title = Some("Q12345".to_string());
        wp.ns_id = Some(0);
        wp.fill_missing().await;
        assert_eq!(wp.prefixed_title,Some("Q12345".to_string()));

        let mut wp = WikiPage::default();
        wp.wiki = Some("commonswiki".to_string());
        wp.title = Some("Foobar".to_string());
        wp.ns_id = Some(14);
        wp.fill_missing().await;
        assert_eq!(wp.prefixed_title,Some("Category:Foobar".to_string()));

    }
}