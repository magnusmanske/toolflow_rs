use serde::{Deserialize, Serialize};
use crate::{data_header::*, wiki_page::WikiPage};

#[derive(Debug, Clone)]
pub enum SourceId {
    QuarryQueryRun(u64),
    QuarryQueryLatest(u64),
    Sparql(String),
    PetScan(u64),
    PagePile(u64),
    AListBuildingTool((String,String)),
    WdFist(String),
    UserEdits(String),
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMapping {
    pub header: ColumnHeader,
    pub mapping: Vec<(String,String)>, // source "column" => new header "property"
}



#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HeaderMapping {
    pub data: Vec<ColumnMapping>,
}

impl HeaderMapping {
    pub fn add_plain_text(&mut self, source_column: &str, header_label: &str) -> &mut Self{
        self.data.push(ColumnMapping{
            header: ColumnHeader{ name: header_label.to_string(), kind: ColumnHeaderType::PlainText },
            mapping: vec![(source_column.to_string(),header_label.to_string())]
        });
        self
    }

    pub fn add_wikidata_item(&mut self, source_column: &str, header_label: &str) -> &mut Self {
        self.data.push(ColumnMapping{
            header: ColumnHeader{ name: header_label.to_string(), kind: ColumnHeaderType::WikiPage(WikiPage::new_wikidata_item()) },
            mapping: vec![(source_column.to_string(),header_label.to_string())]
        });
        self
    }

    pub fn add_wiki_page(&mut self, source_column: &str, header_label: &str, wiki_page: &WikiPage) -> &mut Self {
        self.data.push(ColumnMapping{
            header: ColumnHeader{ name: header_label.to_string(), kind: ColumnHeaderType::WikiPage(wiki_page.to_owned()) },
            mapping: vec![(source_column.to_string(),header_label.to_string())]
        });
        self
    }

    pub fn build(&mut self) -> Self {
        self.to_owned()
    }

    pub fn as_data_header(&self) -> DataHeader {
        DataHeader {
            columns: self.data.iter().map(|cm|cm.header.to_owned()).collect(),
        }
    }
}

