use serde::{Deserialize, Serialize};

use crate::wiki_page::WikiPage;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ColumnHeaderType {
    PlainText,
    WikiPage(WikiPage),
    Int,
    Float,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnHeader {
    pub name: String,
    pub kind: ColumnHeaderType,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
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