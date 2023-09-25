use std::sync::{Mutex, Arc};

use lazy_static::lazy_static;
use anyhow::{Result,anyhow};
use regex::Regex;
use crate::{data_file::DataFile, data_cell::DataCell, data_header::{ColumnHeader, ColumnHeaderType}};

lazy_static!{
    static ref RE_WIKI_TO_PREFIX: Regex = Regex::new(r"^(.+)wik.*$").expect("Regex error");
}


pub trait Renderer {
    fn render_header(&self, df: &mut DataFile) -> Result<String>;
    fn render_footer(&self, df: &mut DataFile) -> Result<String>;
    fn render_row(&self, df: &mut DataFile, row_num: usize, row: Vec<DataCell>) -> Result<String>;
    fn render_cell(&self, col_header: &ColumnHeader, row_num: usize, col_num: usize, cell: DataCell) -> Result<String>;

    fn render_row_separators(&self, df: &mut DataFile, row_num: usize, row: Vec<DataCell>, before: &str, between: &str, after: &str) -> Result<String> {
        let mut ret = before.to_string();
        ret += &row.into_iter()
            .zip(df.header().columns.iter())
            .enumerate()
            .map(|(col_num,(cell,col_header))|self.render_cell(col_header,row_num,col_num,cell))
            .collect::<Result<Vec<String>>>()?
            .join(between);
        ret += after;
        Ok(ret)
    }

    fn render(&self, df: &mut DataFile) -> Result<String> {
        df.load_header()?;
        let mut ret = self.render_header(df)?;
        let mut row_num = 0;
        loop {
            let row = match df.read_row() {
                Some(row) => row,
                None => break,
            };
            let row: Vec<DataCell> = serde_json::from_str(&row)?;
            ret += &self.render_row(df, row_num, row)?;
            row_num += 1;
        }
        ret += &self.render_footer(df)?;
        Ok(ret)
    }

    fn render_from_uuid(&self, uuid: &str) -> Result<String> {
        let mut df = DataFile::default();
        df.open_input_file(uuid)?;
        self.render(&mut df)
    }

}

#[derive(Default, Clone, Debug)]
pub struct RendererWikitext {
    default_wiki: Arc<Mutex<Option<String>>>,
}

impl RendererWikitext {
    fn detect_default_wiki(&self, df: &DataFile) -> Result<()> {
        for column in &df.header().columns {
            if let ColumnHeaderType::WikiPage(wp) = &column.kind {
                match self.default_wiki.lock() {
                    Ok(mut dw) => {
                        if dw.is_none() && wp.wiki.is_some() {
                            *dw = wp.wiki.to_owned();
                        }        
                    }
                    Err(e) => return Err(anyhow!("{e}")),
                }
            }
        }
        Ok(())
    }

    fn pretty_filename(&self, title: &str) -> String {
        let filename_pretty = title.replace('_'," ");

        // Remove file prefix
        let filename_pretty = match filename_pretty.split_once(':') {
            Some(fp) => fp.1.to_string(),
            None => filename_pretty,
        };

        // Remove file ending
        let filename_pretty = match filename_pretty.rsplit_once('.') {
            Some(fp) => fp.0.to_string(),
            None => filename_pretty,
        };

        filename_pretty
    }
}

impl Renderer for RendererWikitext {
    fn render_header(&self, df: &mut DataFile) -> Result<String> {
        self.detect_default_wiki(df)?;

        let mut ret = String::new();
        ret += "{| class=\"wikitable\"\n";
        ret += &df.header().columns.iter()
            .map(|c|c.name.replace('_'," "))
            .map(|s|format!("! {s}\n"))
            .collect::<Vec<String>>()
            .join("");
        Ok(ret)
    }

    fn render_footer(&self, _df: &mut DataFile) -> Result<String> {
        let mut ret = String::new();
        ret += "|}\n";
        // TODO date
        Ok(ret)
    }

    fn render_row(&self, df: &mut DataFile, row_num: usize, row: Vec<DataCell>) -> Result<String> {
        self.render_row_separators(df,row_num,row,"|--\n","","")
    }

    fn render_cell(&self, col_header: &ColumnHeader, row_num: usize, col_num: usize, cell: DataCell) -> Result<String> {
        let default_wiki = self.default_wiki.lock().unwrap();
        Ok("||".to_string() + &match cell {
            DataCell::PlainText(s) => s,
            DataCell::WikiPage(wp) => {
                let mut title = wp.prefixed_title.ok_or_else(||anyhow!("Row {row_num} column {col_num}: WikiPage has no prefixed_title"))?;
                let col_wp = match &col_header.kind  {
                    ColumnHeaderType::WikiPage(col_wp) => col_wp,
                    _ => return Err(anyhow!("Row {row_num} column {col_num}: cell is WikiPage but header is not")),
                };
                let wiki = wp.wiki.to_owned().or(col_wp.wiki.to_owned());
                let wiki = wiki.ok_or_else(||anyhow!("Row {row_num} column {col_num}: No wiki for WikiPage"))?;
                let is_local_wiki = wp.wiki==*default_wiki;
                if !is_local_wiki {
                    if wiki=="commonswiki" && wp.ns_id==Some(6) { // File on Commons
                        let filename_pretty = self.pretty_filename(&title);
                        title = format!("{title}|thumbnail|{filename_pretty}");
                    } else {
                        let wiki_prefix = RE_WIKI_TO_PREFIX.replace(&wiki,"$1");
                        title = format!(":{wiki_prefix}:{title}");
                    }
                } else if wp.ns_id==Some(0) && wiki=="wikidatawiki" { // Wikidata item on Wikidata
                    return Ok(format!("||{{{{Q|{}}}}}\n",&title[1..]));
                } else if wp.ns_id==Some(120) && wiki=="wikidatawiki" { // Wikidata property on Wikidata
                    return Ok(format!("||{{{{P|{}}}}}\n",&title[1..]));
                } else if wp.ns_id==Some(6) { // Local file
                    let filename_pretty = self.pretty_filename(&title);
                    title = format!("{title}|thumbnail|{filename_pretty}");
                } else if wp.ns_id==Some(14) { // Local category
                    title = format!(":{title}");
                }

                let mut link = title.to_owned();
                if wp.ns_id!=Some(6) && title.contains('_') {
                    let pretty_title = title.replace('_', " ");
                    let pretty_title = match title.chars().next() {
                        Some(':') => pretty_title[1..].to_string(),
                        _ => pretty_title,
                    };
                    link = format!("{title}|{pretty_title}");
                }
                format!("[[{link}]]")
            },
            DataCell::Int(i) => format!("{i}"),
            DataCell::Float(f) => format!("{f}"),
            DataCell::Blank => String::new(),
        }+"\n")
    }

}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_renderer_wikitext() {
        let uuid = "cb1e218e-421f-46b8-a77e-eac6799ce4e4";
        let wikitext = RendererWikitext::default().render_from_uuid(uuid).unwrap();
        assert_eq!(wikitext.len(),108767);
    }

}
