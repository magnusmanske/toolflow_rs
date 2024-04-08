use anyhow::{anyhow, Result};
use regex::RegexBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::app::App;
use crate::data_cell::DataCell;
use crate::data_file::{DataFile, DataFileDetails};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FilterOperator {
    Equal,
    Unequal,
    LargerThan,
    SmallerThan,
    LargerOrEqualThan,
    SmallerOrEqualThan,
    Regexp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    pub key: String,
    pub subkey: Option<String>,
    pub operator: FilterOperator,
    pub value: String,

    #[serde(default)]
    pub remove_matching: bool,
}

impl Filter {
    pub async fn process(&self, uuid: &str) -> Result<DataFileDetails> {
        let v_regexp = match self.operator {
            FilterOperator::Regexp => match RegexBuilder::new(&self.value).build() {
                Ok(r) => r,
                Err(_) => return Err(anyhow!("Invalid regular expression: {}", &self.value)),
            },
            _ => RegexBuilder::new(".").build()?,
        };

        let v_plain_text = DataCell::PlainText(self.value.to_owned());
        let v_i64 = DataCell::Int(self.value.parse::<i64>().unwrap_or(0));
        let v_f64 = DataCell::Float(self.value.parse::<f64>().unwrap_or(0.0));

        let mut df_in = DataFile::default();
        let mut df_out = DataFile::new_output_file()?;
        df_in.open_input_file(uuid)?;
        df_in.load_header()?;
        df_out.write_json_row(&json! {df_in.header()})?; // Output new header
        let col_num = df_in
            .header()
            .columns
            .iter()
            .enumerate()
            .find(|(_col_num, h)| h.name == self.key)
            .map(|(col_num, _h)| col_num)
            .ok_or_else(|| anyhow!("File {uuid} does not have a header column {}", self.key))?;
        loop {
            let row = match df_in.read_row() {
                Some(row) => row,
                None => break, // End of file
            };
            let row: Vec<DataCell> = serde_json::from_str(&row)?;
            let cell = row.get(col_num);
            let cell = match cell {
                Some(cell) => match cell {
                    DataCell::WikiPage(_wp) => cell.to_sub_key(&self.subkey),
                    other => other.to_owned(),
                },
                None => DataCell::Blank,
            };

            // println!("{cell:?}");

            let vcell = match cell {
                DataCell::PlainText(_) => &v_plain_text,
                DataCell::WikiPage(_) => {
                    return Err(anyhow!(
                        "cell is DataCell::WikiPage somehow, this should never happen {uuid}"
                    ))
                }
                DataCell::Int(_) => &v_i64,
                DataCell::Float(_) => &v_f64,
                _ => &DataCell::Blank,
            };

            // println!("{cell:?} {:?} {vcell:?}",self.operator);

            let does_match = match self.operator {
                FilterOperator::Equal => *vcell == cell,
                FilterOperator::Unequal => *vcell != cell,
                FilterOperator::LargerThan => *vcell < cell,
                FilterOperator::SmallerThan => *vcell > cell,
                FilterOperator::LargerOrEqualThan => *vcell <= cell,
                FilterOperator::SmallerOrEqualThan => *vcell >= cell,
                FilterOperator::Regexp => v_regexp.is_match(&cell.as_key()),
            };

            if does_match == !self.remove_matching {
                df_out.write_json_row(&json! {row})?; // Output data row
            }
        }
        Ok(df_out.details())
    }
}

// ____________________________________________________________________________________

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterPetScan {
    pub key: String,
    pub psid: u64,
}

impl FilterPetScan {
    pub async fn process(&self, uuid: &str) -> Result<DataFileDetails> {
        // Get page list
        let mut pages = vec![];
        let mut df_in = DataFile::default();
        df_in.open_input_file(uuid)?;
        df_in.load_header()?;
        let col_num = df_in
            .header()
            .columns
            .iter()
            .enumerate()
            .find(|(_col_num, h)| h.name == self.key)
            .map(|(col_num, _h)| col_num)
            .ok_or_else(|| anyhow!("File {uuid} does not have a header column {}", self.key))?;
        loop {
            let row = match df_in.read_row() {
                Some(row) => row,
                None => break, // End of file
            };
            let row: Vec<DataCell> = serde_json::from_str(&row)?;
            let cell = row.get(col_num);
            let wiki_page = match cell {
                Some(cell) => match cell {
                    DataCell::WikiPage(wp) => wp,
                    _ => continue,
                },
                None => continue,
            };
            let page = match &wiki_page.prefixed_title {
                Some(page) => page,
                None => continue,
            };
            pages.push(page.to_owned());
        }

        // Get wiki
        let header = match df_in.header().columns.get(col_num) {
            Some(h) => h,
            None => {
                return Err(anyhow!(
                    "File {uuid} does not have a header column {}",
                    self.key
                ))
            }
        };
        let manual_list_wiki = match &header.kind {
            crate::data_header::ColumnHeaderType::WikiPage(wp) => match &wp.wiki {
                Some(wiki) => wiki.to_owned(),
                None => return Err(anyhow!("No wiki set for column {}", self.key)),
            },
            _ => return Err(anyhow!("Not a wiki column for {}", self.key)),
        };

        // Query PetScan
        let url = "https://petscan.wmflabs.org";
        let pages = pages.join("\n");
        let psid = format!("{}", self.psid);
        let params = [
            ("psid", psid.as_str()),
            ("format", "json"),
            ("output_compatability", "quick-intersection"),
            ("sparse", "1"),
            ("manual_list_wiki", &manual_list_wiki),
            ("manual_list", &pages),
        ];
        let j: Value = App::reqwest_client()?
            .post(url)
            .form(&params)
            .send()
            .await?
            .json()
            .await?;
        let pages: Vec<String> = j
            .get("pages")
            .ok_or(anyhow!(
                "PetScan PSID {} fail: no pages key in JSON",
                self.psid
            ))?
            .as_array()
            .ok_or(anyhow!(
                "PetScan PSID {} fail: pages is not an array",
                self.psid
            ))?
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();

        let mut df_out = DataFile::new_output_file()?;
        let mut df_in = DataFile::default();
        df_in.open_input_file(uuid)?;
        df_in.load_header()?;
        df_out.write_json_row(&json! {df_in.header()})?; // Output new header
        loop {
            let row = match df_in.read_row() {
                Some(row) => row,
                None => break, // End of file
            };
            let row: Vec<DataCell> = serde_json::from_str(&row)?;
            let cell = row.get(col_num);
            let wiki_page = match cell {
                Some(cell) => match cell {
                    DataCell::WikiPage(wp) => wp,
                    _ => continue,
                },
                None => continue,
            };
            let page = match &wiki_page.prefixed_title {
                Some(page) => page,
                None => continue,
            };
            if pages.contains(page) {
                df_out.write_json_row(&json! {row})?; // Output data row
            }
        }
        Ok(df_out.details())
    }
}

// ____________________________________________________________________________________

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterSort {
    pub key: String,
    pub reverse: bool,
}

impl FilterSort {
    pub async fn process(&self, uuid: &str) -> Result<DataFileDetails> {
        let mut df_in = DataFile::default();
        df_in.open_input_file(uuid)?;
        df_in.load_header()?;

        let col_num = df_in
            .header()
            .columns
            .iter()
            .enumerate()
            .find(|(_col_num, h)| h.name == self.key)
            .map(|(col_num, _h)| col_num)
            .ok_or_else(|| anyhow!("File {uuid} does not have a header column {}", self.key))?;

        // Read rows
        let mut rows = vec![];
        loop {
            let row = match df_in.read_row() {
                Some(row) => row,
                None => break, // End of file
            };
            let row: Vec<DataCell> = serde_json::from_str(&row)?;
            rows.push(row);
        }

        // Sort rows
        rows.sort_by_cached_key(|row| {
            let cell = match row.get(col_num) {
                Some(cell) => cell,
                None => return String::default(),
            };
            cell.as_key()
        });
        if self.reverse {
            rows.reverse();
        }

        // Write sorted rows
        let mut df_out = DataFile::new_output_file()?;
        df_out.write_json_row(&json! {df_in.header()})?; // Output new header
        for row in rows {
            df_out.write_json_row(&json! {row})?; // Output data row
        }
        Ok(df_out.details())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::APP;

    #[tokio::test]
    async fn test_filter_wikipage_via_prefixed_title() {
        let uuid = "cb1e218e-421f-46b8-a77e-eac6799ce4e4";
        let filter = Filter {
            key: "wiki_page".to_string(),
            subkey: Some("prefixed_title".to_string()),
            operator: FilterOperator::Equal,
            value: "AGEB".to_string(),
            remove_matching: false,
        };
        let df = filter.process(uuid).await.unwrap();
        assert!(df.rows == 2);
        APP.remove_uuid_file(&df.uuid).unwrap(); // Cleanup
    }

    #[tokio::test]
    async fn test_filter_wikipage_via_namespace_id() {
        let uuid = "cb1e218e-421f-46b8-a77e-eac6799ce4e4";
        let mut filter = Filter {
            key: "wiki_page".to_string(),
            subkey: Some("ns_id".to_string()),
            operator: FilterOperator::Unequal,
            value: "0".to_string(),
            remove_matching: false,
        };
        let df_keep = filter.process(uuid).await.unwrap();
        filter.remove_matching = true;
        let df_remove = filter.process(uuid).await.unwrap();

        assert_eq!(df_keep.rows, 500);
        assert_eq!(df_remove.rows, 1249);

        // Cleanup
        APP.remove_uuid_file(&df_keep.uuid).unwrap();
        APP.remove_uuid_file(&df_remove.uuid).unwrap();
    }

    #[test]
    fn test_filter_operator_deserialization() {
        let operator = json!("Equal").to_string();
        let operator: FilterOperator = serde_json::from_str(&operator).unwrap();
        assert_eq!(operator, FilterOperator::Equal);
    }

    #[tokio::test]
    async fn test_filter_petscan() {
        let uuid = "8c5d1fb3-6ea8-44d1-b938-9d22f569c412";
        let filter = FilterPetScan {
            key: "wikidata_item".to_string(),
            psid: 26256139,
        };
        let df = filter.process(uuid).await.unwrap();
        assert!(df.rows == 34);
        APP.remove_uuid_file(&df.uuid).unwrap(); // Cleanup
    }

    #[tokio::test]
    async fn test_filter_sort() {
        async fn sub_test(reverse: bool, expected_first_item: &str) {
            let uuid = "8c5d1fb3-6ea8-44d1-b938-9d22f569c412";
            let filter = FilterSort {
                key: "wikidata_item".to_string(),
                reverse,
            };
            let df = filter.process(uuid).await.unwrap();
            // println!("Generated test_data/{}.jsonl with {} rows",df.uuid,df.rows);
            assert!(df.rows == 50);
            if true {
                let mut df_in = DataFile::default();
                df_in
                    .open_input_file(&df.uuid)
                    .expect(&format!("New data file missing: {}", df.uuid));
                let _ = df_in
                    .read_row()
                    .expect(&format!("Header row missing for {}", df.uuid));
                let row = df_in
                    .read_row()
                    .expect(&format!("First data row missing for {}", df.uuid));
                let row: Vec<DataCell> =
                    serde_json::from_str(&row).expect("First data row is not JSON");
                let cell = match &row[0] {
                    DataCell::WikiPage(wp) => wp.to_owned(),
                    _ => panic!("Sort failed"),
                };
                assert_eq!(cell.prefixed_title.unwrap(), expected_first_item);
            }
            APP.remove_uuid_file(&df.uuid).unwrap(); // Cleanup
        }

        sub_test(true, "Q99929855").await;
        sub_test(false, "Q18619644").await;
    }
}
