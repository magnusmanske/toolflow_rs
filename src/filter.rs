use anyhow::{Result,anyhow};
use regex::RegexBuilder;
use serde::{Serialize, Deserialize};
use serde_json::json;

use crate::data_cell::DataCell;
use crate::data_file::{DataFileDetails, DataFile};

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
                Err(_) => return Err(anyhow!("Invalid regular expression: {}",&self.value)),
            }
            _ => RegexBuilder::new(".").build().unwrap() // Dummy, never used
        };
            
        let v_plain_text = DataCell::PlainText(self.value.to_owned());
        let v_i64 = DataCell::Int(self.value.parse::<i64>().unwrap_or(0));
        let v_f64 = DataCell::Float(self.value.parse::<f64>().unwrap_or(0.0));

        let mut df_in = DataFile::default();
        let mut df_out = DataFile::new_output_file()?;
        df_in.open_input_file(uuid)?;
        df_in.load_header()?;
        df_out.write_json_row(&json!{df_in.header()})?; // Output new header
        let col_num = df_in.header().columns.iter()
            .enumerate()
            .find(|(_col_num,h)|h.name==self.key)
            .map(|(col_num,_h)|col_num)
            .ok_or_else(||anyhow!("File {uuid} does not have a header column {}",self.key))?;
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
                DataCell::WikiPage(_) => return Err(anyhow!("cell is DataCell::WikiPage somehow, this should never happen {uuid}")),
                DataCell::Int(_) => &v_i64,
                DataCell::Float(_) => &v_f64,
                _ => &DataCell::Blank,
            };

            // println!("{cell:?} {:?} {vcell:?}",self.operator);

            let does_match = match self.operator {
                FilterOperator::Equal => *vcell==cell,
                FilterOperator::Unequal => *vcell!=cell,
                FilterOperator::LargerThan => *vcell<cell,
                FilterOperator::SmallerThan => *vcell>cell,
                FilterOperator::LargerOrEqualThan => *vcell<=cell,
                FilterOperator::SmallerOrEqualThan => *vcell>=cell,
                FilterOperator::Regexp => v_regexp.is_match(&cell.as_key()),
            };

            if does_match==!self.remove_matching {
                df_out.write_json_row(&json!{row})?; // Output data row
            }
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
            remove_matching: false
        };
        let df = filter.process(uuid).await.unwrap();
        assert!(df.rows==2);
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
            remove_matching: false
        };
        let df_keep = filter.process(uuid).await.unwrap();
        filter.remove_matching = true;
        let df_remove = filter.process(uuid).await.unwrap();

        assert_eq!(df_keep.rows,500);
        assert_eq!(df_remove.rows,1249);

        // Cleanup
        APP.remove_uuid_file(&df_keep.uuid).unwrap();
        APP.remove_uuid_file(&df_remove.uuid).unwrap();
    }

    #[test]
    fn test_filter_operator_deserialization() {
        let operator = json!("Equal").to_string() ;
        let operator: FilterOperator = serde_json::from_str(&operator).unwrap();
        assert_eq!(operator,FilterOperator::Equal);
    }
}