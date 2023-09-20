use std::collections::{HashMap, HashSet};
use anyhow::{Result, anyhow};
use serde_json::json;

use crate::{data_file::{DataFile, DataFileDetails}, data_cell::DataCell};


#[derive(Default, Clone, Debug)]
pub struct Join {
}

impl Join {
    // Returns data files, sorted by file size, smallest first
    fn get_files_with_metadata(&self, uuids: Vec<&str>) -> Result<Vec<DataFile>> {
        if uuids.is_empty() {
            return Err(anyhow!("No UUIDs given to inner_join_on_key"));
        }
        if uuids.len()==1 { // TODO Maybe just duplicate this file? Need to check for key presence first?
            return Err(anyhow!("Only one UUID given to inner_join_on_key"));
        }
        let mut files: Vec<_> = uuids.iter()
            .map(|uuid|(uuid,DataFile::default(),0))
            .collect();
        for (uuid,file,size) in &mut files {
            file.open_input_file(uuid)?;
            *size = file.file_size().ok_or(anyhow!("{} has no file size",file.path().unwrap()))?;
        }
        files.sort_by_key(|k| k.2);
        Ok(files.into_iter().map(|(_uuid,df,_size)|df).collect())
    }

    fn read_row_and_key(&self, file: &mut DataFile, key_col_num: usize) -> Option<(Vec<DataCell>,String)> {
        let row = match file.read_row() {
            Some(row) => row,
            None => return None,
        };
        let row: Vec<DataCell> = serde_json::from_str(&row).unwrap_or(vec![]);
        let new_key = match row.get(key_col_num) {
            Some(new_key) => new_key.as_key(),
            None => String::new(),
        };
        Some((row,new_key))
    }

    pub fn merge_unique(&self, uuids: Vec<&str>, key: &str) -> Result<DataFileDetails> {
        let files = self.get_files_with_metadata(uuids)?;
        let mut output_file = DataFile::default();
        output_file.open_output_file()?;
        let mut new_header = None;
        let mut had_key = HashSet::new();
        let first_uuid = files[0].uuid().to_owned();
        for mut file in files.into_iter() {
            file.load_header()?;
            if new_header.is_none() {
                new_header = Some(file.header().to_owned());
                output_file.write_json_row(&json!(new_header))?;
            } else if new_header!=Some(file.header().to_owned()) {
                return Err(anyhow!("File {first_uuid:?} has a different header than {file:?}"));
            }
            let key_col_num = match &new_header {
                Some(x) => x.get_col_num(key).ok_or(anyhow!("No key '{key}' in file {}",file.path().unwrap()))?,
                None => return Err(anyhow!("merge_unique header not initialized")),
            };
            
            loop {
                let (row,key) = match self.read_row_and_key(&mut file, key_col_num) {
                    Some(x) => x,
                    None => break,
                };
                if row.is_empty() || key.is_empty() || had_key.contains(&key){
                    continue;
                }
                had_key.insert(key);
                output_file.write_json_row(&json!(row))?;
            }
        }
        Ok(output_file.details())
    }

    pub fn inner_join_on_key(&self, uuids: Vec<&str>, key: &str) -> Result<DataFileDetails> {
        let mut data_files = self.get_files_with_metadata(uuids)?;
        let mut main_file = data_files.remove(0);
        main_file.load()?;
        let key2row = main_file.key2row(key)?;
        let mut keys_found: HashMap<String,usize> = HashMap::new();
        let number_of_files = data_files.len();
        for mut file in data_files.into_iter() {
            file.load_header()?;
            let mut new_header = file.header().to_owned();
            let key_col_num = new_header.get_col_num(key).ok_or(anyhow!("No key '{key}' in file {}",file.path().unwrap()))?;
            new_header.columns.remove(key_col_num);
            main_file.add_header(new_header);

            loop {
                let (mut row,new_key) = match self.read_row_and_key(&mut file, key_col_num) {
                    Some(x) => x,
                    None => break,
                };
                if row.is_empty() || new_key.is_empty() {
                    continue;
                }
                let row_id = match key2row.get(&new_key) {
                    Some(id) => *id,
                    None => continue , // Not in the first file
                };
                *keys_found.entry(new_key.to_owned()).or_insert(0) += 1;
                row.remove(key_col_num);
                main_file.rows[row_id].append(&mut row);
            }    
        }
        let keys_in_all_files: Vec<&String> = keys_found.iter()
            .filter(|(_,count)|**count==number_of_files)
            .map(|(key_name,_)|key_name)
            .collect();
        
        let mut output_file = DataFile::default();
        output_file.open_output_file()?;
        output_file.write_json_row(&json!(main_file.header()))?;
        for key in keys_in_all_files {
            let row_id = match key2row.get(key) {
                Some(id) => *id,
                None => continue,
            };
            let row = match main_file.rows.get(row_id) {
                Some(row) => row,
                None => continue,
            };
            output_file.write_json_row(&json!(row))?;
        }
        Ok(output_file.details())
    }
}