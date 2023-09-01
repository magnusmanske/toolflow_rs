use std::collections::HashMap;
use std::{fmt, fs::File};
use std::io::{Write, BufReader, BufWriter, BufRead};
use anyhow::{anyhow, Result};
use serde_json::Value;
use uuid::Uuid;
use crate::APP;
use crate::data_header::{DataHeader, DataCell};

#[derive(Default)]
pub struct DataFile {
    reader: Option<BufReader<File>>,
    writer: Option<BufWriter<File>>,
    uuid: Option<String>,
    header: DataHeader,
    pub rows: Vec<Vec<DataCell>>,
}

impl fmt::Debug for DataFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataFile")
         .field("uuid", &self.uuid)
         .finish()
    }
}

impl DataFile {
    pub fn write_json_row(&mut self, v: &Value) -> Result<()> {
        let fh = self.writer()?;
        fh.write(v.to_string().as_bytes())?;
        fh.write(b"\n")?;
        Ok(())
    }

    pub fn open_output_file(&mut self) -> Result<()> {
        let uuid = Uuid::new_v4();
        let name = format!("{uuid}");
        self.open_named_output_file(&name)
    }

    pub fn open_named_output_file(&mut self, uuid: &str) -> Result<()> {
        self.uuid = Some(uuid.to_string());
        let path = self.path().expect("base name was just set, this should be impossible");
        let file_handle = File::create(path)?;
        let writer = BufWriter::new(file_handle);
        self.writer = Some(writer);
        Ok(())
    }

    pub fn open_input_file(&mut self, uuid: &str) -> Result<()> {
        self.uuid = Some(uuid.to_string());
        let path = self.path().expect("base name was just set, this should be impossible");
        let file_handle = File::open(path)?;
        let reader = BufReader::new(file_handle);
        self.reader = Some(reader);
        Ok(())
    }

    pub fn file_size(&self) -> Option<u64> {
        let reader = self.reader.as_ref()?;
        let file = reader.get_ref();
        Some(file.metadata().ok()?.len())

        // let path = self.path();
        // let path = path.as_ref()?;
        // let path = Path::new(path);
        // let len = path.metadata().ok()?.len();
        // Some(len)
    }

    pub fn path(&self) -> Option<String> {
        let name = self.uuid.as_ref()?;
        Some(format!("{}/{name}.jsonl",APP.data_path()))
    }

    pub fn name(&self) -> &Option<String> {
        &self.uuid
    }

    pub fn is_output_open(&self) -> bool {
        self.writer.is_some()
    }

    pub fn is_input_open(&self) -> bool {
        self.reader.is_some()
    }

    pub fn writer(&mut self) -> Result<&mut BufWriter<File>> {
        match self.writer.as_mut() {
            Some(writer) => Ok(writer),
            None => Err(anyhow!("No writer open")),
        }
    }

    pub fn read_row(&mut self) -> Option<String> {
        let mut line = String::new();
        if self.reader.as_mut()?.read_line(&mut line).ok()? == 0 {
            None // No empty lines expected, mut be the end
        } else {
            Some(line)
        }
    }

    pub fn load_header(&mut self) -> Result<()> {
        let row = self.read_row().ok_or(anyhow!("No header row in JSONL file"))?;
        self.header = serde_json::from_str(&row)?;
        Ok(())
    }

    pub fn load(&mut self) -> Result<()> {
        if self.header.columns.is_empty() {
            self.load_header()?;
        }
        loop {
            let row = match self.read_row() {
                Some(row) => row,
                None => break,
            };
            let row: Vec<DataCell> = serde_json::from_str(&row)?;
            self.rows.push(row);
        }
        Ok(())
    }

    pub fn header(&self) -> &DataHeader {
        &self.header
    }

    pub fn key2row(&self, key: &str) -> Result<HashMap<String,usize>> {
        let mut ret = HashMap::new();
        let key_col_num = self.header.get_col_num(key).ok_or(anyhow!("No column named '{key}'"))?;
        for (row_num,row) in self.rows.iter().enumerate() {
            let cell = match row.get(key_col_num) {
                Some(cell) => cell,
                None => return Err(anyhow!("None value found for key '{key}' in data row {row_num}")),
            };
            let cell_key = cell.as_key();
            if ret.contains_key(&cell_key) {
                return Err(anyhow!("Duplicate key '{cell_key}' for '{key}' in data row {row_num}"));
            }
            ret.insert(cell_key, row_num);
        }
        Ok(ret)
    }

    pub fn add_header(&mut self, header: DataHeader) {
        self.header.add_header(header);
    }
}