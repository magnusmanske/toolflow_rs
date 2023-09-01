use std::{fmt, fs::File};
use std::io::{Write, BufReader, Read, BufWriter};
use anyhow::{anyhow, Result};
use bzip2::Compression;
use serde_json::Value;
use bzip2::bufread::BzDecoder;
use bzip2::bufread::BzEncoder;
use uuid::Uuid;
use crate::APP;
use crate::data_header::{DataHeader, DataCell};

const BUFFER_SIZE: usize = 40;

#[derive(Default)]
pub struct DataFile {
    reader: Option<BzDecoder<BufReader<File>>>,
    writer: Option<BzEncoder<BufWriter<File>>>,
    uuid: Option<String>,
    header: DataHeader,
    rows: Vec<Vec<DataCell>>,
    bytes_read: Vec<u8>,
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
        let buf_writer = BufWriter::new(file_handle);
        let writer = BzEncoder::new(buf_writer, Compression::best());
        self.writer = Some(writer);
        Ok(())
    }

    pub fn open_input_file(&mut self, uuid: &str) -> Result<()> {
        self.uuid = Some(uuid.to_string());
        let path = self.path().expect("base name was just set, this should be impossible");
        let file_handle = File::open(path)?;
        let buf_reader = BufReader::new(file_handle);
        let reader = BzDecoder::new(buf_reader);
        self.reader = Some(reader);
        Ok(())
    }

    pub fn file_size(&self) -> Option<u64> {
        let reader = self.reader.as_ref()?;
        let buf_reader = reader.get_ref();
        let file = buf_reader.get_ref();
        Some(file.metadata().ok()?.len())

        // let path = self.path();
        // let path = path.as_ref()?;
        // let path = Path::new(path);
        // let len = path.metadata().ok()?.len();
        // Some(len)
    }

    pub fn path(&self) -> Option<String> {
        let name = self.uuid.as_ref()?;
        Some(format!("{}/{name}.jsonl.bz2",APP.data_path()))
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

    pub fn writer(&mut self) -> Result<&mut BzEncoder<File>> {
        match self.writer.as_mut() {
            Some(writer) => Ok(writer),
            None => Err(anyhow!("No writer open")),
        }
    }

    pub fn read_row(&mut self) -> Option<String> {
        println!("1");
        let reader = self.reader.as_mut()?;
        println!("2");

        // Poor man's readbuf
        let line: String;
        let mut buffer = [0; BUFFER_SIZE];
        let mut bytes_read ;
        let mut eof_reached = false;
        println!("3");
        loop {
            println!("A");
            if self.bytes_read.contains(&10) {
                println!("A1");
                let first: Vec<u8> = self.bytes_read.iter().take_while(|b|**b!=10).map(|b|*b).collect();
                let second: Vec<u8> = self.bytes_read.iter().skip_while(|b|**b!=10).skip(1).map(|b|*b).collect();
                line = String::from_utf8(first).ok()?.trim().to_string();
                self.bytes_read = second;
                break;
            }
            println!("B");
            if eof_reached {
                println!("B1");
                if self.bytes_read.is_empty() {
                    return None;
                }
                println!("B2");
                let first: Vec<u8> = self.bytes_read.iter().map(|b|*b).collect();
                line = String::from_utf8(first).ok()?.trim().to_string();
                self.bytes_read.clear();
                break;
            }

            println!("C");
            bytes_read = reader.read(&mut buffer[..]).ok()?;
            self.bytes_read.append(&mut buffer.to_vec());
            eof_reached = bytes_read < BUFFER_SIZE;
            if eof_reached {
                println!("EOF REACHED");
            }
        }
        println!("X");
        println!("{line}\n");
        Some(line)
    }

    pub fn load_header(&mut self) -> Result<()> {
        let row = self.read_row().ok_or(anyhow!("No header row in JSONL file"))?;
        println!("Row");
        self.header = serde_json::from_str(&row)?;
        Ok(())
    }

    pub fn load(&mut self) -> Result<()> {
        println!("Q1");
        if self.header.columns.is_empty() {
            println!("Q2");
            self.load_header()?;
        }
        println!("Q3");
        loop {
            println!("!!");
            let row = match self.read_row() {
                Some(row) => row,
                None => break,
            };
            println!("!!!! ?{row}?");
            let row: Vec<DataCell> = serde_json::from_str(&row)?;
            self.rows.push(row);
            println!("!!!!!!");
        }
        Ok(())
    }
}