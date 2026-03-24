
pub mod scanner;
mod computer;
mod copy_planner;
pub mod data_collection_mrs;
mod configs;
pub mod object;
pub mod request;

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use array_lib::{ArrayDim, DimLabel, DimSize};
use array_lib::cfl::num_complex::Complex32;
use indexmap::IndexMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use crate::copy_planner::CopyPlanner;
use crate::data_collection_mrs::{collect_meta_mrs, collect_raw_mrs, collect_traj_mrs};
use crate::object::ObjectManager;
use crate::request::{DataRequest, DataResponse};
use crate::scanner::{Scanner, HostProperties};







pub fn submit_request(req: DataRequest) -> Result<DataResponse, RequestError> {
    let cmd = req
        .obj_man
        .data_host
        .properties()
        .server_bin
        .to_string_lossy()
        .to_string();
    let start = std::time::Instant::now();
    let stdout = req.obj_man.data_host.host().run_cmd2(cmd, &[req.to_base64()]).map_err(|_|RequestError::SSHAuthentication)?;
    let re = Regex::new(r"\|\|\|(.*?)\|\|\|").expect("invalid regex");
    let cap = if let Some(cap) = re.captures(&stdout) {
        cap.get(1)
            .expect("failed to capture response")
    }else {
        println!("stdout: {}",stdout);
        panic!("failed to match regular expression from response");
    };
    let dur = start.elapsed().as_secs_f32();
    Ok(DataResponse::from_base64(cap.as_str()).unwrap())
}

pub fn decode_request(req_str:String) -> Result<DataRequest,RequestError> {
    DataRequest::from_base64(&req_str).map_err(|_|RequestError::BadRequest)
}


pub fn write_to_stdout(bytes: &[u8]) -> std::io::Result<()> {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    handle.write_all(bytes)?;
    handle.flush()?;
    Ok(())
}

pub trait Base64: Serialize + DeserializeOwned {
    fn to_base64(&self) -> String {
        use base64::Engine;
        use base64::engine::general_purpose;
        let bytes = postcard::to_stdvec(self)
            .expect("Serialization failed");
        general_purpose::STANDARD.encode(bytes)
    }

    fn from_base64(s: &str) -> Result<Self,()> {
        use base64::Engine;
        use base64::engine::general_purpose;
        let bytes = general_purpose::STANDARD
            .decode(s)
            .expect("Base64 decode failed");
        postcard::from_bytes(&bytes).map_err(|_|())
    }
}

pub trait JsonState: Serialize + DeserializeOwned {
    fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("failed to serialize")
    }

    fn from_json(s: &str) -> Self {
        serde_json::from_str(s).expect("failed to deserialize string")
    }

    fn from_json_file(file:impl AsRef<Path>) -> Self {
        let mut f = File::open(file.as_ref().with_extension("json")).expect("failed to open file");
        let mut s = String::new();
        f.read_to_string(&mut s).expect("failed to read file");
        Self::from_json(&s)
    }

    fn to_json_file(&self, file:impl AsRef<Path>) {
        let s = self.to_json();
        let mut f = File::create(file.as_ref().with_extension("json"))
            .expect("failed to create file");
        f.write_all(s.as_bytes()).expect("failed to write file");
    }

}

impl JsonState for ObjectManager {}


impl Base64 for RequestError {}

/// Request error type to describe things that can go wrong with
/// collecting data
#[derive(Serialize, Deserialize, Debug)]
pub enum RequestError {
    IO(String),
    TrajFileIndexOutOfBounds(usize,usize),
    BadSearchPattern(String),
    DataNotReady,
    DataNotFound(String),
    BufferIndexNotFound(usize),
    RawFileExtNotDefined(String),
    UnsupportedRawFileType(String),
    UnsupportedTrajFileType(String),
    UnexpectedDataLayout(Vec<usize>,Vec<usize>),
    BadRequest,
    CannotCreateDirectory(PathBuf),
    FailedToWriteCfl,
    FailedToReadResponse,
    FailedToReadRequest,
    DataTransfer,
    CleanupFailure,
    CannotGetViewTable,
    CannotGetMetaData,
    CannotWriteViewTable,
    CannotReadViewTable(PathBuf),
    ViewTableNotFound(PathBuf),
    FailedToResolveDataRequest,
    FailedToOpenBrukerData,
    FailedToOpenAgilentFid(PathBuf,PathBuf),
    FailedToGetAgilentMetaData(PathBuf),
    FailedToFindMrdFile(PathBuf),
    FailedToOpenMrdFile(PathBuf),
    FailedToExtractMrdData(PathBuf),
    FailedToExtractBrukerData(PathBuf),
    FailedToExtractAgilentData(PathBuf),
    AgilentError,
    FailedToConvertStreamToRaw,
    SSHAuthentication,
    BadPETable,
}

impl From<std::io::Error> for RequestError {
    fn from(e: std::io::Error) -> Self {
        RequestError::IO(e.to_string())
    }
}