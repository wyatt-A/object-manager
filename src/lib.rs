
pub mod scanner;
mod computer;
mod copy_planner;
mod data_collection;


use std::io::Write;
use std::path::PathBuf;
use array_lib::{ArrayDim, DimLabel, DimSize};
use array_lib::cfl::num_complex::Complex32;
use indexmap::IndexMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use crate::copy_planner::CopyPlanner;
use crate::data_collection::{collect_meta_mrs, collect_raw_mrs, collect_traj_mrs};
use crate::scanner::{Scanner, ScannerProperties};

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct ObjectManagerConf {

    /// total number of expected objects from experiment
    n_objects:usize,

    /// working directory for the object manager
    work_dir: PathBuf,

    /// directory for the remote data relative to the scanner's defined base directory
    remote_dir: PathBuf,
    /// max number of retries for ssh transfers
    max_xfer_retries: usize,
    /// max duration of ssh transfer
    total_xfer_timeout_sec: usize,

    /// scanner information
    scanner: Scanner,

    /// file patterns to search for on the scanner to read data from. This is relative to the
    /// remote_base_dir. ex. m00/*.mrd or /*/*.mrd
    raw_file_patterns:Vec<PathBuf>,

    /// search patterns for meta data
    meta_file_patterns:Vec<PathBuf>,
    /// only consider the first meta file found for every object
    single_meta_file: bool,

    /// search patterns for the trajectory file
    traj_file_patterns:Vec<PathBuf>,
    /// only consider the first traj file found for every object
    single_traj_file: bool,


    /// copy planner handles data copying operations on the data host
    copy_planner: CopyPlanner,

}

impl Default for ObjectManagerConf {
    fn default() -> Self {

        // dummy example of a plausible data layout structure
        let obj_layout = vec![DimSize::READ(512),DimSize::PHS1(256)];
        let raw_layout = vec![
            vec![DimSize::READ(512),DimSize::PHS1(256),DimSize::SLICE(75)],
            vec![DimSize::READ(512),DimSize::PHS1(256),DimSize::SLICE(24)],
        ];

        let copy_planner = CopyPlanner::new(&obj_layout,&raw_layout);

        ObjectManagerConf {
            n_objects: 1,
            work_dir: dirs::home_dir().unwrap(),
            remote_dir: dirs::home_dir().unwrap(),
            max_xfer_retries: 1,
            total_xfer_timeout_sec: 120,
            scanner: Scanner::MrSolutions(ScannerProperties::default_mrsolutions()),
            raw_file_patterns: vec![PathBuf::from("results/*.cfl")],
            meta_file_patterns: vec![PathBuf::from("results/*.headfile")],
            single_meta_file: true,
            traj_file_patterns: vec![PathBuf::from("results/traj.cfl")],
            single_traj_file: true,
            copy_planner
        }
    }
}

impl ObjectManagerConf {
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(&self).unwrap()
    }

    pub fn from_json(s: &str) -> Self {
        serde_json::from_str(s).unwrap()
    }

}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub enum RequestType {
    /// raw MRI data
    Raw,
    /// cartesian or non-cartesian trajectory information
    Traj,
    /// meta data request
    Metadata
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct DataRequest {
    r_type: RequestType,
    object_index: usize,
    conf: ObjectManagerConf,
}

impl Base64 for DataRequest {}

impl DataRequest {
    pub fn from_conf(conf: &ObjectManagerConf, request_type:RequestType, object_index:usize) -> Self {
        DataRequest {
            r_type: request_type,
            object_index,
            conf: conf.clone()
        }
    }
}

#[derive(Debug,Serialize,Deserialize)]
pub struct DataResponse {
    pub raw_payload:Option<Vec<Complex32>>,
    pub meta_payload:Option<IndexMap<String,String>>,
    pub traj_payload:Option<(Vec<Complex32>,ArrayDim)>,
    pub req: Option<DataRequest>,
    pub error: Option<RequestError>,
}

impl Base64 for DataResponse {}

pub fn submit_request(req: DataRequest) -> Result<DataResponse, RequestError> {
    let cmd = req
        .conf
        .scanner
        .properties()
        .server_bin
        .to_string_lossy()
        .to_string();
    let start = std::time::Instant::now();
    let stdout = req.conf.scanner.host().run_cmd2(cmd, &[req.to_base64()]).map_err(|_|RequestError::SSHAuthentication)?;
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


pub fn handle_request_mrs(req: &DataRequest) -> Result<DataResponse, RequestError> {
    match &req.r_type {
        RequestType::Raw => {
            let raw = collect_raw_mrs(&req.conf,req.object_index)?;
            Ok(DataResponse {
                raw_payload: Some(raw),
                meta_payload: None,
                traj_payload: None,
                req:Some(req.clone()),
                error: None,
            })
        },
        RequestType::Metadata => {
            let meta = collect_meta_mrs(&req.conf,req.object_index)?;
            Ok(DataResponse {
                raw_payload: None,
                meta_payload: Some(meta),
                traj_payload: None,
                req:Some(req.clone()),
                error: None,
            })
        },
        RequestType::Traj => {
            let traj = collect_traj_mrs(&req.conf,req.object_index)?;
            Ok(DataResponse {
                raw_payload: None,
                meta_payload: None,
                traj_payload: Some(traj),
                req:Some(req.clone()),
                error: None,
            })
        }
    }
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



impl Base64 for RequestError {}

/// Request error type to describe things that can go wrong with
/// collecting data
#[derive(Serialize, Deserialize, Debug)]
pub enum RequestError {

    TrajFileIndexOutOfBounds(usize,usize),
    BadSearchPattern(String),
    DataNotReady,
    DataNotFound,
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
}