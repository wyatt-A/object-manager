
pub mod scanner;
mod computer;

use std::path::PathBuf;
use array_lib::{ArrayDim, DimLabel, DimSize};
use array_lib::cfl::num_complex::Complex32;
use indexmap::IndexMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
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

    /// these are the dimensions of the object requested from the scanner
    object_dims: ArrayDim,

    /// expected layout of each raw file on the scanner
    raw_layout: Vec<(DimLabel,usize)>
}

impl Default for ObjectManagerConf {
    fn default() -> Self {
        ObjectManagerConf {
            n_objects: 1,
            work_dir: dirs::home_dir().unwrap(),
            remote_dir: dirs::home_dir().unwrap(),
            max_xfer_retries: 1,
            total_xfer_timeout_sec: 120,
            scanner: Scanner::MrSolutions(ScannerProperties::default_mrsolutions()),
            raw_file_patterns: vec![PathBuf::from("results/*.cfl")],
            object_dims: ArrayDim::new().with_dim_from_label(DimSize::READ(512)).with_dim_from_label(DimSize::PHS1(8192)),
            raw_layout: vec![(DimLabel::READ,512),(DimLabel::PHS1,8192),(DimLabel::SLICE,150)],
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

#[derive(Debug,Serialize,Deserialize)]
pub enum RequestType {
    /// raw MRI data
    Raw,
    /// cartesian trajectory information (phase encoding table)
    TrajectoryCart,
    /// non-cartesian trajectory information (full traj file)
    TrajectoryNonCart,
    /// meta data request
    Metadata
}

#[derive(Debug,Serialize,Deserialize)]
pub struct DataRequest {
    r_type: RequestType,
    object_index: usize,
    conf: ObjectManagerConf,
}

impl Base64 for DataRequest {}

impl DataRequest {
    pub fn from_conf(conf: &ObjectManagerConf, request_type:RequestType, obj_index:usize) -> Self {
        DataRequest {
            r_type: request_type,
            object_index: obj_index,
            conf: conf.clone()
        }
    }
}

#[derive(Debug,Serialize,Deserialize)]
pub struct DataResponse {
    raw_payload:Option<(Vec<Complex32>,ArrayDim)>,
    meta_payload:Option<IndexMap<String,String>>,
    req: DataRequest,
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
    Ok(DataResponse::from_base64(cap.as_str()))
}

trait Base64: Serialize + DeserializeOwned {
    fn to_base64(&self) -> String {
        use base64::Engine;
        use base64::engine::general_purpose;
        let bytes = postcard::to_stdvec(self)
            .expect("Serialization failed");
        general_purpose::STANDARD.encode(bytes)
    }

    fn from_base64(s: &str) -> Self {
        use base64::Engine;
        use base64::engine::general_purpose;
        let bytes = general_purpose::STANDARD
            .decode(s)
            .expect("Base64 decode failed");
        postcard::from_bytes(&bytes)
            .expect("Deserialization failed")
    }
}

/// Request error type to describe things that can go wrong with
/// collecting data
#[derive(Serialize, Deserialize, Debug)]
pub enum RequestError {
    DataNotReady,
    DataNotFound,
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