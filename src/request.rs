use cs_table::ViewTable;
use mr_data::{
    agilent_fid::AgilentFidError, base64_encoding::AsBase64, bruker_fid::BrukerFidError, dim_order::{self, resolve_buffer_indices, DimIndex, DimLabel, DimOrder}, raw::Raw
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing_subscriber::fmt::time::SystemTime;
use std::{
    collections::HashMap, fmt::Display, io::Write, path::PathBuf
};
use crate::resource_manager::data_collection::{collect_meta_data_agilent, collect_raw_stream_agilent, collect_view_table_agilent};

use super::{
    data_collection::{
        collect_meta_data_bruker, collect_meta_data_mrs, collect_raw_stream_bruker,
        collect_raw_stream_mrs, collect_view_table_bruker, collect_view_table_mrs,
    },
    scanner::Scanner,
};
use tracing::info;

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
    FailedToOpenBrukerData(BrukerFidError),
    FailedToOpenAgilentFid(PathBuf,PathBuf),
    FailedToGetAgilentMetaData(PathBuf),
    FailedToFindMrdFile(PathBuf),
    FailedToOpenMrdFile(PathBuf),
    FailedToExtractMrdData(PathBuf),
    FailedToExtractBrukerData(PathBuf),
    FailedToExtractAgilentData(PathBuf),
    AgilentError(AgilentFidError),
    FailedToConvertStreamToRaw,
    SSHAuthentication,
}

/// request info specifies everything needed to fetch some data from a scanner
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestInfo {
    pub request_type: RequestType,
    pub resource_base_dir: PathBuf,
    pub base_dir_ext: Vec<String>,
    pub file_layout: Vec<usize>,
    pub scanner: Scanner,
    pub data_set_layout: DimOrder,
    pub dim_indices: Vec<DimIndex>,
    pub max_xfer_retries: usize,
    pub total_xfer_timeout_sec: usize,
}

impl AsBase64 for RequestInfo {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RequestType {
    RawData{meta_data_path:Option<PathBuf>},
    ViewTable{table_path:Option<PathBuf>},
    MetaData{meta_data_path:Option<PathBuf>},
}

#[derive(Serialize, Deserialize)]
pub enum Response {
    Success {
        request_type: RequestType,
        payload_base64: String,
    },
    Failure(RequestError),
}

impl TryFrom<Response> for Raw {
    type Error = RequestError;
    fn try_from(value: Response) -> Result<Self, Self::Error> {
        match value {
            Response::Success {
                request_type,
                payload_base64,
            } => match request_type {
                RequestType::RawData{..} => Ok(Raw::from_base64(&payload_base64)),
                _ => panic!("unexpected request"),
            },
            Response::Failure(error) => Err(error),
        }
    }
}

impl TryFrom<Response> for ViewTable {
    type Error = RequestError;
    fn try_from(value: Response) -> Result<Self, Self::Error> {
        match value {
            Response::Success {
                request_type,
                payload_base64,
            } => match request_type {
                RequestType::ViewTable{..} => Ok(ViewTable::from_base64(&payload_base64)),
                _ => panic!("unexpected request"),
            },
            Response::Failure(error) => Err(error),
        }
    }
}

impl TryFrom<Response> for HashMap<String, String> {
    type Error = RequestError;
    fn try_from(value: Response) -> Result<Self, Self::Error> {
        match value {
            Response::Success {
                request_type,
                payload_base64,
            } => match request_type {
                RequestType::MetaData{ .. } => {
                    Ok(HashMap::<String, String>::from_base64(&payload_base64))
                }
                _ => panic!("unexpected request"),
            },
            Response::Failure(error) => Err(error),
        }
    }
}

impl AsBase64 for Response {}

// client side
pub fn fill_request(info: RequestInfo) -> Result<Response, RequestError> {
    let cmd = info
        .scanner
        .properties()
        .server_bin
        .to_string_lossy()
        .to_string();
    info!("sending request to {}",info.scanner.host());
    let start = std::time::Instant::now();
    info!("running: {} {}",cmd,info.to_base64());
    let stdout = info.scanner.host().run_cmd2(cmd, &[info.to_base64()]).map_err(|_|RequestError::SSHAuthentication)?;
    let re = Regex::new(r"\|\|\|(.*?)\|\|\|").expect("invalid regex");
    let cap = if let Some(cap) = re.captures(&stdout) {
        cap.get(1)
        .expect("failed to capture response")
    }else {
        println!("stdout: {}",stdout);
        panic!("failed to match regular expression from response");
    };
    let dur = start.elapsed().as_secs_f32();
    info!("request took {} seconds to fill",dur);
    Ok(Response::from_base64(cap.as_str()))
}

// server side
pub fn handle_request(request_str: &str) {
    let req_info = RequestInfo::from_base64(request_str);
    let response_str = collect_data(req_info).to_base64();
    let stdout = format!("|||{}|||", response_str);
    write_to_stdout(&stdout.as_bytes()).expect("failed to write to stdout");
}

// server-side
fn collect_data(info: RequestInfo) -> Response {
    println!("handling request for {:?}",info.request_type);
    match info.request_type {
        RequestType::RawData{..} => match collect_raw(&info) {
            Err(e) => Response::Failure(e),
            Ok(raw) => Response::Success {
                request_type: info.request_type,
                payload_base64: raw.to_base64(),
            },
        },
        RequestType::ViewTable{ .. } => match collect_view_table(&info) {
            Err(e) => Response::Failure(e),
            Ok(vt) => Response::Success {
                request_type: info.request_type,
                payload_base64: vt.to_base64(),
            },
        },
        RequestType::MetaData{..} => match collect_meta(&info) {
            Err(e) => Response::Failure(e),
            Ok(meta) => Response::Success {
                request_type: info.request_type,
                payload_base64: meta.to_base64(),
            },
        },
    }
}

fn write_to_stdout(bytes: &[u8]) -> std::io::Result<()> {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    handle.write_all(bytes)?;
    handle.flush()?;
    Ok(())
}

pub fn collect_raw(request_info: &RequestInfo) -> Result<Raw, RequestError> {
    let (_,&last_dim_size) = request_info.data_set_layout.last();
    let n_outer = request_info.file_layout.iter().sum::<usize>();
    assert_eq!(last_dim_size,n_outer);
    let (ext_idx,indices,new_dim_order) = dim_order::resolve_local_indices(
        request_info.data_set_layout.clone(),
        &request_info.file_layout,
        &request_info.dim_indices
    );
    let mut raw_stream = vec![];

    let meta_data = if let RequestType::RawData { meta_data_path } = request_info.request_type.clone() {
        meta_data_path
    }else {
        None
    };

    for (&dir_idx,indices) in ext_idx.iter().zip(indices.iter()) {

        println!("dir idx: {}",dir_idx);
        println!("indices head: {:?}",&indices[0..20]);

        let resolved_dir = if let Some(ext) = request_info.base_dir_ext.get(dir_idx) {
            request_info.resource_base_dir.join(ext)
        }else {
            request_info.resource_base_dir.to_owned()
        };

        println!("resolved dir: {}",resolved_dir.to_string_lossy());

        let mut rs = match request_info.scanner {
            Scanner::Bruker(_) => collect_raw_stream_bruker(resolved_dir, indices)?,
            Scanner::MrSolutions(_) => collect_raw_stream_mrs(resolved_dir, indices)?,
            Scanner::Agilent(_) => collect_raw_stream_agilent(resolved_dir, indices, meta_data.clone())?,
        };
        raw_stream.append(&mut rs)
    }
    Raw::from_stream(raw_stream, new_dim_order).map_err(|_| RequestError::FailedToConvertStreamToRaw)
}

pub fn collect_meta(request_info: &RequestInfo) -> Result<HashMap<String, String>, RequestError> {

    let (buff_indices,_,_) = dim_order::resolve_local_indices(
        request_info.data_set_layout.clone(),
        &request_info.file_layout,
        &request_info.dim_indices
    );

    assert_eq!(buff_indices.len(),1);
    let resolved_dir = if let Some(ext) = request_info.base_dir_ext.get(buff_indices[0]) {
        request_info.resource_base_dir.join(ext)
    }else {
        request_info.resource_base_dir.to_owned()
    };

    let meta_data = if let RequestType::MetaData { meta_data_path } = request_info.request_type.clone() {
        meta_data_path
    }else {
        None
    };

    match request_info.scanner {
        Scanner::Bruker(_) => collect_meta_data_bruker(resolved_dir),
        Scanner::MrSolutions(_) => collect_meta_data_mrs(resolved_dir),
        Scanner::Agilent(_) => {
            let vol_idx = request_info.dim_indices.last().and_then(|idx|Some(idx.index())).unwrap_or(0);
            println!("gathering meta data for vol idx {}",vol_idx);
            collect_meta_data_agilent(resolved_dir,vol_idx,meta_data)
        },
    }
}

pub fn collect_view_table(request_info: &RequestInfo) -> Result<ViewTable, RequestError> {
    let (buff_indices,_,_) = dim_order::resolve_local_indices(
        request_info.data_set_layout.clone(),
        &request_info.file_layout,
        &request_info.dim_indices
    );
    assert_eq!(buff_indices.len(),1);
    let resolved_dir = if let Some(ext) = request_info.base_dir_ext.get(buff_indices[0]) {
        request_info.resource_base_dir.join(ext)
    }else {
        request_info.resource_base_dir.to_owned()
    };
    match request_info.scanner {
        Scanner::Bruker(_) => collect_view_table_bruker(resolved_dir),
        Scanner::MrSolutions(_) => collect_view_table_mrs(resolved_dir),
        Scanner::Agilent(_) => {
            let table_path = match &request_info.request_type {
                RequestType::ViewTable { table_path } => table_path.as_ref().map(|p|p.to_owned()),
                _=> None
            };
            collect_view_table_agilent(resolved_dir,table_path, false)
        }
    }
}