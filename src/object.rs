use std::path::PathBuf;
use array_lib::{ArrayDim, DimSize};
use array_lib::num_complex::Complex32;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use crate::copy_planner::CopyPlanner;
use crate::request::{DataRequest, DataResponse, RequestType};
use crate::scanner::{HostProperties, Scanner};
use crate::{submit_request, RequestError};

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct ObjectManagerConf {
    /// working directory for the object manager
    pub work_dir: PathBuf,
    /// directory for the remote data relative to the scanner's defined base directory
    pub remote_dir: PathBuf,
    /// max number of retries for ssh transfers
    pub max_xfer_retries: usize,
    /// max duration of ssh transfer
    pub total_xfer_timeout_sec: usize,
    /// scanner information
    pub data_host: HostProperties,
    /// file patterns to search for on the scanner to read data from. This is relative to the
    /// remote_base_dir. ex. m00/*.mrd or /*/*.mrd
    pub raw_file_patterns:Vec<PathBuf>,
    /// search patterns for meta data
    pub meta_file_patterns:Vec<PathBuf>,
    /// only consider the first meta file found for every object
    pub single_meta_file: bool,
    /// search patterns for the trajectory file
    pub trajectory_file_patterns:Vec<PathBuf>,
    /// only consider the first traj file found for every object
    pub single_traj_file: bool,
    /// description of object data layout
    pub obj_layout: Vec<DimSize>,
    /// description of raw data layout on host
    pub raw_layout: RawLayout,
}

impl Default for ObjectManagerConf {
    fn default() -> Self {

        // dummy example of a plausible data layout structure
        let obj_layout = vec![DimSize::READ(512),DimSize::PHS1(256)];
        let raw_layout = RawLayout::MixedBuffer {
            buffer_layouts: vec![
                vec![DimSize::READ(512),DimSize::PHS1(256),DimSize::SLICE(75)],
                vec![DimSize::READ(512),DimSize::PHS1(256),DimSize::SLICE(24)],
            ],
        };

        ObjectManagerConf {
            work_dir: dirs::home_dir().unwrap(),
            remote_dir: dirs::home_dir().unwrap(),
            max_xfer_retries: 10,
            total_xfer_timeout_sec: 120,
            data_host: HostProperties::default_mrsolutions(),
            raw_file_patterns: vec![PathBuf::from("results/*.cfl")],
            meta_file_patterns: vec![PathBuf::from("results/*.headfile")],
            single_meta_file: true,
            trajectory_file_patterns: vec![PathBuf::from("results/traj.cfl")],
            single_traj_file: true,
            obj_layout,
            raw_layout,
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

#[derive(Debug,Clone,Serialize,Deserialize)]
/// defines the raw data layout on the data host (scanner)
pub enum RawLayout {
    /// defines the raw layout as a uniform array of buffers with some data layout
    /// e.g. multiple .cfl files with the same size and shape
    BuffArray{buffer_layout:Vec<DimSize>,n:usize},
    /// heterogeneous mix of buffer layouts. e.g. cfl files with different sizes and shapes
    MixedBuffer{buffer_layouts:Vec<Vec<DimSize>>},
    /// single buffer with defined layout. e.g. one cfl file with a known size and shape
    Single{buffer_layout:Vec<DimSize>},
}

impl RawLayout {
    /// build the full data layout vector
    pub fn layout(&self) -> Vec<Vec<DimSize>> {
        match self {
            RawLayout::BuffArray{buffer_layout,n} => vec![buffer_layout.clone();*n],
            RawLayout::Single{buffer_layout} => vec![buffer_layout.clone()],
            RawLayout::MixedBuffer{buffer_layouts,} => buffer_layouts.clone()
        }
    }
}



#[derive(Debug,Clone,Serialize,Deserialize)]
/// defines the state information for the object manager
pub struct ObjectManager {
    /// underlying configuration
    pub conf: ObjectManagerConf,
    /// host information
    pub data_host: Scanner,
    /// copy planner handles data copying operations on the data host
    pub copy_planner: CopyPlanner,
}

impl From<ObjectManagerConf> for ObjectManager {
    fn from(conf: ObjectManagerConf) -> Self {
        let copy_planner = CopyPlanner::new(&conf.obj_layout,&conf.raw_layout.layout());
        let scanner = conf.data_host.scanner();
        ObjectManager {
            conf,
            data_host: scanner,
            copy_planner,
        }
    }
}

impl ObjectManager {
    pub fn submit_raw_request(&self, object_index:usize) -> Result<Vec<Complex32>,RequestError> {
        let req = DataRequest {
            object_index,
            obj_man: self.clone(),
            r_type: RequestType::Raw,
        };
        let resp = submit_request(req)?;
        Ok(resp.raw_payload.unwrap())
    }

    pub fn submit_traj_request(&self, object_index:usize) -> Result<(Vec<Complex32>,ArrayDim),RequestError> {
        let req = DataRequest {
            object_index,
            obj_man: self.clone(),
            r_type: RequestType::Trajectory,
        };
        let resp = submit_request(req)?;
        Ok(resp.traj_payload.unwrap())
    }

    pub fn submit_meta_request(&self, object_index:usize) -> Result<IndexMap<String,String>,RequestError> {
        let req = DataRequest {
            object_index,
            obj_man: self.clone(),
            r_type: RequestType::Metadata,
        };
        let resp = submit_request(req)?;
        Ok(resp.meta_payload.unwrap())
    }
}