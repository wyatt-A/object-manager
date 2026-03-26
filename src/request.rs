use std::fmt::Display;
use array_lib::ArrayDim;
use array_lib::num_complex::Complex32;
use clap::ValueEnum;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use crate::{Base64, RequestError};
use crate::object::ObjectManager;

#[derive(Debug,Serialize,Deserialize,Clone,ValueEnum)]
pub enum RequestType {
    /// raw MRI data
    Raw,
    /// cartesian or non-cartesian trajectory information
    Trajectory,
    /// meta data request
    Metadata
}

impl Display for RequestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct DataRequest {
    /// object index to request
    pub object_index: usize,
    /// object manager state
    pub obj_man: ObjectManager,
    /// data request type
    pub r_type: RequestType,
}

#[derive(Debug,Serialize,Deserialize)]
pub struct DataResponse {
    pub raw_payload:Option<(Vec<Complex32>,ArrayDim)>,
    pub meta_payload:Option<IndexMap<String,String>>,
    pub traj_payload:Option<(Vec<Complex32>,ArrayDim)>,
    pub req: Option<DataRequest>,
    pub error: Option<RequestError>,
}

impl Base64 for DataResponse {}
impl Base64 for DataRequest {}