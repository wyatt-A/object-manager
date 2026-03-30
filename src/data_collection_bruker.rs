use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use array_lib::{io_bruker, ArrayDim};
use array_lib::io_cfl::read_cfl;
use array_lib::io_mrd::read_mrd;
use array_lib::num_complex::Complex32;
use bruker_jcamp_rs::parse_paravision_params;
use headfile::Headfile;
use indexmap::IndexMap;
use crate::copy_planner::CopyPlanner;
use crate::data_collection_mrs::{collect_meta_mrs, collect_raw_mrs, collect_traj_mrs};
use crate::request::{DataRequest, DataResponse, RequestType};
use crate::RequestError;

pub fn handle_request_bruker(req:&DataRequest) -> Result<DataResponse, RequestError> {

    let base_dir = if let Some(base_dir) = req.obj_man.data_host.base_dir().as_ref() {
        base_dir.join(&req.obj_man.conf.remote_dir)
    }else {
        req.obj_man.conf.remote_dir.clone()
    };

    match req.r_type {
        RequestType::Raw => {
            let patterns = &req.obj_man.conf.raw_file_patterns;
            let copy_planner = &req.obj_man.copy_planner;
            let raw = collect_raw_bruker(&base_dir,patterns,copy_planner,req.object_index)?;
            let resp = DataResponse {
                raw_payload: Some(raw),
                meta_payload: None,
                traj_payload: None,
                req: Some(req.clone()),
                error: None,
            };
            Ok(resp)
        } ,
        RequestType::Trajectory => {
            let patterns = &req.obj_man.conf.trajectory_file_patterns;
            let traj = collect_traj_bruker(&base_dir,patterns,req.object_index,req.obj_man.conf.single_traj_file)?;
            let resp = DataResponse {
                raw_payload: None,
                meta_payload: None,
                traj_payload: Some(traj),
                req: Some(req.clone()),
                error: None,
            };
            Ok(resp)
        }
        RequestType::Metadata => {
            let patterns = &req.obj_man.conf.meta_file_patterns;
            let meta = collect_meta_bruker(&base_dir,patterns,req.object_index,req.obj_man.conf.single_meta_file)?;
            let resp = DataResponse {
                raw_payload: None,
                meta_payload: Some(meta),
                traj_payload: None,
                req: Some(req.clone()),
                error: None,
            };
            Ok(resp)
        }
    }


}

pub fn collect_raw_bruker(base_dir:impl AsRef<Path>, file_patterns:&[PathBuf], copy_planner:&CopyPlanner, object_index:usize) -> Result<(Vec<Complex32>, ArrayDim), RequestError> {

    let buff_idx = copy_planner.group_index(object_index);

    let mut raw_files = vec![];
    let mut patterns = vec![];

    for pattern in file_patterns {
        let pat = base_dir.as_ref().join(pattern);
        println!("looking for pattern: {}",pat.display());
        patterns.push(pat.to_string_lossy().to_string());
        for path in glob::glob(&pat.to_string_lossy().to_string())
            .map_err(|e| RequestError::BadSearchPattern(e.to_string()))?.filter_map(Result::ok) {
            raw_files.push(path)
        }
    }

    if raw_files.len() < buff_idx {
        return Err(RequestError::BufferIndexNotFound(buff_idx));
    }

    let buffer_to_open = &raw_files[buff_idx];

    let obj_dims = copy_planner.obj_dims();
    let mut dst = obj_dims.alloc(Complex32::ZERO);

    let (src,_) = io_bruker::read_bruker_fid(buffer_to_open.parent().unwrap())
        .map_err(|e|RequestError::BrukerData(e.to_string()))?;

    copy_planner.copy_data(object_index,&src,&mut dst);
    Ok((dst,obj_dims))
}

pub fn collect_meta_bruker(dir:impl AsRef<Path>, file_patterns:&[PathBuf], object_index:usize, single_file:bool) -> Result<IndexMap<String,String>,RequestError> {

    let candidate_files = find_matches(dir, &file_patterns, true)?;

    if candidate_files.is_empty() {
        return Err(RequestError::CannotGetMetaData)
    }

    if object_index >= candidate_files.len() {
        return Err(RequestError::CannotGetMetaData)
    }

    let file = if single_file {
        &candidate_files[0]
    }else {
        &candidate_files[object_index]
    };

    let pv_acqp = parse_paravision_params(&file).unwrap();
    let pv_method = parse_paravision_params(file.with_file_name("method")).unwrap();

    let mut params = pv_acqp.to_hash();
    params.extend(pv_method.to_hash());

    Ok(params)

}

pub fn collect_traj_bruker(dir:impl AsRef<Path>, file_patterns:&[PathBuf], object_index:usize, single_traj:bool) -> Result<(Vec<Complex32>,ArrayDim),RequestError> {

    let mut candidate_files = find_matches(dir, file_patterns, true)?;

    let file = if single_traj {
        &candidate_files[0]
    }else {
        if object_index >= candidate_files.len() {
            return Err(RequestError::CannotGetViewTable)
        }
        &candidate_files[object_index]
    };

    let mut f = File::open(&file)?;
    let mut s = String::new();
    f.read_to_string(&mut s)?;
    // paravision formatted cs table
    let entries:Vec<&str> = s.lines().skip(3).collect();

    let n = entries.len() / 2;

    let dims = ArrayDim::from_shape(&[2,n]);
    let traj:Vec<Complex32> = entries.iter().map(|x| x.parse::<i32>().unwrap()).map(|x| Complex32::new(x as f32,0.0)).collect();
    assert_eq!(traj.len(), dims.numel());

    Ok((traj,dims))
}

fn find_matches(base_dir:impl AsRef<Path>, search_patterns:&[PathBuf], sort:bool) -> Result<Vec<PathBuf>,RequestError> {

    let search_pats:Vec<PathBuf> = search_patterns.iter().map(|pat|{
        base_dir.as_ref().join(pat)
    }).collect();

    let mut matches = vec![];
    for pat in search_pats {
        for path in glob::glob(&pat.to_string_lossy().to_string())
            .map_err(|e| RequestError::BadSearchPattern(e.to_string()))?.filter_map(Result::ok) {
            matches.push(path)
        }
    }
    if sort {
        matches.sort();
    }
    Ok(matches)
}