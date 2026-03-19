use std::path::PathBuf;
use array_lib::ArrayDim;
use array_lib::io_cfl::read_cfl;
use array_lib::io_mrd::{read_mrd, read_mrd_header};
use array_lib::num_complex::Complex32;
use crate::{ObjectManagerConf, RequestError};
use glob;
use indexmap::IndexMap;

pub fn collect_meta_mrs(conf:&ObjectManagerConf, obj_idx:usize) -> Result<IndexMap<String,String>,RequestError> {
    let mut meta_files = vec![];
    for pattern in &conf.meta_file_patterns {
        let pat = conf.remote_dir.join(pattern);
        for path in glob::glob(&pat.to_string_lossy().to_string())
            .map_err(|e| RequestError::BadSearchPattern(e.to_string()))?.filter_map(Result::ok) {
            meta_files.push(path)
        }
    }

    // match on file type



    todo!()

}

pub fn collect_traj_mrs(conf:&ObjectManagerConf, obj_idx:usize) -> Result<(Vec<Complex32>,ArrayDim),RequestError> {

    let mut traj_files = vec![];
    for pattern in &conf.traj_file_patterns {
        let pat = conf.remote_dir.join(pattern);
        for path in glob::glob(&pat.to_string_lossy().to_string())
            .map_err(|e| RequestError::BadSearchPattern(e.to_string()))?.filter_map(Result::ok) {
            traj_files.push(path)
        }
    }

    traj_files.sort();

    let traj_to_load = if conf.single_traj_file {
        &traj_files[0]
    }else {
        if obj_idx >= traj_files.len() {
            return Err(RequestError::TrajFileIndexOutOfBounds(obj_idx,traj_files.len()))
        }else {
            &traj_files[obj_idx]
        }
    };

    let file_ext = traj_to_load.extension();
    match file_ext {
        None => {
            // assume this is a standard cs stream table
            todo!()
        },
        Some(ext) => {
            if ext != "cfl" {
                return Err(RequestError::UnsupportedTrajFileType(ext.to_string_lossy().to_string()))
            }
            let (traj,dims) = read_cfl(traj_to_load);
            Ok((traj,dims))
        }
    }
}



pub fn collect_raw_mrs(conf:&ObjectManagerConf, obj_index:usize) -> Result<Vec<Complex32>, RequestError> {

    let mut raw_files = vec![];
    for pattern in &conf.raw_file_patterns {
        let pat = conf.remote_dir.join(pattern);
        for path in glob::glob(&pat.to_string_lossy().to_string())
            .map_err(|e| RequestError::BadSearchPattern(e.to_string()))?.filter_map(Result::ok) {
            raw_files.push(path)
        }
    }

    if raw_files.is_empty() {
        return Err(RequestError::DataNotFound)
    }
    raw_files.sort();
    let buff_idx = conf.copy_planner.group_index(obj_index);
    if buff_idx >= raw_files.len() {
        return Err(RequestError::BufferIndexNotFound(buff_idx))
    }

    let buffer_to_open = &raw_files[buff_idx];

    let file_ext = buffer_to_open.extension()
        .ok_or(RequestError::RawFileExtNotDefined(buffer_to_open.to_string_lossy().to_string()))?;

    let mut dst = conf.copy_planner.obj_dims().alloc(Complex32::ZERO);

    let (src,dims) = match  file_ext.to_str().unwrap() {
        "mrd" => {
            let (src,dim,..) = read_mrd(buffer_to_open);
            (src,dim)
        },
        "cfl" => {
            let (src,dim,..) = read_cfl(buffer_to_open);
            (src,dim)
        }
        _=> return Err(RequestError::UnsupportedRawFileType(file_ext.to_string_lossy().to_string()))
    };

    let expected_dims = conf.copy_planner.src_dims(obj_index);
    if dims.shape() != expected_dims.shape() {
        return Err(RequestError::UnexpectedDataLayout(expected_dims.shape().to_vec(),dims.shape().to_vec()))
    }

    conf.copy_planner.copy_data(obj_index,&src,&mut dst);
    Ok(dst)
}








// use super::request::RequestError::{self, *};
// use base64::{engine::general_purpose, Engine};
// use civm_rust_utils::find_files;
// use cs_table::ViewTable;
// use headfile::headfile::{json_to_hashmap, Headfile};
// use mr_data::agilent_fid::{self, ProcPar};
// use mr_data::{agilent_fid::AgilentFid, bruker_fid::BrukerData};
// use mr_data::mrd2::Mrd;
// use num_complex::Complex32;
// use tracing::info;
// use std::path::PathBuf;
// use std::{collections::HashMap, fs::File, io::Read, path::Path};
//
// pub fn collect_raw_stream_mrs(
//     mrd_directory: impl AsRef<Path>,
//     sample_indices: &[usize],
// ) -> Result<Vec<Complex32>, RequestError> {
//     // find the mrd file
//     let mrd_file = &find_files(mrd_directory.as_ref(), "mrd", true)
//         .ok_or(FailedToFindMrdFile(mrd_directory.as_ref().to_owned()))?[0];
//     // check that the "ac" file exists for completion checking
//     let _ = find_files(&mrd_directory.as_ref(), "ac", false).ok_or(DataNotReady)?;
//     let mrd = Mrd::from_file(mrd_file).map_err(|_| FailedToOpenMrdFile(mrd_file.to_owned()))?;
//     mrd.extract_data(sample_indices)
//         .map_err(|_| FailedToExtractMrdData(mrd_file.to_owned()))
// }
//
// pub fn collect_meta_data_mrs(
//     data_dir: impl AsRef<Path>,
// ) -> Result<HashMap<String, String>, RequestError> {
//     // meta.txt is a requirement
//     match civm_rust_utils::find_files_by_name(data_dir.as_ref(), "meta.txt", false) {
//         None => Err(CannotGetMetaData)?,
//         Some(meta_files) => {
//             let f = meta_files.first().expect("file to exist");
//             println!("found meta file {:?}",f);
//             let mut h = Headfile::open(f).to_hash();
//             if let Some(ppl) = find_files(data_dir.as_ref(), "ppl", true) {
//                 let ppl = ppl.first().expect("file to exist");
//                 let s = civm_rust_utils::read_to_string(ppl, None);
//                 let b64_encoded = general_purpose::STANDARD.encode(s.as_bytes());
//                 h.insert("ppl_base64".to_string(), b64_encoded);
//             }
//
//             println!("headfile: {:#?}",h);
//
//             // // results stores extra data from json files
//             // let mut results = HashMap::<String, String>::new();
//             // if let Some(json_files) = find_files(data_dir.as_ref(), "json", true) {
//             //     // ingore raw_to_kspace files
//             //     let ignore_pat =
//             //         regex::Regex::new(r"raw_to_kspace").expect("this should be valid regex");
//
//             //     let filtered_jsons: Vec<_> = json_files
//             //         .into_iter()
//             //         .filter(|file| {
//             //             let name = file
//             //                 .file_name()
//             //                 .expect("failed to get file name")
//             //                 .to_string_lossy();
//             //             !ignore_pat.is_match(&name)
//             //         })
//             //         .collect();
//
//             //     for json_file in filtered_jsons {
//             //         let mut f = File::open(json_file).expect("failed to open file");
//             //         let mut s = String::new();
//             //         f.read_to_string(&mut s).expect("failed to read file");
//             //         let val = serde_json::to_value(&s).expect("failed to deserialize json");
//             //         results.extend(json_to_hashmap(&val));
//             //     }
//             //     // we want h to take presidence in case of a key collision
//             //     results.extend(h);
//             // }
//             Ok(h)
//         }
//     }
// }
//
// pub fn collect_view_table_mrs(data_dir: impl AsRef<Path>) -> Result<ViewTable, RequestError> {
//     let files = civm_rust_utils::find_files_by_name(data_dir.as_ref(), "cs_table", false);
//     match files {
//         Some(files) => {
//             let view_table_file = files.first().unwrap();
//             ViewTable::from_file(view_table_file)
//                 .map_err(|_| RequestError::CannotReadViewTable(view_table_file.to_path_buf()))
//         }
//         None => Err(RequestError::ViewTableNotFound(
//             data_dir.as_ref().to_owned(),
//         )),
//     }
// }
//
// pub fn collect_raw_stream_bruker(
//     fid_directory: impl AsRef<Path>,
//     sample_indices: &[usize],
// ) -> Result<Vec<Complex32>, RequestError> {
//     let mut bruker_data =
//         BrukerData::open(fid_directory.as_ref()).map_err(FailedToOpenBrukerData)?;
//     bruker_data
//         .extract_data(sample_indices)
//         .map_err(|_| DataNotReady)
// }
//
// pub fn collect_view_table_bruker(
//     fid_directory: impl AsRef<Path>,
// ) -> Result<ViewTable, RequestError> {
//     let mut bruker_data =
//         BrukerData::open(fid_directory.as_ref()).map_err(FailedToOpenBrukerData)?;
//     bruker_data
//         .extract_view_table()
//         .map_err(FailedToOpenBrukerData)
// }
//
// pub fn collect_meta_data_bruker(
//     fid_directory: impl AsRef<Path>,
// ) -> Result<HashMap<String, String>, RequestError> {
//     match BrukerData::open(fid_directory.as_ref())
//         .map_err(FailedToOpenBrukerData)?
//         .collect_meta()
//     {
//         Ok(meta) => Ok(meta),
//         Err(_) => Err(CannotGetMetaData),
//     }
// }
//
// pub fn collect_raw_stream_agilent(
//     fid_directory: impl AsRef<Path>,
//     sample_indices: &[usize],
//     alternative_procpar:Option<impl AsRef<Path>>
// ) -> Result<Vec<Complex32>, RequestError> {
//     let (fid_file,procpar) = find_agilent_fid(fid_directory,alternative_procpar)?;
//     let fid = AgilentFid::open(&fid_file, procpar.clone()).map_err(|_|RequestError::FailedToOpenAgilentFid(fid_file.clone(),procpar))?;
//     fid.extract_data(sample_indices).map_err(|_|RequestError::FailedToExtractAgilentData(fid_file))
// }
//
//
// fn find_agilent_fid(primary_dir: impl AsRef<Path>,alternative_procpar:Option<impl AsRef<Path>>) -> Result<(PathBuf,PathBuf),RequestError> {
//     if primary_dir.as_ref().exists() {
//         let fid_file = primary_dir.as_ref().join("fid");
//         let proc = primary_dir.as_ref().join("procpar");
//         println!("resolved fid: {:?}",fid_file);
//         println!("resolved procpar: {:?}",proc);
//         Ok((fid_file,proc))
//     }else { // handling for incomplete scans
//         let mut possible_fids: Vec<PathBuf> = vec![];
//         let mut possible_procpars: Vec<PathBuf> = vec![];
//         let possible_locations = vec![
//             "/home/vnmr1/vnmrsys/exp1/acqfil",
//             "/home/vnmr1/vnmrsys/exp2/acqfil",
//             "/home/vnmr1/vnmrsys/exp3/acqfil",
//         ];
//         possible_locations.iter().for_each(|path| {
//             if let Some(files) =
//                 civm_rust_utils::find_files_by_name(Path::new(path), "fid", false)
//             {
//                 possible_fids.extend(files)
//             }
//         });
//         possible_locations.iter().for_each(|path| {
//             if let Some(files) =
//                 civm_rust_utils::find_files_by_name(Path::new(path), "procpar", false)
//             {
//                 possible_procpars.extend(files)
//             }
//         });
//         let last_fid = possible_fids
//             .iter()
//             .max_by_key(|f| f.metadata().unwrap().modified().unwrap())
//             .expect("no max found!")
//             .to_owned();
//
//         let proc = alternative_procpar.ok_or(RequestError::AgilentError(agilent_fid::AgilentFidError::ProcparNotFound))?.as_ref().to_path_buf();
//         println!("resolved fid: {:?}",last_fid);
//         println!("resolved procpar: {:?}",proc);
//         Ok((last_fid,proc))
//     }
// }
//
// pub fn collect_meta_data_agilent(
//     fid_directory: impl AsRef<Path>,
//     vol_idx:usize,
//     alternative_procpar:Option<impl AsRef<Path>>
// ) -> Result<HashMap<String, String>, RequestError> {
//     println!("alternative procpar: {:?}",alternative_procpar.as_ref().map(|x|x.as_ref()));
//     if let Some(proc) = alternative_procpar.as_ref() {
//         println!("alternative procpar specified as {}",proc.as_ref().display());
//         let p = ProcPar::open(proc.as_ref());
//         let meta = p.meta_data(vol_idx);
//         if meta.is_ok() {
//             println!("successfully extracted meta data from procpar ...");
//         }
//         return meta.map_err(|e|RequestError::AgilentError(e))
//     }
//     else {
//         let (fid_file,proc) = find_agilent_fid(fid_directory, alternative_procpar)?;
//         let fid = AgilentFid::open(&fid_file, proc.clone()).map_err(|_|RequestError::FailedToOpenAgilentFid(fid_file,proc.clone()))?;
//         return fid.meta_data(vol_idx).map_err(|_|RequestError::FailedToGetAgilentMetaData(proc))
//     }
// }
//
// pub fn collect_view_table_agilent(
//     fid_directory: impl AsRef<Path>,
//     cs_mask_path:Option<impl AsRef<Path>>,
//     is_coord_stream:bool,
// ) -> Result<ViewTable, RequestError> {
//
//     if let Some(cs_mask_path) = cs_mask_path {
//         if is_coord_stream {
//             return cs_table::ViewTable::from_file(cs_mask_path).map_err(|_|RequestError::AgilentError(agilent_fid::AgilentFidError::CSTableNotFound))
//         }else {
//             return agilent_fid::cs_mask_to_view_table(cs_mask_path).map_err(|e|RequestError::AgilentError(e))
//         }
//     }
//
//     let fid_file = fid_directory.as_ref().join("fid");
//     let proc = fid_directory.as_ref().join("procpar");
//     let fid = AgilentFid::open(&fid_file, proc.clone()).map_err(|_|RequestError::FailedToOpenAgilentFid(fid_file,proc.clone()))?;
//     fid.view_table().map_err(|e|RequestError::AgilentError(e))
// }