use super::request::{fill_request, RequestType};
use crate::config::config::{JsonState, TomlConfig};
use crate::data_server::pipeline_component::{PipelineComponent, PipelineState};
use crate::data_server::recon_data::ReconData;
use crate::recon_error::{ReconError, ResourceError};
use crate::resource_manager::scanner::Scanner;
use civm_rust_utils::{m_number, m_number_formatter};
use cs_table::ViewTable;
use headfile::headfile::Headfile;
use mr_data::dim_order::{DimIndex, DimOrder};
use mr_data::raw::Raw;
use serde::{Deserialize, Serialize};
use tracing::info;
use std::collections::{HashMap, HashSet};
use std::fs::create_dir;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use super::request::RequestInfo;
pub const DEBUG: bool = false;
pub const FILENAME: &str = "resource-manager";
pub const DIRNAME: &str = FILENAME;
const MAX_XFER_RETRIES: usize = 10;
const TOTAL_XFER_TIMEOUT_SEC: usize = 60 * 60;

/// describes the names of sub-directories that contain raw data for the data set. "Range" is a
/// simple enumeration of sub-dirs specified by a start and end. "Labeled" is a custom list of
/// sub-dir names, and MNumbers is similar to Range, but will prefix the numbers with an "m" and
/// and perform zero-padding.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum BaseDirExt {
    /// specify the starting and ending extensions (inclusive)
    Range {
        start: usize,
        end: usize,
    },
    Labeled {
        labels: Vec<String>,
    },
    MNumbers {
        n: usize,
    },
}

impl ToString for BaseDirExt {
    fn to_string(&self) -> String {
        match &self {
            BaseDirExt::Range { start, end } => format!("range:{}:{}", start, end),
            BaseDirExt::Labeled { labels } => format!("labels: {:?}", labels),
            BaseDirExt::MNumbers { n } => {
                if *n > 1 {
                    format!("{}:{}", m_number(0, *n), m_number(n - 1, *n))
                } else {
                    m_number(0, *n)
                }
            }
        }
    }
}

impl BaseDirExt {
    pub fn to_vec(&self) -> Vec<String> {
        match self {
            BaseDirExt::Range { start, end } => {
                (*start..(*end + 1)).map(|x| x.to_string()).collect()
            }
            BaseDirExt::Labeled { labels } => labels.to_owned(),
            BaseDirExt::MNumbers { n } => m_number_formatter(*n),
        }
    }
}

/// describes the file layout for data set. No FileLayout indicates that all the data for the resource
/// resides in a single file. "OneToOne" indicates that there is one file for every n "BaseDirExt"s,
/// and "Custom" allows for an arbitrary layout of files for the "BaseDirExt"s.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum FileLayout {
    OneToOne { n: usize },
    Custom { layout: Vec<usize> },
}

impl FileLayout {
    pub fn to_vec(&self) -> Vec<usize> {
        match self {
            FileLayout::OneToOne { n } => vec![1; *n],
            FileLayout::Custom { layout } => layout.to_owned(),
        }
    }
}

impl ToString for FileLayout {
    fn to_string(&self) -> String {
        match &self {
            FileLayout::OneToOne { n } => {
                format!("one-to-one: {}", n)
            }
            FileLayout::Custom { layout } => {
                format!("custom layout: {:?}", layout)
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ResourceManager {
    working_dir: PathBuf,
    resource_base_dir: PathBuf,
    max_xfer_retries: usize,
    total_xfer_timeout_sec: usize,
    base_dir_ext: Option<BaseDirExt>,
    file_layout: Option<FileLayout>,
    pub data_set_layout: DimOrder,
    scanner: Scanner,
    /// this is a hard-set path to the view table on the scanner
    view_table_path: Option<PathBuf>,
    meta_data_path: Option<PathBuf>,
}

//impl TomlConfig for ResourceManager {}

#[derive(Serialize, Deserialize, Clone)]
pub struct ResourceManagerState {
    cached_items: HashSet<PathBuf>,
    is_complete: bool,
    completion_time: Option<SystemTime>,
    start_time: Option<SystemTime>,
}

impl PipelineState for ResourceManagerState {
    fn complete_flag_mut(&mut self) -> &mut bool {
        &mut self.is_complete
    }
    
    fn completion_time_mut(&mut self) -> &mut Option<std::time::SystemTime> {
        &mut self.completion_time
    }
    
    fn start_time_mut(&mut self) -> &mut Option<SystemTime> {
        &mut self.start_time
    }
}

impl JsonState for ResourceManagerState {}
impl TomlConfig for ResourceManager {}

impl Default for ResourceManagerState {
    fn default() -> Self {
        Self {
            cached_items: HashSet::<PathBuf>::new(),
            is_complete: false,
            completion_time: None,
            start_time: None,
        }
    }
}

impl PipelineComponent for ResourceManager {
    type State = ResourceManagerState;

    fn working_directory(&self) -> &Path {
        &self.working_dir
    }

    fn label(&self) -> &str {
        FILENAME
    }
}

impl ResourceManager {
    pub fn new<P1: AsRef<Path>, P2: AsRef<Path>>(
        work_dir: P1,
        resource_base_dir: P2,
        data_set_layout: &DimOrder,
        scanner: Scanner,
    ) -> Self {
        if !work_dir.as_ref().exists() {
            create_dir(&work_dir)
                .unwrap_or_else(|_| panic!("failed to create directory {:?}", work_dir.as_ref()));
        }

        Self {
            working_dir: work_dir.as_ref().to_path_buf(),
            resource_base_dir: resource_base_dir.as_ref().to_path_buf(),
            base_dir_ext: None,
            file_layout: None,
            data_set_layout: data_set_layout.to_owned(),
            scanner,
            max_xfer_retries: MAX_XFER_RETRIES,
            total_xfer_timeout_sec: TOTAL_XFER_TIMEOUT_SEC,
            view_table_path: None,
            meta_data_path: None,
        }
    }

    pub fn raw_data(&self, dim_indices: &[DimIndex]) -> Result<Raw, ReconError> {
        let mut state = self.load_state().map_err(ReconError::Config)?;
        let expected_file = self
            .working_directory()
            .join(Self::raw_data_name(dim_indices));

        if !state.cached_items.contains(&expected_file) {
            info!("raw data not cached: {}",expected_file.to_string_lossy());
            self.fill_request(RequestType::RawData{meta_data_path: self.meta_data_path.clone()}, dim_indices, self.working_directory())
                .map_err(ReconError::Resource)?;
            state.cached_items.insert(expected_file.clone());
            self.save_state(&state).map_err(ReconError::Config)?;
        }
        Raw::from_file(&expected_file)
            .map_err(|_| ReconError::Resource(ResourceError::FailedToLoad(expected_file)))
    }

    pub fn view_table(&self, dim_indices: &[DimIndex]) -> Result<ViewTable, ReconError> {
        let mut state = self.load_state().map_err(ReconError::Config)?;
        let expected_file = self
            .working_directory()
            .join(Self::view_table_name(dim_indices));
        if !state.cached_items.contains(&expected_file) {
            info!("view table not cached: {}",expected_file.to_string_lossy());
            self.fill_request(
                RequestType::ViewTable{ table_path: self.view_table_path.clone() },
                dim_indices,
                self.working_directory(),
            )?;
            state.cached_items.insert(expected_file.clone());
            self.save_state(&state).map_err(ReconError::Config)?;
        }
        ViewTable::from_file(&expected_file)
            .map_err(|_| ReconError::Resource(ResourceError::FailedToLoad(expected_file)))
    }

    pub fn meta_data(
        &self,
        dim_indices: &[DimIndex],
    ) -> Result<HashMap<String, String>, ReconError> {
        let mut state = self.load_state().map_err(ReconError::Config)?;
        let expected_file = self
            .working_directory()
            .join(Self::meta_data_name(dim_indices));
        if !state.cached_items.contains(&expected_file) {
            info!("meta data not cached: {}",expected_file.to_string_lossy());
            self.fill_request(RequestType::MetaData{meta_data_path: self.meta_data_path.clone()}, dim_indices, self.working_directory())
                .map_err(ReconError::Resource)?;
            state.cached_items.insert(expected_file.clone());
            self.save_state(&state).map_err(ReconError::Config)?;
        }
        Ok(Headfile::open(&expected_file).to_hash())
    }

    pub fn scanner(&self) -> String {
        match self.scanner {
            Scanner::Bruker { .. } => "bruker".to_string(),
            Scanner::MrSolutions { .. } => "mrsolutions".to_string(),
            Scanner::Agilent { .. } => "agilent".to_string(),
        }
    }

    pub fn with_dir_extensions(mut self, base_dir_ext: BaseDirExt) -> Self {
        self.with_dir_extensions_mut(base_dir_ext);
        self
    }

    pub fn with_dir_extensions_mut(&mut self, base_dir_ext: BaseDirExt) {
        self.base_dir_ext = Some(base_dir_ext);
    }

    pub fn with_file_layout_mut(&mut self, file_layout: FileLayout) {
        self.file_layout = Some(file_layout);
    }

    pub fn with_file_layout(mut self, file_layout: FileLayout) -> Self {
        self.with_file_layout_mut(file_layout);
        self
    }

    pub fn with_view_table_path_mut(&mut self, view_table:impl AsRef<Path>) {
        self.view_table_path = Some(view_table.as_ref().to_owned());
    }

    pub fn with_meta_data_path_mut(&mut self, meta_data:impl AsRef<Path>) {
        self.meta_data_path = Some(meta_data.as_ref().to_path_buf())
    }

    pub fn fill_request<P: AsRef<Path>>(
        &self,
        request_type: RequestType,
        dimension_indices: &[DimIndex],
        directory: P,
    ) -> Result<(), ResourceError> {

        let (_,&last_size) = self.data_set_layout.last();

        let req_info = RequestInfo {
            request_type:request_type.clone(),
            resource_base_dir: self.resource_base_dir.clone(),
            base_dir_ext: self
                .base_dir_ext
                .clone()
                .map(|x| x.to_vec())
                .unwrap_or_default(),
            file_layout: self
                .file_layout
                .clone()
                .map(|x| x.to_vec())
                .unwrap_or(vec![last_size]),
            scanner: self.scanner.clone(),
            data_set_layout: self.data_set_layout.clone(),
            dim_indices: dimension_indices.to_vec(),
            max_xfer_retries: 1,
            total_xfer_timeout_sec: 3 * 60,
        };

        if !directory.as_ref().exists() {
            create_dir(&directory).expect("failed to create directory");
        }

        info!("sending a request");

        let resp = fill_request(req_info).map_err(|e| ResourceError::DataRequest(e))?;

        match request_type {
            RequestType::RawData{..} => {
                let raw: Raw = resp.try_into().map_err(|e| ResourceError::DataRequest(e))?;
                let filename = directory
                    .as_ref()
                    .join(Self::raw_data_name(&dimension_indices));
                raw.write_to_file(&filename)
                    .map_err(|_| ResourceError::FailedToWrite(filename))?
            }
            RequestType::ViewTable{..} => {
                let view_table: ViewTable =
                    resp.try_into().map_err(|e| ResourceError::DataRequest(e))?;
                let filename = directory
                    .as_ref()
                    .join(Self::view_table_name(&dimension_indices));
                view_table
                    .write_to_file(&filename)
                    .map_err(|_| ResourceError::FailedToWrite(filename))?
            }
            RequestType::MetaData{..} => {
                let meta: HashMap<String, String> =
                    resp.try_into().map_err(|e| ResourceError::DataRequest(e))?;
                info!("recieved meta: {:#?}",meta);
                let filename = directory
                    .as_ref()
                    .join(Self::meta_data_name(&dimension_indices));
                meta.write_to_file(&filename)
                    .map_err(|_| ResourceError::FailedToWrite(filename))?
            }
        }
        Ok(())
    }

    pub fn data_set_layout(&self) -> &DimOrder {
        &self.data_set_layout
    }

    fn raw_data_name(dim_indices: &[DimIndex]) -> String {
        DimIndex::list_to_string(dim_indices)
    }

    fn view_table_name(dim_indices: &[DimIndex]) -> String {
        format!("{}-cs_table", Self::raw_data_name(dim_indices))
    }

    fn meta_data_name(dim_indices: &[DimIndex]) -> String {
        format!("{}-meta.txt", Self::raw_data_name(dim_indices))
    }
}


#[cfg(test)]
mod tests {
    use fft::ifftnc;
    use image_utils::nifti_dump_magnitude;
    use mr_data::dim_order::{DimIndex, DimOrder};
    use crate::resource_manager::{resource_manager::{BaseDirExt, FileLayout, ResourceManager}, scanner::Scanner};

    //cargo test --release --package recon2 --lib -- resource_manager::resource_manager::tests::test --exact --nocapture
    #[test]
    fn test() {
        let re = ResourceManager::new(
            "/privateShares/wa41/resman_test",
            "D:/dev/studies/N20240726_02/5xfad_se",
            &DimOrder::new(&[788, 28800, 67], &["samples", "views", "experiments"]).unwrap(),
            Scanner::default_mrsolutions(),
        ).with_dir_extensions(BaseDirExt::MNumbers { n: 67 })
        .with_file_layout(FileLayout::OneToOne { n: 67 });

        println!("fetching raw data");
        let raw = re.raw_data(&[DimIndex::Experiment(0)]).unwrap();
        println!("view table");
        let vt = re.view_table(&[DimIndex::Experiment(0)]).unwrap();

        println!("converting kspace");
        let ksp = raw.to_kspace(&vt.coordinate_pairs::<i32>().unwrap(), true).unwrap();
        let mut arr = ksp.grid([788,480,480].as_slice()).into_dyn();
        ifftnc(&mut arr);
        
        println!("writing out ...");
        nifti_dump_magnitude(&arr, "/privateShares/wa41/resman_test/test_nii");
        println!("done.");

    }

}





