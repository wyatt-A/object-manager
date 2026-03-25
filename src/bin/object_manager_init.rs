use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use array_lib::DimSize::{PHS1, READ};
use object_manager::{JsonState, TomlConf};
use object_manager::object::{ObjectManagerConf, RawLayout};
use object_manager::scanner::HostProperties;

fn main() {
    let raw_file_pattern = PathBuf::from("results/m*.cfl");
    let meta_file_pattern = PathBuf::from("results/*.headfile");
    let traj_file_pattern = PathBuf::from("results/traj.cfl");

    let obj_layout = [READ(512),PHS1(1892)];

    let raw_layout = RawLayout::BuffArray {
        buffer_layout: vec![READ(512),PHS1(1892)],
        n: 150,
    };

    let c = ObjectManagerConf {
        work_dir: Default::default(),
        remote_dir: PathBuf::from(r"D:/dev/test/26.wang.06/260316_00"),
        max_xfer_retries: 10,
        total_xfer_timeout_sec: 10*60,
        data_host: HostProperties::default_mrsolutions(),
        raw_file_patterns: vec![raw_file_pattern],
        meta_file_patterns: vec![meta_file_pattern],
        single_meta_file: true,
        trajectory_file_patterns: vec![traj_file_pattern],
        single_traj_file: true,
        obj_layout: obj_layout.to_vec(),
        raw_layout,
    };

    c.to_json_file("configs/conf");

}