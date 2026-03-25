use std::path::PathBuf;
use array_lib::io_cfl::write_cfl;
use clap::Parser;
use headfile::Headfile;
use object_manager::{JsonState, RequestError};
use object_manager::object::{ObjectManager, ObjectManagerConf};
use object_manager::request::RequestType;

#[derive(Debug,Parser)]
struct Args {
    /// path to object manager config file
    object_manager_conf: PathBuf,
    /// object index to copy
    object_index: usize,
    /// request type string
    #[arg(value_enum)]
    request_type: RequestType,
    /// working directory to write data to
    #[clap(short,long)]
    work_dir:Option<PathBuf>,
}

fn main() -> Result<(), RequestError> {

    let args = Args::parse();

    let conf = ObjectManagerConf::from_json_file(args.object_manager_conf);
    let o:ObjectManager = conf.into();

    assert!(args.object_index < o.copy_planner.n_objects(),"object_index out of bounds");

    let work_dir = &o.conf.work_dir;

    match args.request_type {
        RequestType::Raw => {
            let (data,dims) = o.submit_raw_request(args.object_index)?;
            let out_file = work_dir.join(format!("raw_{}",args.object_index));
            write_cfl(out_file,&data,dims);
        },
        RequestType::Metadata => {
            let metadata = o.submit_meta_request(args.object_index)?;
            let h = Headfile::from_hash(&metadata);
            let out_file = work_dir.join(format!("meta_{}",args.object_index));
            h.to_file(out_file)?;
        },
        RequestType::Trajectory => {
            let (data,dims) = o.submit_traj_request(args.object_index)?;
            let out_file = work_dir.join(format!("traj_{}",args.object_index));
            write_cfl(out_file,&data,dims);
        }
    }

    Ok(())

}