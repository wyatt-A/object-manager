use clap::Parser;
use object_manager::{decode_request, write_to_stdout, Base64};
use object_manager::data_collection_bruker::handle_request_bruker;
use object_manager::request::DataResponse;

#[derive(clap::Parser)]
struct Args {
    /// base-64 encoded request string
    base64_request_string: String,
}

fn main() {
    let args = Args::parse();

    match decode_request(args.base64_request_string) {
        Ok(request) => {
            match handle_request_bruker(&request) {
                Ok(response) => {
                    let rs = format!("|||{}|||",response.to_base64());
                    write_to_stdout(rs.as_bytes()).unwrap();
                },
                Err(e) => {
                    let response = DataResponse {
                        raw_payload: None,
                        meta_payload: None,
                        traj_payload: None,
                        req: Some(request.clone()),
                        error: Some(e),
                    };
                    let rs = format!("|||{}|||",response.to_base64());
                    write_to_stdout(rs.as_bytes()).unwrap();
                }
            }
        },
        Err(error) => {
            let response = DataResponse {
                raw_payload: None,
                meta_payload: None,
                traj_payload: None,
                req:None,
                error: Some(error),
            };
            let rs = format!("|||{}|||",response.to_base64());
            write_to_stdout(rs.as_bytes()).unwrap();
        }
    }

}