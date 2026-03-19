use clap::Parser;
use object_manager::{decode_request, handle_request_mrs, write_to_stdout, Base64, DataResponse};

#[derive(clap::Parser)]
struct Args {
    /// input file
    base64_request_string: String,
}

fn main() {
    let args = Args::parse();

    match decode_request(args.base64_request_string) {
        Ok(request) => {
            match handle_request_mrs(&request) {
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