cargo build --release --bin data_server_bruker --target x86_64-unknown-linux-musl
scp target/x86_64-unknown-linux-musl/release/data_server_bruker nmrsu@nemo:/opt/recon-utils