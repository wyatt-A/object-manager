cargo build --release --bin data_server_mrs --target x86_64-pc-windows-gnu
scp target/x86_64-pc-windows-gnu/release/data_server_mrs.exe stejskal:/c/workstation/bin