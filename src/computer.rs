use serde::{Deserialize, Serialize};
use ssh2::Session;
use walkdir::WalkDir;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::{create_dir_all, remove_dir_all};
use std::io::Read;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use std::{fs, io};
use wait_timeout::ChildExt;

const SCP_WAIT_TIMEOUT_SEC: u64 = 3 * 60;
pub const MAX_RETRIES: usize = 3;

/*
   A host where recon data is found
*/
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Computer {
    // if hostname is none, then it's assumed to be the local host
    hostname: Option<String>,
    // if the user is none, then it is assumed that the host is local or ssh config allows ssh connection
    user: Option<String>,
}

impl Display for Computer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}@{}",
            self.user.as_deref().unwrap_or(""),
            self.hostname.as_deref().unwrap_or("")
        )
    }
}

impl Computer {
    pub fn set_user(&mut self, username: &str) {
        self.user = Some(username.to_string())
    }
    pub fn set_host(&mut self, hostname: &str) {
        self.hostname = Some(hostname.to_string())
    }

    pub fn new_local() -> Self {
        Self {
            hostname: None,
            user: None,
        }
    }
    pub fn new_remote(hostname: &str, user: Option<&str>) -> Self {
        let user = user.map(|user| user.to_string());
        Self {
            hostname: Some(hostname.to_string()),
            user,
        }
    }
}

impl Computer {
    /// return the host name of the computer if it has one
    pub fn hostname(&self) -> Option<String> {
        self.hostname.clone()
    }

    /// return the user of the computer if it has one
    pub fn user(&self) -> Option<String> {
        self.user.clone()
    }

    /// return the home director of the computer
    pub fn home_dir(&self) -> PathBuf {
        if self.is_local() {
            dirs::home_dir().expect("Could not find home directory")
        } else {
            let hostname = self.hostname().unwrap();
            let mut cmd = if let Some(user) = self.user() {
                let mut cmd = Command::new("ssh");
                cmd.arg(format!("{}@{}", user, hostname));
                cmd.arg("echo ~");
                cmd
            } else {
                let mut cmd = Command::new("ssh");
                cmd.arg(&hostname);
                cmd.arg("echo ~");
                cmd
            };
            let o = cmd.output().expect("failed to launch ssh");
            if o.status.success() {
                let resp = String::from_utf8_lossy(&o.stdout).to_string();
                return PathBuf::from(resp.trim());
            } else {
                panic!("cannot get home dir from {}", hostname);
            }
        }
    }

    /// run a command and gather standard out
    pub fn run_command<T: AsRef<Path>, S: AsRef<Path>>(
        &self,
        exec: T,
        args: Vec<S>,
        debug: bool,
    ) -> Result<String, Box<dyn Error>> {
        if self.is_local() {
            let mut command = Command::new(exec.as_ref());
            command.args(args.iter().map(|t| t.as_ref()));
            if debug {
                println!("running: {:?}", command);
            }
            let output = command.output()?;
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            let mut ssh_command = Command::new("ssh");
            ssh_command.arg(self.host_arg().ok_or("host name must be specified")?);
            ssh_command.arg(exec.as_ref());
            ssh_command.args(args.iter().map(|t| t.as_ref()));
            if debug {
                println!("running: {:?}", ssh_command);
            }
            let output = ssh_command.output()?;
            if !output.status.success() {
                let std_err = String::from_utf8_lossy(&output.stderr);
                println!("process didn't return successfully: {}", std_err);
            }
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        }
    }

    pub fn run_cmd2<S:AsRef<str>>(&self, cmd:S, args: &[S]) -> Result<String, Box<dyn Error>> {

        let username = self.user().unwrap_or(whoami::username().expect("could not get username"));
        let hostname = self.hostname().unwrap_or("localhost".to_string());
    
        let tcp = TcpStream::connect(format!("{}:22", hostname)).unwrap();
        let mut sess = Session::new().unwrap();
        sess.set_tcp_stream(tcp);
        sess.handshake().unwrap();
    
        // get public keys
        let ssh_dir = dirs::home_dir().expect("could not find home dir").join(".ssh");
    
        let mut pub_keys = vec![];
        for entry in WalkDir::new(ssh_dir)
            .max_depth(1)
            .into_iter()
            .filter_map(Result::ok)
        {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("pub") {
                pub_keys.push(path.to_owned())
            }
        }
    
        // find valid public/private key to authenticate
        if !pub_keys
            .iter()
            .find_map(|key| {
                sess.userauth_pubkey_file(&username, None, &key.with_extension(""), None)
                    .ok()
                    .and_then(|_| if sess.authenticated() { Some(()) } else { None })
            })
            .is_some()
        {
            return Err(format!("ssh auth error: {}@{}",username,hostname).into())
        }
        let mut channel = sess.channel_session().unwrap();
        let mut cmd = vec![cmd.as_ref()];
    
        cmd.extend(args.iter().map(|s|s.as_ref()));
        let cmd = cmd.join(" ");

        channel.exec(&cmd).unwrap();
        let mut s = String::new();
        channel.read_to_string(&mut s).unwrap();
        Ok(s)
    }

    /// check if the computer is this one
    pub fn is_local(&self) -> bool {
        let computer_name = whoami::hostname().expect("could not get hostname");
        if self.hostname().is_none() {
            true
        } else {
            computer_name.as_str() == self.hostname().unwrap().as_str()
        }
    }

    /// test the connection to the computer
    pub fn test_connection(&self) -> bool {
        if self.hostname().is_none() {
            println!("hostname not specified ... assuming local");
            true
        } else {
            let hostname = self.hostname().unwrap();
            // build ssh connection command with or without user specified
            let mut cmd = if let Some(user) = self.user() {
                println!("testing connection for user {} on {}", user, hostname);
                let mut cmd = Command::new("ssh");
                cmd.arg("-o BatchMode=yes");
                cmd.arg(format!("{}@{}", user, hostname));
                cmd.arg("exit");
                cmd
            } else {
                println!("testing connection to {}", hostname);
                let mut cmd = Command::new("ssh");
                cmd.arg("-o BatchMode=yes");
                cmd.arg(&hostname);
                cmd.arg("exit");
                cmd
            };
            match cmd.output().expect("failed to launch ssh").status.success() {
                true => {
                    println!("connection successful");
                    true
                }
                false => {
                    if let Some(user) = self.user() {
                        println!(
                            "password-less connection failed for {} on {}.",
                            user, hostname
                        );
                        println!(
                            "try to run ssh-copy-id for {} on {} to fix the connection",
                            user, hostname
                        );
                    } else {
                        println!("password-less connection to {} failed.", hostname);
                        println!(
                            "try to run ssh-copy-id for {} to fix the connection",
                            hostname
                        );
                    }
                    false
                }
            }
        }
    }

    /// check if the directory exists on the computer
    pub fn dir_exists(&self, dir: &Path) -> bool {
        if self.hostname().is_none() {
            dir.is_dir()
        } else {
            let hostname = self.hostname().unwrap();
            let mut cmd = if let Some(user) = self.user() {
                let mut cmd = Command::new("ssh");
                cmd.arg(format!("{}@{}", user, hostname));
                cmd.arg(format!("[ -d \"{}\" ]", dir.to_string_lossy()));
                cmd
            } else {
                let mut cmd = Command::new("ssh");
                cmd.arg(&hostname);
                cmd.arg(format!("[ -d \"{}\" ]", dir.to_string_lossy()));
                cmd
            };
            let o = cmd.output().expect("failed to launch ssh");
            o.status.success()
        }
    }

    /// return the host section of an scp call
    pub fn host_arg(&self) -> Option<String> {
        if !self.is_local() {
            let hostname = self.hostname().unwrap();
            if let Some(user) = self.user() {
                Some(format!("{}@{}", user, hostname))
            } else {
                Some(hostname.to_string())
            }
        } else {
            None
        }
    }

    pub fn copy_dir_persistent<P1: AsRef<Path>, P2: AsRef<Path>>(
        &self,
        dir_to_copy: P1,
        destination: P2,
        max_tries: usize,
        timeout_sec: u64,
    ) -> bool {
        let mut count = 0;
        while !self.copy_dir(
            dir_to_copy.as_ref(),
            destination.as_ref(),
            timeout_sec,
            true,
        ) {
            count += 1;
            if count == max_tries {
                return false;
            }
        }
        true
    }

    /// copy data via scp or cp
    pub fn copy_dir(
        &self,
        dir_to_copy: &Path,
        destination: &Path,
        timeout_sec: u64,
        debug: bool,
    ) -> bool {
        if self.is_local() {
            if let Ok(_) = copy_recursively(dir_to_copy, destination) {
                true
            } else {
                if let Err(_) = fs::remove_dir_all(destination) {
                    println!("failed to clean up destination directory");
                }
                false
            }
        } else {
            // check if remote source exists ...
            if !self.dir_exists(dir_to_copy) {
                return false;
            }
            // check that the local destination doesn't exist
            if !destination.exists() {
                if let Err(_) = create_dir_all(destination) {
                    return false;
                }
            }

            let host_arg = self.host_arg().expect("there to be an arg");
            let source_arg = format!("{}:{}/*", host_arg, dir_to_copy.to_string_lossy());
            let dest_arg = destination.to_string_lossy().to_string();
            let mut scp_command = Command::new("scp");
            scp_command.args(vec![source_arg.as_str(), dest_arg.as_str()]);
            if debug {
                println!("running:{:?}", scp_command);
            }
            let success = run_with_timeout(&mut scp_command, timeout_sec, true);
            // clean up destination
            if !success {
                if let Err(_) = fs::remove_dir_all(destination) {
                    panic!("unable to clean up destination dir");
                }
            };
            success
        }
    }

    /// remove a directory with rm -r
    pub fn rm_dir<P: AsRef<Path>>(&self, host_dir: P) -> bool {
        if self.is_local() {
            remove_dir_all(host_dir).is_ok()
        } else {
            let host_arg = self.host_arg().expect("there to be an arg");
            let dest_arg = host_dir.as_ref().to_string_lossy().to_string();
            let mut ssh_command = Command::new("ssh");
            ssh_command.args(vec![host_arg.as_str(), "rm", "-rf", dest_arg.as_str()]);
            run_with_timeout(
                &mut ssh_command,
                SCP_WAIT_TIMEOUT_SEC,
                true,
            )
        }
    }

    /// push a directory to the computer via scp -r or cp -r
    pub fn push_dir(&self, destination: &Path, dir_to_copy: &Path) -> bool {
        if self.is_local() {
            self.copy_dir(
                dir_to_copy,
                destination,
                SCP_WAIT_TIMEOUT_SEC,
                true,
            )
        } else {
            let dest_dir = destination.to_string_lossy().to_string();
            let host_arg = self.host_arg().expect("there to be a hostname");
            let dest_arg = format!("{}:{}", host_arg, dest_dir);
            let source_arg = dir_to_copy.to_string_lossy().to_string();
            // create dest dir on remote system
            let mut ssh_command = Command::new("ssh");
            ssh_command.args(vec![host_arg.as_str(), "mkdir", "-p", dest_dir.as_str()]);
            if !run_with_timeout(
                &mut ssh_command,
                SCP_WAIT_TIMEOUT_SEC,
                true,
            ) {
                return false;
            }
            let mut scp_command = Command::new("scp");
            scp_command.args(vec!["-r", source_arg.as_str(), dest_arg.as_str()]);
            if !run_with_timeout(
                &mut scp_command,
                SCP_WAIT_TIMEOUT_SEC,
                true,
            ) {
                return false;
            }
            true
        }
    }

    /// push a single file via scp or cp
    pub fn push_file(&self, destination: &Path, file_to_copy: &Path) -> bool {
        if self.is_local() {
            if let Err(_) = fs::copy(file_to_copy, destination) {
                return false;
            }
            true
        } else {
            if !self.dir_exists(destination) {
                return false;
            }

            let source_arg = file_to_copy.to_string_lossy().to_string();
            let dest_dir = destination.to_string_lossy().to_string();
            let host_arg = self.host_arg().expect("there to be a host name");
            let dest_arg = format!("{}:{}", host_arg, dest_dir);

            let mut scp_command = Command::new("scp");
            scp_command.args(vec![source_arg.as_str(), dest_arg.as_str()]);

            if !run_with_timeout(
                &mut scp_command,
                SCP_WAIT_TIMEOUT_SEC,
                true,
            ) {
                return false;
            }
            true
        }
    }
}

fn copy_recursively(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> io::Result<()> {
    create_dir_all(&destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let filetype = entry.file_type()?;
        if filetype.is_dir() {
            copy_recursively(entry.path(), destination.as_ref().join(entry.file_name()))?;
        } else {
            fs::copy(entry.path(), destination.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}

fn run_with_timeout(command: &mut Command, timeout_sec: u64, debug: bool) -> bool {
    if debug {
        println!("running: {:?}", command);
    }
    let mut child = command
        .spawn()
        .unwrap_or_else(|_| panic!("failed to launch {:?}", command));
    let scp_wait_dur = Duration::from_secs(timeout_sec);

    if let Some(status) = child
        .wait_timeout(scp_wait_dur)
        .expect("expecting wait-timeout to work")
    {
        status.success()
    } else {
        println!(
            "scp process timed out after {} seconds ... killing process",
            timeout_sec
        );
        child.kill().unwrap();
        let code = child.wait().unwrap().code();
        println!("{:?} exit code: {:?}", command, code);
        false
    }
}
