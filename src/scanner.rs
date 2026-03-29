use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use super::computer::Computer;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HostProperties {
    pub host: Computer,
    pub server_bin: PathBuf,
    pub tesla_image_code: String,
    pub raw_base_directory: Option<PathBuf>,
}

impl HostProperties {
    pub fn new(
        hostname: impl AsRef<str>,
        username: impl AsRef<str>,
        server_bin: impl AsRef<str>,
        tesla_image_code: impl AsRef<str>,
        raw_base_directory: impl AsRef<str>,
    ) -> Self {
        let computer = if hostname.as_ref().is_empty() {
            Computer::new_local()
        } else if username.as_ref().is_empty() {
            Computer::new_remote(hostname.as_ref(), None)
        } else {
            Computer::new_remote(hostname.as_ref(), Some(username.as_ref()))
        };

        let raw_base_directory = if raw_base_directory.as_ref().is_empty() {
            None
        } else {
            Some(PathBuf::from(raw_base_directory.as_ref()))
        };

        Self {
            host: computer,
            server_bin: PathBuf::from(server_bin.as_ref()),
            tesla_image_code: tesla_image_code.as_ref().to_string(),
            raw_base_directory,
        }
    }

    pub fn default_bruker() -> Self {
        Self {
            host: Computer::new_remote("nemo", Some("qa")),
            server_bin: PathBuf::from("/opt/recon-utils/resource-server"),
            tesla_image_code: String::from("bt7"),
            raw_base_directory: None,
        }
    }

    pub fn default_mrsolutions() -> Self {
        Self {
            host: Computer::new_remote("stejskal", Some("mrs")),
            server_bin: PathBuf::from("/c/workstation/bin/resource-server.exe"),
            tesla_image_code: String::from("t9"),
            raw_base_directory: Some(PathBuf::from("D:/dev/studies")),
        }
    }

    pub fn default_agilet() -> Self {
        Self {
            host: Computer::new_remote("lx7-civm", Some("omega")),
            server_bin: PathBuf::from("/home/omega/resource-server"),
            tesla_image_code: String::from("t7"),
            raw_base_directory: Some(PathBuf::from("/mrraw")),
        }
    }

    pub fn scanner(&self) -> Scanner {
        let hostname = self.host.hostname().unwrap_or("localhost".to_string());
        match hostname.as_str() {
            "stejskal" => Scanner::MrSolutions(self.clone()),
            "nemo" => Scanner::Bruker(self.clone()),
            "lx7-civm" => Scanner::Agilent(self.clone()),
            _ => panic!("unsupported host name: {hostname}",)
        }
    }


}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Scanner {
    Bruker(HostProperties),
    MrSolutions(HostProperties),
    Agilent(HostProperties),
}

impl Scanner {
    pub fn properties(&self) -> HostProperties {
        match &self {
            Scanner::Bruker(props) => props.clone(),
            Scanner::MrSolutions(props) => props.clone(),
            Scanner::Agilent(props) => props.clone(),
        }
    }

    /// return the host of the scanner
    pub fn host(&self) -> &Computer {
        match self {
            Scanner::Bruker(props) => &props.host,
            Scanner::MrSolutions(props) => &props.host,
            Scanner::Agilent(props) => &props.host,
        }
    }

    pub fn host_mut(&mut self) -> &mut Computer {
        match self {
            Scanner::Bruker(props) => &mut props.host,
            Scanner::MrSolutions(props) => &mut props.host,
            Scanner::Agilent(props) => &mut props.host,
        }
    }

    pub fn base_dir(&self) -> Option<&PathBuf> {
        match self {
            Scanner::Bruker(props) => props.raw_base_directory.as_ref(),
            Scanner::MrSolutions(props) => props.raw_base_directory.as_ref(),
            Scanner::Agilent(props) => props.raw_base_directory.as_ref(),
        }
    }

    /// return the path to the server binary on that system
    pub fn server_bin(&self) -> &Path {
        match self {
            Scanner::Bruker(props) => &props.server_bin,
            Scanner::MrSolutions(props) => &props.server_bin,
            Scanner::Agilent(props) => &props.server_bin,
        }
    }

    /// return the default scanner config for mrsolutions system
    pub fn default_mrsolutions() -> Self {
        Self::MrSolutions(HostProperties::default_mrsolutions())
    }

    /// return the default scanner config for the bruker system
    pub fn default_bruker() -> Self {
        Self::Bruker(HostProperties::default_bruker())
    }

    /// return the default scanner config for the agilent system
    pub fn default_agilent() -> Self {
        Self::Agilent(HostProperties::default_agilet())
    }

    pub fn local_bruker_data<P: AsRef<Path>>(server_binary: P) -> Self {
        Scanner::Bruker(HostProperties {
            host: Computer::new_local(),
            server_bin: server_binary.as_ref().to_path_buf(),
            tesla_image_code: String::from("bt7"),
            raw_base_directory: None,
        })
    }

    pub fn local_mrs_data<P: AsRef<Path>>(server_binary: P) -> Self {
        Scanner::MrSolutions(HostProperties {
            host: Computer::new_local(),
            server_bin: server_binary.as_ref().to_path_buf(),
            tesla_image_code: String::from("t9"),
            raw_base_directory: None,
        })
    }

    pub fn image_code(&self) -> String {
        match &self {
            Scanner::Bruker(props) => props.tesla_image_code.to_owned(),
            Scanner::MrSolutions(props) => props.tesla_image_code.to_owned(),
            Scanner::Agilent(props) => props.tesla_image_code.to_owned(),
        }
    }

    pub fn with_user(mut self, username: &str) -> Self {
        self.host_mut().set_user(username);
        self
    }

    pub fn with_host(mut self, hostname: &str) -> Self {
        self.host_mut().set_host(hostname);
        self
    }

    pub fn vendor(&self) -> String {
        match &self {
            Scanner::Bruker(_) => "bruker".to_string(),
            Scanner::MrSolutions(_) => "mrsolutions".to_string(),
            Scanner::Agilent(_) => "agilent".to_string(),
        }
    }

}
