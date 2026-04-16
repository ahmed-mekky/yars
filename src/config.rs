use std::path::PathBuf;

use anyhow::{Result, anyhow};

#[derive(Clone, Copy, Debug)]
pub enum FsyncMode {
    Always,
    EverySec,
    No,
}

impl FsyncMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::EverySec => "everysec",
            Self::No => "no",
        }
    }
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub append_only: bool,
    pub aof_path: PathBuf,
    pub fsync_mode: FsyncMode,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let default_path = dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("./data"))
            .join("yars/data.aof");
        let aof_path = std::env::var("YARS_AOF_PATH")
            .map(|s| parse_path(PathBuf::from(&s)))
            .unwrap_or(default_path);
        let fsync_mode = match std::env::var("YARS_AOF_FSYNC")
            .unwrap_or_else(|_| "everysec".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "always" => FsyncMode::Always,
            "everysec" => FsyncMode::EverySec,
            "no" => FsyncMode::No,
            other => return Err(anyhow!("Invalid YARS_AOF_FSYNC value: {other}")),
        };
        let append_only = std::env::var("YARS_APPEND_ONLY")
            .unwrap_or_else(|_| "true".to_string())
            .parse()?;

        Ok(Self {
            append_only,
            aof_path,
            fsync_mode,
        })
    }

    pub fn set_fsync_mode(&mut self, fsync: &str) -> Result<()> {
        self.fsync_mode = match fsync.trim().to_ascii_lowercase().as_str() {
            "always" => FsyncMode::Always,
            "everysec" => FsyncMode::EverySec,
            "no" => FsyncMode::No,
            _ => return Err(anyhow!("Invalid fsync mode: {fsync}")),
        };
        Ok(())
    }
}

fn parse_path(path: PathBuf) -> PathBuf {
    let s = path.to_str().unwrap_or("");
    if let Some(rest) = s.strip_prefix('~')
        && let Some(home) = dirs::home_dir()
    {
        return PathBuf::from(format!("{}{}", home.to_str().unwrap(), rest));
    }
    path
}
