use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum FsyncMode {
    Always,
    #[default]
    EverySec,
    No,
}

impl FromStr for FsyncMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "always" => Ok(Self::Always),
            "everysec" => Ok(Self::EverySec),
            "no" => Ok(Self::No),
            other => Err(anyhow!("Invalid fsync mode: {other}")),
        }
    }
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
    pub config_path: PathBuf,
    pub data_dir: PathBuf,
}

const CONFIG_HEADER: &str = "\
# YARS configuration file\n\
";

fn default_append_only() -> bool {
    true
}

fn default_aof_filename() -> String {
    String::from("data.aof")
}

#[derive(Deserialize, Serialize)]
struct TomlConfig {
    #[serde(default = "default_append_only")]
    append_only: bool,
    #[serde(default = "default_aof_filename")]
    append_filename: String,
    #[serde(default)]
    fsync_mode: FsyncMode,
}

impl Default for TomlConfig {
    fn default() -> Self {
        Self {
            append_only: default_append_only(),
            append_filename: default_aof_filename(),
            fsync_mode: FsyncMode::default(),
        }
    }
}

fn commented_defaults() -> String {
    let defaults = TomlConfig::default();
    let toml_str = toml_edit::ser::to_string_pretty(&defaults).unwrap_or_default();
    toml_str
        .lines()
        .map(|line| format!("# {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let config_dir = dirs::config_dir().unwrap_or_else(|| PathBuf::from("./config"));
        let yars_config_dir = config_dir.join("yars");
        let data_dir = dirs::data_dir().unwrap_or_else(|| PathBuf::from("./data"));
        let yars_data_dir = data_dir.join("yars");

        let config_path = std::env::var("YARS_CONFIG_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| yars_config_dir.join("yars.toml"));

        let file_vals: TomlConfig = if config_path.exists() {
            let raw = std::fs::read_to_string(&config_path)?;
            toml_edit::de::from_str(&raw)?
        } else {
            if let Some(parent) = config_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(
                &config_path,
                format!("{CONFIG_HEADER}\n{}", commented_defaults()),
            )?;
            TomlConfig::default()
        };

        let mut append_only = file_vals.append_only;
        let mut aof_filename = file_vals.append_filename;
        let mut fsync_mode = file_vals.fsync_mode;

        if let Ok(v) = std::env::var("YARS_APPEND_ONLY") {
            append_only = v.parse().unwrap_or(append_only);
        }
        if let Ok(v) = std::env::var("YARS_AOF_FILENAME") {
            aof_filename = v;
        }
        if let Ok(v) = std::env::var("YARS_AOF_FSYNC")
            && let Ok(mode) = FsyncMode::from_str(&v)
        {
            fsync_mode = mode;
        }

        let aof_path = yars_data_dir.join(&aof_filename);

        Ok(Self {
            append_only,
            aof_path,
            fsync_mode,
            config_path,
            data_dir: yars_data_dir,
        })
    }

    pub fn write_to_file(&self) -> Result<()> {
        let append_filename = self
            .aof_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("data.aof")
            .to_string();

        let content = if self.config_path.exists() {
            let raw = std::fs::read_to_string(&self.config_path)?;
            let mut doc: toml_edit::DocumentMut = raw.parse()?;
            if doc.as_table().is_empty() {
                Self::build_fresh(&append_filename, self.append_only, self.fsync_mode)
            } else {
                if self.append_only != default_append_only() || doc.contains_key("append_only") {
                    doc["append_only"] = toml_edit::value(self.append_only);
                }
                if append_filename != default_aof_filename() || doc.contains_key("append_filename")
                {
                    doc["append_filename"] = toml_edit::value(&append_filename);
                }
                if self.fsync_mode != FsyncMode::default() || doc.contains_key("fsync_mode") {
                    doc["fsync_mode"] = toml_edit::value(self.fsync_mode.as_str());
                }
                doc.to_string()
            }
        } else {
            Self::build_fresh(&append_filename, self.append_only, self.fsync_mode)
        };

        if let Some(parent) = self.config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let tmp_path = self.config_path.with_extension("toml.tmp");
        std::fs::write(&tmp_path, &content)?;
        std::fs::rename(&tmp_path, &self.config_path)?;

        Ok(())
    }

    fn build_fresh(aof_filename: &str, append_only: bool, fsync_mode: FsyncMode) -> String {
        let mut active = String::new();
        if append_only != default_append_only() {
            active.push_str(&format!("append_only = {}\n", append_only));
        }
        if aof_filename != default_aof_filename() {
            active.push_str(&format!("append_filename = \"{}\"\n", aof_filename));
        }
        if fsync_mode != FsyncMode::default() {
            active.push_str(&format!("fsync_mode = \"{}\"\n", fsync_mode.as_str()));
        }
        format!("{CONFIG_HEADER}\n{}\n{active}", commented_defaults())
    }

    pub fn set_fsync_mode(&mut self, fsync: &str) -> Result<()> {
        self.fsync_mode = FsyncMode::from_str(fsync)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fsync_mode_from_str_valid() {
        assert_eq!(FsyncMode::from_str("always").unwrap(), FsyncMode::Always);
        assert_eq!(
            FsyncMode::from_str("everysec").unwrap(),
            FsyncMode::EverySec
        );
        assert_eq!(FsyncMode::from_str("no").unwrap(), FsyncMode::No);
    }

    #[test]
    fn fsync_mode_from_str_case_insensitive_and_trimmed() {
        assert_eq!(
            FsyncMode::from_str("  ALWAYS  ").unwrap(),
            FsyncMode::Always
        );
        assert_eq!(
            FsyncMode::from_str("EverySec").unwrap(),
            FsyncMode::EverySec
        );
        assert_eq!(FsyncMode::from_str("NO").unwrap(), FsyncMode::No);
    }

    #[test]
    fn fsync_mode_from_str_invalid() {
        assert!(FsyncMode::from_str("sometimes").is_err());
    }

    #[test]
    fn fsync_mode_as_str_round_trip() {
        assert_eq!(FsyncMode::Always.as_str(), "always");
        assert_eq!(FsyncMode::EverySec.as_str(), "everysec");
        assert_eq!(FsyncMode::No.as_str(), "no");
    }

    #[test]
    fn commented_defaults_contains_expected_keys() {
        let defs = commented_defaults();
        assert!(defs.contains("append_only"));
        assert!(defs.contains("append_filename"));
        assert!(defs.contains("fsync_mode"));
        for line in defs.lines() {
            assert!(line.starts_with("# "));
        }
    }

    #[test]
    fn set_fsync_mode_valid() {
        let mut cfg = AppConfig {
            append_only: true,
            aof_path: PathBuf::from("/tmp/a.aof"),
            fsync_mode: FsyncMode::Always,
            config_path: PathBuf::from("/tmp/c.toml"),
            data_dir: PathBuf::from("/tmp"),
        };
        cfg.set_fsync_mode("no").unwrap();
        assert_eq!(cfg.fsync_mode, FsyncMode::No);
    }

    #[test]
    fn set_fsync_mode_invalid() {
        let mut cfg = AppConfig {
            append_only: true,
            aof_path: PathBuf::from("/tmp/a.aof"),
            fsync_mode: FsyncMode::Always,
            config_path: PathBuf::from("/tmp/c.toml"),
            data_dir: PathBuf::from("/tmp"),
        };
        assert!(cfg.set_fsync_mode("invalid").is_err());
    }

    #[test]
    fn build_fresh_with_defaults_is_mostly_empty() {
        let s = AppConfig::build_fresh("data.aof", true, FsyncMode::EverySec);
        assert!(s.contains("# YARS configuration file"));
        let lines: Vec<&str> = s.lines().collect();
        let uncommented: Vec<&str> = lines
            .iter()
            .filter(|l| !l.starts_with("#"))
            .copied()
            .collect();
        assert!(uncommented.is_empty() || uncommented.iter().all(|l| l.trim().is_empty()));
    }

    #[test]
    fn build_fresh_with_non_defaults_emits_them() {
        let s = AppConfig::build_fresh("custom.aof", false, FsyncMode::No);
        assert!(s.contains("append_only = false\n"));
        assert!(s.contains("append_filename = \"custom.aof\"\n"));
        assert!(s.contains("fsync_mode = \"no\"\n"));
    }

    #[test]
    fn write_to_file_creates_and_updates() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("yars.toml");
        let cfg = AppConfig {
            append_only: false,
            aof_path: dir.path().join("data.aof"),
            fsync_mode: FsyncMode::No,
            config_path: config_path.clone(),
            data_dir: dir.path().to_path_buf(),
        };
        cfg.write_to_file().unwrap();
        assert!(config_path.exists());
        let contents = std::fs::read_to_string(&config_path).unwrap();
        assert!(contents.contains("append_only = false"));
    }
}
