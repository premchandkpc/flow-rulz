//! Configuration for the standalone broker path, independent of database settings.
use anyhow::{Context, Result, bail};
use std::{collections::HashSet, env, net::IpAddr, path::PathBuf, time::Duration};

#[derive(Clone)]
pub(crate) struct Config {
    pub transports: Vec<String>,
    pub http_addr: String,
    pub tcp_addr: String,
    pub nats_url: String,
    pub nats_subject: String,
    pub token: String,
    pub output: String,
    pub output_url: String,
    pub output_addr: String,
    pub output_nats_url: String,
    pub output_nats_subject: String,
    pub output_token: String,
    pub rules_dir: PathBuf,
    pub storage: String,
    pub data_dir: PathBuf,
    pub partitions: u32,
    pub max_messages: usize,
    pub max_bytes: usize,
    pub ack_wait: Duration,
    pub retry_delay: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name, default| env::var(name).unwrap_or_else(|_| default.into()))
    }

    fn from_lookup(get: impl Fn(&str, &str) -> String) -> Result<Self> {
        let number = |name, default| -> Result<u64> {
            get(name, default)
                .parse()
                .with_context(|| format!("invalid {name}"))
        };
        let nats_url = get("NATS_URL", "nats://127.0.0.1:4222");
        let mut config = Self {
            transports: get("BROKER_TRANSPORTS", "http")
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
            http_addr: get("BROKER_ADDR", "127.0.0.1:8090"),
            tcp_addr: get("BROKER_TCP_ADDR", "127.0.0.1:8091"),
            nats_url: nats_url.clone(),
            nats_subject: get("BROKER_NATS_SUBJECT", "flowrulz.broker"),
            token: get("BROKER_TOKEN", ""),
            output: get("BROKER_OUTPUT_TRANSPORT", "local"),
            output_url: get("BROKER_OUTPUT_URL", ""),
            output_addr: get("BROKER_OUTPUT_ADDR", ""),
            output_nats_url: get("BROKER_OUTPUT_NATS_URL", &nats_url),
            output_nats_subject: get("BROKER_OUTPUT_NATS_SUBJECT", "flowrulz.broker"),
            output_token: get("BROKER_OUTPUT_TOKEN", ""),
            rules_dir: get("WORKER_RULES_DIR", "").into(),
            storage: get("BROKER_STORAGE", "memory"),
            data_dir: get("BROKER_DATA_DIR", "data/rust-broker").into(),
            partitions: u32::try_from(number("NUM_SHARDS", "4096")?)?,
            max_messages: usize::try_from(number("BROKER_MAX_MESSAGES", "10000")?)?,
            max_bytes: usize::try_from(number("BROKER_MAX_BYTES", "67108864")?)?,
            ack_wait: Duration::from_millis(number("BROKER_ACK_WAIT_MS", "30000")?),
            retry_delay: Duration::from_millis(number("EVENT_RETRY_DELAY_MS", "5000")?),
        };
        if config.output_nats_url.is_empty() {
            config.output_nats_url = nats_url;
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.rules_dir.as_os_str().is_empty()
            || !(1..=4096).contains(&self.partitions)
            || !(2..=10000).contains(&self.max_messages)
            || !(1..=64 * 1024 * 1024).contains(&self.max_bytes)
            || !(100..=300000).contains(&self.ack_wait.as_millis())
            || self.retry_delay.is_zero()
        {
            bail!("invalid broker rules directory, partition count, capacity or timeout");
        }
        if !matches!(self.storage.as_str(), "memory" | "file")
            || (self.storage == "file" && self.data_dir.as_os_str().is_empty())
        {
            bail!("BROKER_STORAGE must be memory or file with a data directory");
        }
        let mut seen = HashSet::new();
        for transport in &self.transports {
            if !seen.insert(transport) {
                bail!("duplicate broker transport");
            }
            match transport.as_str() {
                "none" if self.transports.len() == 1 => (),
                "http" | "tcp" => {
                    let host = validate_address(if transport == "http" {
                        &self.http_addr
                    } else {
                        &self.tcp_addr
                    })?;
                    let local = host == "localhost"
                        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback());
                    if !local && self.token.len() < 16 {
                        bail!(
                            "non-loopback listeners require BROKER_TOKEN of at least 16 characters"
                        );
                    }
                }
                "nats" if !self.nats_url.is_empty() && valid_subject(&self.nats_subject) => (),
                _ => bail!("BROKER_TRANSPORTS accepts http,tcp,nats or none"),
            }
        }
        match self.output.as_str() {
            "local" => (),
            "http" => {
                validate_url(&self.output_url)?;
            }
            "tcp" => {
                validate_address(&self.output_addr)?;
            }
            "nats"
                if !self.output_nats_url.is_empty() && valid_subject(&self.output_nats_subject) => {
            }
            _ => bail!("invalid broker output transport or peer address"),
        }
        Ok(())
    }
}

fn validate_address(value: &str) -> Result<&str> {
    let (host, port) = value
        .rsplit_once(':')
        .context("broker address requires host:port")?;
    port.parse::<u16>().context("invalid broker port")?;
    Ok(host.trim_matches(['[', ']']))
}

fn validate_url(value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!(
            "HTTP output requires an absolute HTTP(S) URL without credentials, query or fragment"
        );
    }
    Ok(())
}

fn valid_subject(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .split('.')
            .all(|part| !part.is_empty() && !part.contains(['*', '>', ' ', '\t', '\r', '\n']))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disabled_dependencies_are_not_required() {
        let config = Config::from_lookup(|name, default| match name {
            "WORKER_RULES_DIR" => "rules".into(),
            "BROKER_TRANSPORTS" => "none".into(),
            "NATS_URL" | "DATABASE_URL" | "BROKER_ADDR" | "BROKER_TCP_ADDR" => String::new(),
            _ => default.into(),
        })
        .unwrap();
        assert_eq!(config.transports, vec!["none"]);
    }
    #[test]
    fn unknown_or_duplicate_transports_fail_closed() {
        for transport in ["udp", "http,http", "none,tcp", "nats"] {
            assert!(
                Config::from_lookup(|name, default| match name {
                    "WORKER_RULES_DIR" => "rules".into(),
                    "BROKER_TRANSPORTS" => transport.into(),
                    "NATS_URL" => String::new(),
                    _ => default.into(),
                })
                .is_err()
            );
        }
    }
}
