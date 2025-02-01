#![allow(dead_code)]

use super::{Error, Pool};
use camino::Utf8PathBuf as PathBuf;
use eyre::{eyre, WrapErr};
use serde_derive::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    env,
};

#[derive(Debug)]
pub struct ZpoolCmd {
    zpool_cmd: PathBuf,
}

/*
 *
{
  "output_version": {
    "command": "zpool list",
    "vers_major": 0,
    "vers_minor": 1
  },
  "pools": {
    "mainrust": {
*/
#[derive(Debug)]
pub struct ZpoolList {
    output_version: ZpoolListOutputVersion,
    pools: Vec<ZpoolListPool>,
}

#[derive(Debug, Deserialize)]
pub struct ZpoolListOutputVersion {
    command: String,
    vers_major: u32,
    vers_minor: u32,
}

/*
 *
  "pools": {
    "mainrust": {
      "name": "mainrust",
      "type": "POOL",
      "state": "ONLINE",
      "pool_guid": "13666012711349147706",
      "txg": "45178096",
      "spa_version": "5000",
      "zpl_version": "5",
*/
#[derive(Debug)]
pub struct ZpoolListPool {
    name: String,
    r#type: String,
    state: String,
    pool_guid: String,
    txg: String,
    spa_version: String,
    zpl_version: String,
    properties: BTreeMap<String, ZpoolListPoolProperty>,
    vdev: BTreeMap<String, ZpoolListVdev>,
}

#[derive(Debug, Deserialize)]
pub struct ZpoolListVdev {
    name: String,
    vdev_type: String,
    guid: String,
    class: String,
    state: String,
    properties: BTreeMap<String, ZpoolListVdevProperty>,
}

#[derive(Debug, Deserialize)]
pub struct ZpoolListVdevProperty {
    name: String,
    value: String,
    source: ZpoolListVdevPropertySource,
}

#[derive(Debug, Deserialize)]
pub struct ZpoolListVdevPropertySource {
    r#type: String,
    data: String,
}

impl ZpoolCmd {
    pub async fn list_pools(&self) -> Result<Vec<Pool>, Error> {
        let output = tokio::process::Command::new(&self.zpool_cmd)
            .arg("list")
            .arg("-H")
            .arg("-o")
            .arg("name")
            .output()
            .await
            .wrap_err("Failed to execute zpool list")?;

        if output.status.success() {
            Ok(String::from_utf8(output.stdout)
                .wrap_err("Failed to parse zpool list output")?
                .trim_end()
                .lines()
                .map(|line| Pool(line.to_owned()))
                .collect())
        } else {
            Err(eyre!(
                "zpool list failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into())
        }
    }

    pub async fn list_pool(&self, pool: &str) -> Result<Pool, Error> {
        let output = tokio::process::Command::new(&self.zpool_cmd)
            .arg("list")
            .arg("-H")
            .arg("-o")
            .arg("name")
            .arg(pool)
            .output()
            .await
            .wrap_err("Failed to execute zpool list")?;

        if output.status.success() {
            Ok(String::from_utf8(output.stdout)
                .wrap_err("Failed to parse zpool list output")?
                .trim_end()
                .lines()
                .map(|line| Pool(line.to_owned()))
                .next()
                .ok_or_else(|| eyre!("Pool not found"))?)
        } else {
            Err(eyre!(
                "zpool list failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into())
        }
    }

    // -g   Display vdev, GUIDs
    // -L   Display real paths of vdevs resolving all symbolic links
    // -n   Display config that would be used without adding
    // -P   Display real paths of vdevs instead of last component
    pub async fn add(&self, _force: bool, _pool: &str, _vdev: &str) -> Result<(), ()> {
        unimplemented!();
    }

    pub async fn attach(
        &self,
        _force: bool,
        _pool_properties: Vec<PoolProperty>,
        _pool: &str,
        _device: &str,
        _new_device: &str,
    ) -> Result<(), ()> {
        unimplemented!();
    }

    pub async fn clear(&self, _pool: &str, _device: Option<&str>) -> Result<(), ()> {
        unimplemented!();
    }

    pub async fn list(&self) -> Result<(), ()> {
        tokio::process::Command::new(&self.zpool_cmd)
            .arg("list")
            .arg("-jv")
            .status()
            .await
            .map_err(|_| ())
            .and_then(|status| if status.success() { Ok(()) } else { Err(()) })
    }
}

impl Default for ZpoolCmd {
    fn default() -> Self {
        ZpoolCmd {
            zpool_cmd: From::from(env::var("ZPOOL_CMD").unwrap_or("zpool".to_owned())),
        }
    }
}

pub struct PoolProperty {
    _property: String,
    _value: String,
}
