extern crate failure;
extern crate fmt_extra;
#[macro_use] extern crate failure_derive;

use std::path::{Path,PathBuf};
use std::env;
use std::ffi::OsStr;
use std::process;
use std::{io,fmt};

mod zpool;

#[derive(Debug,PartialEq,Eq,Clone)]
pub struct Zfs {
    zfs_cmd: PathBuf,
}

#[derive(Debug,PartialEq,Eq,Clone)]
pub enum ListTypes {
    Filesystem,
    Snapshot,
    Volume,
    Bookmark,
}

#[derive(Debug,Fail)]
pub enum ZfsError {
    #[fail(display = "execution of zfs command failed: {}", io)]
    Exec {
        io: io::Error
    },

    #[fail(display = "zfs command returned an error: {}", status)]
    Process {
        status: process::ExitStatus
    },
}

#[derive(Debug,PartialEq,Eq,Clone)]
pub struct ZfsList {
    out: Vec<u8>,
}

impl fmt::Display for ZfsList {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result
    {
        write!(fmt, "[")?;
        for i in self.iter() {
            write!(fmt, "{},", fmt_extra::AsciiStr(i))?;
        }
        write!(fmt, "]")
    }
}

impl ZfsList {
    pub fn iter(&self) -> impl Iterator<Item=&[u8]>
    {
        self.out.split(|&x| x ==  b'\n').filter(|x| x.len() != 0)
    }
}

#[derive(Debug,Default,PartialEq,Eq,Clone)]
struct TypeSpec {
    include_fs: bool,
    include_snap: bool,
    include_vol: bool,
    include_bookmark: bool,
}

impl From<TypeSpec> for String {
    fn from(t: TypeSpec) -> Self {
        let mut v = vec![];
        if t.include_fs {
            v.push("filesystem")
        }
        if t.include_snap {
            v.push("snapshot")
        }
        if t.include_vol {
            v.push("volume")
        }
        if t.include_bookmark {
            v.push("bookmark")
        }

        v.join(",")
    }
}

#[derive(Debug,PartialEq,Eq,Clone)]
enum ListRecurse {
    No,
    Depth(usize),
    Yes,
}

impl Default for ListRecurse {
    fn default() -> Self {
        ListRecurse::No
    }
}

/// Note: no support for sorting, folks can do that in rust if they really want it.
#[derive(Debug,Default,PartialEq,Eq,Clone)]
pub struct ListBuilder {
    recursive: ListRecurse,
    dataset_types: Option<TypeSpec>,
}

impl ListBuilder {
    pub fn depth(&mut self, levels: usize) -> Self {
        self.recursive = ListRecurse::Depth(levels);
        self.clone()
    }

    pub fn recursive(&mut self) -> Self {
        self.recursive = ListRecurse::Yes;
        self.clone()
    }

    pub fn include_filesystems(&mut self) -> Self {
        self.dataset_types.get_or_insert(TypeSpec::default()).include_fs = true;
        self.clone()
    }

    pub fn include_snapshots(&mut self) -> Self {
        self.dataset_types.get_or_insert(TypeSpec::default()).include_snap = true;
        self.clone()
    }

    pub fn include_bookmarks(&mut self) -> Self {
        self.dataset_types.get_or_insert(TypeSpec::default()).include_bookmark = true;
        self.clone()
    }

    pub fn include_volumes(&mut self) -> Self {
        self.dataset_types.get_or_insert(TypeSpec::default()).include_vol = true;
        self.clone()
    }
}

impl Zfs {
    pub fn list_from_builder(&self, builder: ListBuilder) -> Result<ZfsList, ZfsError>
    {
        // zfs list -H
        // '-s <prop>' sort by property (multiple allowed)
        // '-d <depth>' recurse to depth
        // '-r' 
        let mut cmd = process::Command::new(&self.zfs_cmd);

        cmd
            .arg("list")
            // +parsable, +scripting mode
            .arg("-pH")
            // only name
            .arg("-o").arg("name");

        match builder.recursive {
            ListRecurse::No => {},
            ListRecurse::Depth(sz) => {
                cmd.arg("-d").arg(format!("{}",sz));
            },
            ListRecurse::Yes => {
                cmd.arg("-r");
            }
        }

        match builder.dataset_types {
            None => {
                // TODO: should we require this?
            },
            Some(v) => {
                cmd.arg(String::from(v));
            }
        }

        let output = cmd.output().map_err(|e| ZfsError::Exec{ io: e})?;

        if !output.status.success() {
            println!("status: {}", output.status);
            return Err(ZfsError::Process { status: output.status });
        }

        if output.stderr.len() > 0 {
            println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        }

        Ok(ZfsList { out: output.stdout })
    }


    fn list_with_args(&self, args: &[&str]) -> Result<ZfsList, ZfsError>
    {
        // zfs list -H
        // '-s <prop>' sort by property (multiple allowed)
        // '-d <depth>' recurse to depth
        // '-r' 
        let output = process::Command::new(&self.zfs_cmd)
            .arg("list")
            // +parsable, +scripting mode
            .arg("-pH")
            // only name
            .arg("-o").arg("name")
            .args(args)
            .output().map_err(|e| ZfsError::Exec{ io: e})?;

        if !output.status.success() {
            println!("status: {}", output.status);
            return Err(ZfsError::Process { status: output.status });
        }

        if output.stderr.len() > 0 {
            println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        }

        Ok(ZfsList { out: output.stdout })
    }

    pub fn list(&self) -> Result<ZfsList, ZfsError>
    {
        self.list_with_args(&[])
    }

    // delete
    //
    // hold
    // release
    //
    // create
    //
    // send
    // recv
    //
    // get (for resume)
}

impl Default for Zfs {
    fn default() -> Self {
        Zfs {
            zfs_cmd: From::from(env::var_os("ZFS_CMD").unwrap_or(OsStr::new("zfs").to_owned())),
        }
    }
}
