extern crate failure;
extern crate fmt_extra;
#[macro_use] extern crate failure_derive;

use std::ops::{Deref,DerefMut};
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

    #[fail(display = "zfs command returned an error: {}: {:?}: {:?}", status, stderr, cmd)]
    Process {
        status: process::ExitStatus,
        stderr: String,
        cmd: String,
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

impl<'a> From<&'a ZfsList> for Vec<Vec<String>> {
    fn from(x: &'a ZfsList) -> Self {
        // key is first item, remaining items are placed in their own vectors
        
        let mut h = Vec::default();

        for i in x.iter() {
            // collect `i` into a Vec<Vec<u8>>
            let mut vs = Vec::default();
            let mut v = Vec::default();

            for b in i {
                if *b == b'\t'  {
                    vs.push(String::from_utf8(v).unwrap());
                    v = Vec::default();
                } else {
                    v.push(*b);
                }
            }

            vs.push(String::from_utf8(v).unwrap());
            h.push(vs);
        }

        h
    }
}

#[derive(Debug,Default,PartialEq,Eq,Clone)]
struct TypeSpec {
    include_fs: bool,
    include_snap: bool,
    include_vol: bool,
    include_bookmark: bool,
}

impl<'a> From<&'a TypeSpec> for String {
    fn from(t: &'a TypeSpec) -> Self {
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
#[derive(Debug,PartialEq,Eq,Clone,Default)]
pub struct ListBuilder {
    recursive: ListRecurse,
    dataset_types: Option<TypeSpec>,
    elements: Vec<&'static str>,
    base_dataset: Option<String>
}

impl ListBuilder {
    pub fn depth(&mut self, levels: usize) -> &mut Self {
        self.recursive = ListRecurse::Depth(levels);
        self
    }

    pub fn recursive(&mut self) -> &mut Self {
        self.recursive = ListRecurse::Yes;
        self
    }

    pub fn include_filesystems(&mut self) -> &mut Self {
        self.dataset_types.get_or_insert(TypeSpec::default()).include_fs = true;
        self
    }

    pub fn include_snapshots(&mut self) -> &mut Self {
        self.dataset_types.get_or_insert(TypeSpec::default()).include_snap = true;
        self
    }

    pub fn include_bookmarks(&mut self) -> &mut Self {
        self.dataset_types.get_or_insert(TypeSpec::default()).include_bookmark = true;
        self
    }

    pub fn include_volumes(&mut self) -> &mut Self {
        self.dataset_types.get_or_insert(TypeSpec::default()).include_vol = true;
        self
    }

    pub fn with_elements(&mut self, mut elements: Vec<&'static str>) -> &mut Self {
        self.elements.append(&mut elements);
        self
    }

    pub fn with_dataset<T: Into<String>>(&mut self, dataset: T) -> &mut Self {
        self.base_dataset = Some(dataset.into());
        self
    }
}

pub struct ListExecutor<'a> {
    parent: &'a Zfs,
    builder: ListBuilder,
}

impl<'a> ListExecutor<'a> {
    fn from_parent(zfs: &'a Zfs) -> Self {
        ListExecutor {
            parent: zfs,
            builder: Default::default()
        }
    }

    pub fn query(&self) -> Result<ZfsList, ZfsError> {
        self.parent.list_from_builder(self)
    }
}

impl<'a> Deref for ListExecutor<'a> {
    type Target = ListBuilder;
    fn deref(&self) -> &ListBuilder {
        &self.builder
    }
}

impl<'a> DerefMut for ListExecutor<'a> {
    fn deref_mut(&mut self) -> &mut ListBuilder {
        &mut self.builder
    }
}

impl Zfs {
    pub fn list_from_builder(&self, builder: &ListBuilder) -> Result<ZfsList, ZfsError>
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
            // sorting by name is faster.
            // TODO: find out why
            .arg("-s").arg("name")
            ;

        if builder.elements.len() == 0 {
            cmd
                // only name
                .arg("-o").arg("name")
                ;
        } else {
            let mut elem_arg = String::new();
            for e in builder.elements.iter() {
                elem_arg.push_str(e);
                elem_arg.push(',');
            }

            cmd.arg("-o").arg(elem_arg);
        }

        match builder.recursive {
            ListRecurse::No => {},
            ListRecurse::Depth(sz) => {
                cmd.arg("-d").arg(format!("{}",sz));
            },
            ListRecurse::Yes => {
                cmd.arg("-r");
            }
        }

        match &builder.dataset_types {
            &None => {
                // TODO: should we require this?
            },
            &Some(ref v) => {
                cmd.arg("-t").arg(String::from(v));
            }
        }

        match builder.base_dataset {
            None => {},
            Some(ref v) => {
                cmd.arg(v);
            }
        }

        let output = cmd.output().map_err(|e| ZfsError::Exec{ io: e})?;

        if !output.status.success() {
            println!("status: {}", output.status);
            return Err(ZfsError::Process {
                status: output.status,
                stderr: String::from_utf8_lossy(&output.stderr[..]).into_owned(),
                cmd: format!("{:?}", cmd),
            });
        }

        if output.stderr.len() > 0 {
            println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        }

        Ok(ZfsList { out: output.stdout })
    }

    pub fn list_basic(&self) -> Result<ZfsList, ZfsError>
    {
        self.list().query()
    }

    pub fn list(&self) -> ListExecutor {
        ListExecutor::from_parent(self)
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

    pub fn from_env_prefix(prefix: &'static str) -> Self {
        // TODO: consider failing if {}_ZFS_CMD is not a valid OsStr
        // TODO: parse this into a series of values

        let env_name = format!("{}_ZFS_CMD", prefix);
        let env_specific = env::var_os(env_name);
        let env = match env_specific {
            Some(x) => x,
            None => env::var_os("ZFS_CMD").unwrap_or(OsStr::new("zfs").to_owned()),
        };

        Zfs {
            zfs_cmd: From::from(env),
        }
    }
}

impl Default for Zfs {
    fn default() -> Self {
        Zfs {
            zfs_cmd: From::from(env::var_os("ZFS_CMD").unwrap_or(OsStr::new("zfs").to_owned())),
        }
    }
}
