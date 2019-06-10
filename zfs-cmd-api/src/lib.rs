extern crate failure;
extern crate fmt_extra;
#[macro_use] extern crate failure_derive;

extern crate enumflags2;
#[macro_use]
extern crate enumflags2_derive;

use enumflags2::BitFlags;
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

#[derive(Debug)]
pub struct CmdInfo {
    status: process::ExitStatus,
    stderr: String,
    cmd: String,
}


#[derive(Debug,Fail)]
pub enum ZfsError {
    #[fail(display = "execution of zfs command failed: {}", io)]
    Exec {
        io: io::Error
    },

    #[fail(display = "zfs command returned an error: {:?}", cmd_info)]
    Process {
        cmd_info: CmdInfo,
    },

    // A specific CannotOpen kind
    #[fail(display = "no such dataset '{}' ({:?})", dataset, cmd_info)]
    NoDataset {
        dataset: String,
        cmd_info: CmdInfo,
    },

    #[fail(display = "cannot open: {:?}", cmd_info)]
    CannotOpen {
        cmd_info: CmdInfo,
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

impl Default for ZfsList {
    fn default() -> Self {
        ZfsList { out: Vec::new() }
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

    fn cmd(&self) -> process::Command
    {
        process::Command::new(&self.zfs_cmd)
    }

    fn cmdinfo_to_error(cmd_info: CmdInfo) -> ZfsError
    {

        // status: ExitStatus(ExitStatus(256)), stderr: "cannot open \'innerpool/TMP/zoop-test-28239/dst/sub_ds\': dataset does not exist\n"
        let prefix_ca = "cannot open '";
        if cmd_info.stderr.starts_with(prefix_ca) {
            let ds_rest = &cmd_info.stderr[prefix_ca.len()..];
            let mut s = ds_rest.split("': ");
            eprintln!("ds_rest: {:?}", ds_rest);
            let ds = s.next().unwrap();
            eprintln!("ds: {:?}", ds);
            let error = s.next().unwrap();
            eprintln!("error: {:?}", error);
            return match error {
                "dataset does not exist\n" => {
                    ZfsError::NoDataset {
                        dataset: ds.to_owned(),
                        cmd_info: cmd_info,
                    }
                },
                _ => {
                    ZfsError::CannotOpen {
                        cmd_info: cmd_info,
                    }
                }
            };
        }

        // generic error
        ZfsError::Process {
            cmd_info: cmd_info,
        }
    }

    pub fn list_from_builder(&self, builder: &ListBuilder) -> Result<ZfsList, ZfsError>
    {
        // zfs list -H
        // '-s <prop>' sort by property (multiple allowed)
        // '-d <depth>' recurse to depth
        // '-r' 
        let mut cmd = self.cmd();

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

        eprintln!("run: {:?}", cmd);

        let output = cmd.output().map_err(|e| ZfsError::Exec{ io: e})?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr[..]).into_owned();

            let cmd_info = CmdInfo {
                status: output.status,
                stderr: stderr,
                cmd: format!("{:?}", cmd),
            };

            return Err(Self::cmdinfo_to_error(cmd_info))
        }

        if output.stderr.len() > 0 {
            eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
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

    /*
    /// Resume sending a stream using `receive_resume_token` from the destination filesystem
    ///
    /// flags here is constrained to `[Penv]`
    pub fn send_resume(&self, receive_resume_token: String, flags: SendFlags) -> ZfsSend
    {

    }

    //pub fn recv_abort_incomplete(&self)
    */

    pub fn send(&self, snapname: &str, from: Option<&str>, flags: BitFlags<SendFlags>) -> io::Result<ZfsSend>
    {
        let mut cmd = self.cmd();

        cmd.arg("send");

        let mut opts = "-".to_owned();

        let mut include_intermediary = false;
        // realistically, a series of `if flags.contains(*) {*}` statements more susinctly
        // represents the work needed to be done here. Unfortunately, it isn't clear how to form
        // that in a way that ensures we have handling for all `SendFlags`.
        for flag in flags.iter() {
            match flag {
                SendFlags::EmbedData => { opts.push('e') },
                SendFlags::LargeBlock => { opts.push('L') },
                SendFlags::Compressed => { opts.push('c') },
                SendFlags::Raw => { opts.push('w') },

                SendFlags::Dedup => { opts.push('D') },

                SendFlags::IncludeIntermediary => {
                    include_intermediary = true
                },
                SendFlags::IncludeHolds => { opts.push('h') },
                SendFlags::IncludeProps => { opts.push('P') },
                SendFlags::Verbose => { opts.push('v') },
                SendFlags::DryRun => { opts.push('n') },
                SendFlags::Parsable => { opts.push('P') },
                SendFlags::Replicate => { opts.push('R') },
            }
        }

        cmd.arg(opts);

        match from {
            Some(f) => {
                if include_intermediary {
                    cmd.arg("-I")
                } else {
                    cmd.arg("-i")
                }.arg(f);
            }
            None => {
                if include_intermediary {
                    panic!("include_intermediary set to no effect because no `from` was specified");
                }
            }
        }

        cmd.arg(snapname);

        eprintln!("run: {:?}", cmd);

        Ok(ZfsSend {
            child: cmd
                .stdout(std::process::Stdio::piped())
                .spawn()?
        })
    }

    // XXX: `set_props` would ideally take an iterator over things that are &str like
    // 
    // note: `lzc_receive()` uses bools for `force` and `raw`, and has no other flags. It then has
    // a seperate `lzc_receive_resumable()` function for resumable (which internally passes another
    // boolean), `lzc_receive_with_reader()` then exposes an additional `resumable` boolean (but
    // also provides a mechanism to pass in a `dmu_replay_record_t` which was read from the `fd`
    // prior to function invocation).
    pub fn recv(&self, snapname: &str, set_props: Vec<(String,String)>, origin: Option<&str>,
        
        exclude_props: Vec<String>, flags: BitFlags<RecvFlags>) ->
        io::Result<ZfsRecv>
    {
        let mut cmd = self.cmd();

        cmd.arg("recv");

        let mut opts = "-".to_owned();

        for flag in flags.iter() {
            match flag {
                RecvFlags::Force => opts.push('F'),
                RecvFlags::Resumeable => opts.push('s'),

                RecvFlags::DiscardFirstName => opts.push('d'),
                RecvFlags::DiscardAllButLastName => opts.push('e'),
                RecvFlags::IgnoreHolds => opts.push('h'),
                RecvFlags::DryRun => opts.push('n'),
                RecvFlags::NoMount => opts.push('u'),
                RecvFlags::Verbose => opts.push('v'),
            }
        }

        cmd
            .arg(opts);

        for set_prop in set_props.into_iter() {
            let mut s = set_prop.0;
            s.push('=');
            s.push_str(&set_prop.1[..]);
            cmd.arg("-o").arg(s);
        }

        for exclude_prop in exclude_props.into_iter() {
            cmd.arg("-x").arg(exclude_prop);
        }

        match origin {
            Some(o) => { cmd.arg("-o").arg(o); },
            None => {},
        }

        cmd.arg(snapname);
        eprintln!("run: {:?}", cmd);

        Ok(ZfsRecv {
            child: cmd
                .stdin(std::process::Stdio::piped())
                .spawn()?,
        })
    }
}

pub struct ZfsSend {
    // note: in the lzc case, this is just a `fd`
    child: std::process::Child,
}

pub struct ZfsRecv {
    // note: in the lzc case, this is just a `fd`
    child: std::process::Child,
}

pub fn send_recv(mut send: ZfsSend, mut recv: ZfsRecv) -> io::Result<u64>
{
    let bytes = std::io::copy(send.child.stdout.as_mut().unwrap(), recv.child.stdin.as_mut().unwrap())?;

    // discard the stdin/stdout we left open
    // (and hope this causes the subprocesses to exit)
    send.child.stdout.take();
    recv.child.stdin.take();

    let ss = send.child.wait()?;
    let rs = recv.child.wait()?;

    if !ss.success() || !rs.success() {
        return Err(io::Error::new(io::ErrorKind::Other,
                           format!("send or recv failed: {:?}, {:?}", ss.code(), rs.code())));
    }

    Ok(bytes)
}

#[derive(EnumFlags,Copy,Clone,Debug,PartialEq,Eq)]
pub enum RecvFlags {
    // correspond to `lzc` booleans/functions
    /// -F
    Force = 1<<0,
    /// -s
    Resumeable = 1<<1,

    // lzc includes a `raw` boolean with no equivelent in the `zfs recv` cmd. It isn't immediately
    // clear how this gets set by `zfs recv`, but it might be by examining the
    // `dmu_replay_record_t`.
    //
    // Raw,

    // No equive in `lzc`
    // These appear to essentially be implimented by
    // examining the `dmu_replay_record_t` and modifying args to `lzc_recieve_with_header()`.
    /// -d
    DiscardFirstName = 1<<2,
    /// -e
    DiscardAllButLastName = 1<<3,

    // `zfs receive` options with no equive in `lzc`.
    //
    // unclear how holds are handled. `zfs send` has a similar mismatch (no flag in `lzc_send()`)
    /// -h
    IgnoreHolds = 1<<4,
    // I really don't know.
    /// -u
    NoMount = 1<<5,


    /// -v
    Verbose = 1<<6,
    /// -n
    DryRun = 1<<7,
}


#[derive(EnumFlags,Copy,Clone,Debug,PartialEq,Eq)]
pub enum SendFlags {
    // correspond to lzc SendFlags
    /// -e
    EmbedData = 1<<0,
    /// -L
    LargeBlock = 1<<1,
    /// -c
    Compressed = 1<<2,
    /// -w
    Raw = 1<<3,

    // these are additional items corresponding to `zfs send` cmd flags
    /// -D
    Dedup = 1<<4,
    /// -I
    IncludeIntermediary = 1<<5,
    /// -h
    IncludeHolds = 1<<6,
    /// -p
    IncludeProps = 1<<7,
    /// -v
    Verbose = 1<<8,
    /// -n
    DryRun = 1<<9,
    /// -P
    Parsable = 1<<10,
    /// -R
    Replicate = 1<<11,
}


// 
// send -t <token>
//  resume send
// send -D
//  dedup. depricated
// send -I <snapshot>
//  send all intermediary snapshots from <snapshot>
// send -L
//  large block
// send -P
//  print machine parsable info
// send -R
//  replicate (send filesystem and all decendent filesystems up to the named snapshot)
// send -e
//  embed (generate a more compact stream)
// send -c
//  compress
// send -w
//  raw
// send -h
//  holds included
// send -n
//  dry run
// send -p
//  props -- include dataset props in stream
// send -v
//  verbose
// send -i <snapshot>
//  generate stream from the first <snapshot> [src] to the second <snapshot> [target]
// 

impl Default for Zfs {
    fn default() -> Self {
        Zfs {
            zfs_cmd: From::from(env::var_os("ZFS_CMD").unwrap_or(OsStr::new("zfs").to_owned())),
        }
    }
}
