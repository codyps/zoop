
#[macro_use] extern crate log;

extern crate failure;
extern crate fmt_extra;
#[macro_use] extern crate failure_derive;

extern crate enumflags2;
#[macro_use]
extern crate enumflags2_derive;

extern crate shell_words;

use enumflags2::BitFlags;
use std::ops::{Deref,DerefMut};
use std::env;
use std::ffi::OsStr;
use std::process;
use std::{io,fmt};

mod zpool;

#[derive(Debug,PartialEq,Eq,Clone)]
pub struct Zfs {
    // FIXME: we require utf-8 here
    zfs_cmd: Vec<String>,
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

    #[fail(display = "cannot resume send of nonexistent dataset '{}' ({:?})", dataset, cmd_info)]
    CannotResumeSendDoesNotExist {
        dataset: String,
        cmd_info: CmdInfo,
    },

    #[fail(display = "cannot resume send: {:?}", cmd_info)]
    CannotResumeSend {
        cmd_info: CmdInfo,
    },

    #[fail(display = "cannot recv: failed to read stream ({:?})", cmd_info)]
    CannotRecvFailedToRead {
        cmd_info: CmdInfo,
    },

    #[fail(display = "cannot recv new fs: {:?}", cmd_info)]
    CannotRecvNewFs {
        cmd_info: CmdInfo
    }
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

    pub fn with_elements(&mut self, mut elements: &[&'static str]) -> &mut Self {
        self.elements.extend_from_slice(&mut elements);
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

fn cmdinfo_to_error(cmd_info: CmdInfo) -> ZfsError
{
    // status: ExitStatus(ExitStatus(256)), stderr: "cannot open \'innerpool/TMP/zoop-test-28239/dst/sub_ds\': dataset does not exist\n"
    let prefix_ca = "cannot open '";
    if cmd_info.stderr.starts_with(prefix_ca) {
        let ds_rest = &cmd_info.stderr[prefix_ca.len()..].to_owned();
        let mut s = ds_rest.split("': ");
        let ds = s.next().unwrap();
        let error = s.next().unwrap();
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

    // Resuming partial recv in tank/backup/zxfer/franklin/franklin/ROOT/arch
    // cannot resume send: 'franklin/ROOT/arch@znap_2019-10-28-1630_frequent' used in the initial send no longer exists
    // cannot receive: failed to read from stream
    // thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: Custom { kind: Other, error: "send or recv failed: Some(255), Some(1)" }>
    // note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace.
    let prefix_crs = "cannot resume send: '";
    if cmd_info.stderr.starts_with(prefix_crs) {
        let ds_rest = &cmd_info.stderr[prefix_crs.len()..];
        let mut s = ds_rest.split("' ");
        let ds = s.next().unwrap().to_owned();
        let error = s.next().unwrap();
        return match error {
            "used in the initial send no longer exists\n" => {
                ZfsError::CannotResumeSendDoesNotExist {
                    dataset: ds.to_owned(),
                    cmd_info: cmd_info,
                }
            },
            _ => {
                ZfsError::CannotResumeSend {
                    cmd_info: cmd_info,
                }
            }
        };
    }

    // run: "zfs" "send" "-eLcw" "mainrust/ROOT@znap_2019-10-01-0446_monthly"
    // run: "zfs" "recv" "-Fs" "tank/backup/zoop/arnold2/mainrust/ROOT"
    // cannot receive new filesystem stream: destination has snapshots (eg. tank/backup/zoop/arnold2/mainrust/ROOT@znap_2019-08-23-2348_frequent)
    // must destroy them to overwrite it
    // thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: Custom { kind: Other, error: "send or recv failed: Some(0), Some(1)" }', src/libcore/result.rs:1084:5
    // note: run with `RUST_BACKTRACE=1` environment variable to display a backtra
    let prefix_crnfs = "cannot receive new filesystem stream: ";
    if cmd_info.stderr.starts_with(prefix_crnfs) {
        return ZfsError::CannotRecvNewFs {
            cmd_info: cmd_info,
        };
    }

    //   sending mainrust/ROOT@znap_2019-09-01-0631_monthly
    //  run: "zfs" "send" "-eLcw" "-i" "mainrust/ROOT@znap_2019-11-22-0334_frequent" "mainrust/ROOT@znap_2019-09-01-0631_monthly"
    //  run: "zfs" "recv" "-Fs" "tank/backup/zoop/arnold2/mainrust/ROOT"
    //  warning: cannot send 'mainrust/ROOT@znap_2019-09-01-0631_monthly': not an earlier snapshot from the same fs
    //  cannot receive: failed to read from stream
    //  thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: Custom { kind: Other, error: "send or recv failed: Some(1), Some(1)" }', src/libcore/result.rs:1084:5
    //  note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace.


//  zcopy: mainrust/ROOT/gentoo to tank/backup/zoop/arnold2/mainrust/ROOT/gentoo
//   new filesystem (no basis)
//   sending mainrust/ROOT/gentoo@znap_2019-09-01-0631_monthly
//  run: "zfs" "send" "-eLcw" "mainrust/ROOT/gentoo@znap_2019-09-01-0631_monthly"
//  run: "zfs" "recv" "-Fs" "tank/backup/zoop/arnold2/mainrust/ROOT/gentoo"
//  umount: /tank/backup/zoop/arnold2/mainrust/ROOT/gentoo/var: no mount point specified.
//  cannot unmount '/tank/backup/zoop/arnold2/mainrust/ROOT/gentoo/var': umount failed
//  thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: Custom { kind: Other, error: "send or recv failed: Some(0), Some(1)" }', src/libcore/result.rs:1084:5
//  note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace.

    match cmd_info.stderr.as_ref() {
        "cannot receive: failed to read from stream\n" => {
            ZfsError::CannotRecvFailedToRead {
                cmd_info: cmd_info,
            }
        },
        _ => {
            // generic error
            ZfsError::Process {
                cmd_info: cmd_info,
            }
        }
    }
}


impl Zfs {

    fn cmd(&self) -> process::Command {
        let mut cmd = process::Command::new(&self.zfs_cmd[0]);
        cmd.args(&self.zfs_cmd[1..]);
        cmd
    }

    fn run_output(&self, mut cmd: process::Command) -> Result<std::process::Output, ZfsError> {
        info!("run: {:?}", cmd);

        let output = cmd.output().map_err(|e| ZfsError::Exec{ io: e})?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr[..]).into_owned();

            let cmd_info = CmdInfo {
                status: output.status,
                stderr: stderr,
                cmd: format!("{:?}", cmd),
            };

            return Err(cmdinfo_to_error(cmd_info))
        }

        if output.stderr.len() > 0 {
            warn!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        }

       Ok(output)
    }

    pub fn list_from_builder(&self, builder: &ListBuilder) -> Result<ZfsList, ZfsError> {
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

        let output = self.run_output(cmd)?;

        Ok(ZfsList { out: output.stdout })
    }

    pub fn list_basic(&self) -> Result<ZfsList, ZfsError>
    {
        self.list().query()
    }

    pub fn list(&self) -> ListExecutor {
        ListExecutor::from_parent(self)
    }

    /// NOTE: manual documents that if `dataset` is a bookmark, no flags are permitted
    pub fn destroy(&self, flags: BitFlags<DestroyFlags>, dataset: &str) -> Result<std::process::Output, ZfsError> {
        let mut cmd = self.cmd();
        cmd.arg("destroy");

        if !flags.is_empty() {
            let mut opts = "-".to_owned();
            for flag in flags.iter() {
                opts.push(match flag {
                    DestroyFlags::RecursiveDependents => 'R',
                    DestroyFlags::ForceUmount => 'f',
                    DestroyFlags::DryRun => 'n',
                    DestroyFlags::MachineParsable => 'p',
                    DestroyFlags::RecursiveChildren => 'r',
                    DestroyFlags::Verbose => 'v',
                });
            }

            cmd.arg(opts);
        }
        cmd.arg(dataset);
        self.run_output(cmd)
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

        let env = env.to_str().expect("env is not utf-8");

        let zfs_cmd = shell_words::split(env).expect("failed to split words");

        Zfs {
            zfs_cmd: zfs_cmd,
        }
    }

    /// Resume sending a stream using `receive_resume_token` from the destination filesystem
    ///
    /// flags here is constrained to `[Penv]`
    pub fn send_resume(&self, receive_resume_token: &str, flags: BitFlags<SendFlags>) -> io::Result<ZfsSend>
    {
        let mut cmd = self.cmd();

        cmd.arg("send");

        if !flags.is_empty() {
            let mut opts = "-".to_owned();

            // forbidden flags:
            //  - `replicate`: `-R`
            //  - `props`: `-p`
            //  - `backup`: `-b`
            //  - `dedup`: `-D`
            //  - `holds`: `-h`
            //  - `redactbook`: `-d` `arg`

            for flag in flags.iter() {
                match flag {
                    SendFlags::LargeBlock => { opts.push('L') },
                    SendFlags::EmbedData => { opts.push('e') },
                    SendFlags::Compressed => { opts.push('c') },
                    SendFlags::Raw => { opts.push('w') },

                    SendFlags::Verbose => { opts.push('v') },
                    SendFlags::DryRun => { opts.push('n') },
                    SendFlags::Parsable => { opts.push('P') },
                    _ => { panic!("unsupported flag: {:?}", flag); }
                }
            }
            cmd.arg(opts);
        }

        cmd.arg("-t").arg(receive_resume_token);

        info!("run: {:?}", cmd);

        Ok(ZfsSend {
            child: cmd
                .stdout(std::process::Stdio::piped())
                .spawn()?
        })
    }

    pub fn recv_abort_incomplete(&self, dataset: &str) -> Result<(), ZfsError> {
        let mut cmd = self.cmd();

        cmd.arg("recv")
            .arg("-A")
            .arg(dataset);

        self.run_output(cmd)?;
        Ok(())
    }

    pub fn send(&self, snapname: &str, from: Option<&str>, flags: BitFlags<SendFlags>) -> io::Result<ZfsSend>
    {
        let mut cmd = self.cmd();

        cmd.arg("send");

        let mut include_intermediary = false;
        if !flags.is_empty() {
            let mut opts = "-".to_owned();
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
                    SendFlags::IncludeProps => { opts.push('p') },
                    SendFlags::Verbose => { opts.push('v') },
                    SendFlags::DryRun => { opts.push('n') },
                    SendFlags::Parsable => { opts.push('P') },
                    SendFlags::Replicate => { opts.push('R') },
                }
            }

            cmd.arg(opts);
        }

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

        info!("run: {:?}", cmd);

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
    pub fn recv(&self, snapname: &str, set_props: &[(&str, &str)], origin: Option<&str>,
        exclude_props: &[&str], flags: BitFlags<RecvFlags>) ->
        io::Result<ZfsRecv>
    {
        let mut cmd = self.cmd();

        cmd.arg("recv");

        if !flags.is_empty() {
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
        }

        for set_prop in set_props.into_iter() {
            let mut s = String::new();
            s.push_str(set_prop.0.as_ref());
            s.push('=');
            s.push_str(set_prop.1.as_ref());
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
        info!("run: {:?}", cmd);

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
    // XXX: It woudl be _really_ nice to be able to consume stderr from both send & recv into our
    // own data to examine. right now we have to guess about the error cause.
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

#[derive(EnumFlags,Copy,Clone,Debug,PartialEq,Eq)]
pub enum DestroyFlags {
    RecursiveDependents = 1 << 0,
    ForceUmount = 1 << 1,
    DryRun = 1 << 2,
    MachineParsable = 1 << 3,
    RecursiveChildren = 1 << 4,
    Verbose = 1 << 5,
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
            zfs_cmd: vec![env::var_os("ZFS_CMD").unwrap_or(OsStr::new("zfs").to_owned()).to_str().unwrap().to_owned()],
        }
    }
}
