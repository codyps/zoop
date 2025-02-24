extern crate env_logger;
extern crate log;

#[macro_use]
extern crate clap;

extern crate zfs_cmd_api;
extern crate zoop;

use clap::Parser;
use clap::{AppSettings, Arg, SubCommand};
use std::io::Write;
use zfs_cmd_api::Zfs;
use zoop::*;

#[derive(Parser, Debug)]
struct Opts {
    /// Do not execute anything which would change system state. Print what state would be changed
    #[clap(short = "n")]
    dry_run: bool,

    /// emit extra info
    #[clap(short = "v")]
    verbose: bool,

    #[clap(subcommand)]
    subcommand: Command,
}

#[derive(Debug, Parser)]
enum Command {
    /// replicate an entire set of snapshots from one tree to another
    Zcopy(ZcopyOpts),
}

#[derive(Debug, Parser)]
struct ZcopyOpts {
    /// Also examine datasets that decend from the specified dataset
    #[clap(short = "r")]
    recusive: bool,

    /// Do not enable resumable send/recv when receiving
    #[clap(short = "Y")]
    not_resumable: bool,

    src_dataset: PathBuf,
    dest_dataset: PathBuf,
}

// hack to try to get `app_from_crate!()` to regenerate.
#[allow(dead_code)]
const CARGO_TOML: &'static str = include_str!("../Cargo.toml");

fn level_to_msg_prefix(level: log::Level) -> &'static str {
    use log::Level;
    match level {
        Level::Error => "<3>",
        Level::Warn => "<4>",
        Level::Info => "<6>",
        Level::Debug => "<7>",
        Level::Trace => "<7>",
    }
}

fn main() {
    env_logger::builder()
        .format(|w, rec| writeln!(w, "{}{}", level_to_msg_prefix(rec.level()), rec.args()))
        .init();

    let matches = app_from_crate!()
        .arg(Arg::with_name("dry-run")
             .help("Do not execute anything which would change system state. Print what would state would be changed")
             .short("n")
             .global(true)
            )
        .arg(Arg::with_name("verbose")
             .help("Emit extra info")
             .short("v")
             .global(true)
            )
        .setting(AppSettings::SubcommandRequired)
        .subcommand(SubCommand::with_name("zcopy")
            .arg(Arg::with_name("recursive")
                 .short("r")
                 .help("Also examine datasets the decend from the specified dataset")
                 )
            .arg(Arg::with_name("not-resumeable")
                 .short("Y")
                 .help("Do not enable resumable send/recv when receiving")
                 )
            // this matches zxfer style behavior
            /*
            .arg(Arg::with_name("preseve-path")
                 .short("P")
                 .help("Rather than using DEST_DATASET as the dest, place SRC_DATASET (the entire path) under DEST_DATASET")
            */
            .arg(Arg::with_name("SRC_DATASET")
                 .index(1)
                 .required(true)
                 )
            .arg(Arg::with_name("DEST_DATASET")
                 .index(2)
                 .required(true)
                 )
        /*
        .subcommand(SubCommand::with_name("zcopy-all")
            .arg(Arg::with_name("not-resumeable")
                 .short("Y")
                 .help("Do not enable resumable send/recv when receiving")
                 )
            .arg(Arg::with_name("DEST_DATASET")
                 .index(1)
                 .required(true)
                 )
        .subcommand(SubCommand::with_name("trim-by-time")
            .arg(Arg::with_name("DATASET")
                 .index(1)
                 .required(true)
                 )
        */
        // convert snapshots that were transfered to bookmarks
        //  use `guid` to determine those already replicated
        // subcommand(SubCommand::with_name("forget-replicated")
        //
        // create new snapshot(s)
        // subcommand(SubCommand::with_name("snap")
        //
        // examine snapshots & delete some of them
        // subcommand(SubCommand::with_name("snap-cleanup")
            ).get_matches();

    let dry_run = matches.occurrences_of("dry-run") > 0;
    let verbose = matches.occurrences_of("verbose") > 0;
    let not_resumable = matches.occurrences_of("not-resumable") > 0;

    let opts = ZcopyOpts {
        dry_run: dry_run,
        verbose: verbose,
        resumable: !not_resumable,
    };

    if let Some(matches) = matches.subcommand_matches("zcopy") {
        let src_dataset = matches.value_of("SRC_DATASET").unwrap();
        let dest_dataset = matches.value_of("DEST_DATASET").unwrap();

        let recursive = matches.occurrences_of("recursive") > 0;

        let dry_run = matches.occurrences_of("dry-run") > 0 || dry_run;
        let src_zfs = Zfs::from_env_prefix("SRC");
        let dest_zfs = Zfs::from_env_prefix("DEST");

        println!(
            "copy from {} to {} (recursive={})",
            src_dataset, dest_dataset, recursive
        );
        println!("dry_run: {}", dry_run);

        if recursive {
            zcopy_recursive(&src_zfs, &dest_zfs, &opts, src_dataset, dest_dataset).unwrap();
        } else {
            zcopy_one(&src_zfs, &dest_zfs, &opts, src_dataset, dest_dataset).unwrap();
        }
    } else {
        println!("need a SubCommand");
    }
}
