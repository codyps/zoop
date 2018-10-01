
#[macro_use]
extern crate clap;

extern crate zfs_cmd_api;

use zfs_cmd_api::Zfs;
use clap::{Arg,SubCommand,AppSettings};

// hack to try to get `app_from_crate!()` to regenerate.
#[allow(dead_code)]
const CARGO_TOML: &'static str = include_str!("../Cargo.toml");

fn main() {

    let matches = app_from_crate!()
        .arg(Arg::with_name("dry-run")
             .help("Do not execute anything which would change system state. Print what would state would be changed")
             .short("N")
             .global(true)
            )
        .setting(AppSettings::SubcommandRequired)
        .subcommand(SubCommand::with_name("zcopy")
            .arg(Arg::with_name("recursive")
                 .short("R")
                 .help("Also examine datasets the decend from the specified dataset")
                 )
            .arg(Arg::with_name("SRC_DATASET")
                 .index(1)
                 .required(true)
                 )
            .arg(Arg::with_name("DEST_DATASET")
                 .index(2)
                 .required(true)
                )
            ).get_matches();


    let dry_run = matches.occurrences_of("dry-run") > 0;


    if let Some(matches) = matches.subcommand_matches("zcopy") {
        let src_dataset = matches.value_of("SRC_DATASET").unwrap();
        let dest_dataset = matches.value_of("DEST_DATASET").unwrap();

        let recursive = matches.occurrences_of("recursive") > 0;

        let dry_run = matches.occurrences_of("dry-run") > 0 || dry_run;

        println!("copy from {} to {} (recursive={})", src_dataset, dest_dataset, recursive);
        println!("dry_run: {}", dry_run);


        let src_zfs = Zfs::default();
        let dest_zfs = Zfs::default();


        for i in src_zfs.list() {
            println!("item: {}", i);
        }

        // XXX: bookmarks on the SRC allow deletion of snapshots while still keeping send
        // efficiency. As a result, we should create bookmarks to identify points we'll want to
        // sync from
        //
        // XXX: the createtxg property allows ordering by creation for incremental send/recv
        // (bookmarks are basically a named createtxg). We may need to use the createtxg in out
        // common-base-selection
        //
        // XXX: datasets have guids that persist across send/recv. Use this to identify common
        // elements. guids identify snapshots across pools.

        // for dataset, find the common base snapshot
        // for each snapshot after the common base
        //  send it snap
        //  common base = sent snap
        //  repeat until all snaps in src are in dest

    } else {
        println!("need a SubCommand");
    }
}
