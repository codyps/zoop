
#[macro_use]
extern crate clap;

extern crate zfs_cmd_api;
extern crate fmt_extra;

use zfs_cmd_api::Zfs;
use clap::{Arg,SubCommand,AppSettings};
use std::collections::BTreeMap;

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

        let src_zfs = Zfs::from_env_prefix("SRC");
        let dest_zfs = Zfs::from_env_prefix("DEST");

        let mut list_builder = zfs_cmd_api::ListBuilder::default();
        list_builder.include_bookmarks().include_snapshots()
            .depth(1)
            .with_elements(vec!["createtxg", "name"]);

        let src_list = src_zfs.list_from_builder(list_builder.clone().with_dataset(src_dataset))
            .expect("src list failed");
        let dst_list = dest_zfs.list_from_builder(list_builder.clone().with_dataset(dest_dataset))
            .expect("dst list failed");


        fn to_createtxg_set(list_vecs: Vec<Vec<String>>) -> BTreeMap<String, (String, )>
        {
            let mut list_map: BTreeMap<String, (String,)> = BTreeMap::default();

            for mut e in list_vecs.into_iter() {
                let createtxg = e.pop().unwrap();
                let name = e.pop().unwrap();
                match list_map.insert(createtxg.clone(), (name.clone(),)) {
                    Some(x) => println!("duplicate createtxg: {} {} {}", createtxg, name, x.0),
                    None => {}
                }
            }

            list_map
        }

        let src_map = to_createtxg_set(From::from(&src_list));
        let dst_map = to_createtxg_set(From::from(&dst_list));

        // Find the most recent createtxg (highest number?) in common between the two
        // Find all snapshots in src with createtxgs after the common createtxg
        // Send the found snapshots in order. (`send -I`? may not work with bookmarks).
        // XXX: consider if we should or can also send bookmarks.

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
