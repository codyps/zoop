
#[macro_use]
extern crate clap;

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

        // for dataset, find the common base snapshot
        // for each snapshot after the common base
        //  send it snap
        //  common base = sent snap
        //  repeat until all snaps in src are in dest

    } else {
        println!("need a SubCommand");
    }
}
