
#[macro_use]
extern crate clap;

use clap::{Arg,SubCommand};

// hack to try to get `app_from_crate!()` to regenerate.
#[allow(dead_code)]
const CARGO_TOML: &'static str = include_str!("../Cargo.toml");

fn main() {

    let matches = app_from_crate!()
        .subcommand(SubCommand::with_name("zcopy")
            .arg(Arg::with_name("recursive")
                 .help("Also examine datasets the decend from the specified dataset")
                 .short("R")
                 .takes_value(false)
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


    if let Some(matches) = matches.subcommand_matches("zcopy") {
        let src_dataset = matches.value_of("SRC_DATASET").unwrap();
        let dest_dataset = matches.value_of("DEST_DATASET").unwrap();

        let recursive = matches.occurrences_of("recusrive") > 0;

        println!("copy from {} to {} (recursive={})", src_dataset, dest_dataset, recursive); 

        // for dataset, find the common base snapshot
        // for each snapshot after the common base
        //  send it snap
        //  common base = sent snap
        //  repeat until all snaps in src are in dest

    } else {
        println!("need a SubCommand");
    }
}
