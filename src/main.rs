// bookmark contents:
//  NAME                                              PROPERTY              VALUE                  SOURCE
//  innerpool/data/home#znap_2019-06-01-0400_monthly  type                  bookmark               -
//  innerpool/data/home#znap_2019-06-01-0400_monthly  creation              Sat Jun  1  0:00 2019  -
//  innerpool/data/home#znap_2019-06-01-0400_monthly  createtxg             8405881                -
//  innerpool/data/home#znap_2019-06-01-0400_monthly  guid                  8242301612637477726    -
//
// guid, createtxg, and creation match the snapshot's properties of the same name
//
// snapshot after send|recv has a different:
//  createtxg 
//  objsetid
// and same:
//  guid
//  (and everything else)


#[macro_use]
extern crate clap;

extern crate zfs_cmd_api;
extern crate fmt_extra;

use zfs_cmd_api::Zfs;
use clap::{Arg,SubCommand,AppSettings};
use std::collections::BTreeMap;
use std::borrow::Borrow;
use std::convert::TryFrom;

#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone)]
enum DatasetType {
    Snapshot,
    Bookmark
}

impl TryFrom<&str> for DatasetType {
    type Error = String;

    fn try_from(type_str: &str) -> Result<Self, Self::Error> {
        Ok(match type_str {
            "bookmark" => DatasetType::Bookmark,
            "snapshot" => DatasetType::Snapshot,
            _ => return Err(format!("type {} unrecognized", type_str)),
        })
    }
}

#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone)]
struct SubDataset {
    type_: DatasetType,
    name: String,
    createtxg: CreateTxg,
}

#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone)]
struct GlobalDataset {
    guid: Guid,
    src: SubDataset,
    dst: Option<SubDataset>,
}

#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone)]
struct Dataset {
    guid: Guid,
    ds: SubDataset,
}

#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone)]
struct CreateTxg {
    s: String
}

impl From<String> for CreateTxg {
    fn from(s: String) -> Self {
        CreateTxg { s: s }
    }
}

impl From<&str> for CreateTxg {
    fn from(s: &str) -> Self {
        CreateTxg { s: s.to_owned() }
    }
}

impl Borrow<str> for CreateTxg {
    fn borrow(&self) -> &str {
        &self.s
    }
}

impl AsRef<str> for CreateTxg {
    fn as_ref(&self) -> &str {
        &self.s
    }
}

#[derive(Debug,PartialEq,Eq,PartialOrd,Ord,Hash,Clone)]
struct Guid {
    s: String
}

impl From<&str> for Guid {
    fn from(s: &str) -> Self {
        Guid { s: s.to_owned() }
    }
}

impl From<String> for Guid {
    fn from(s: String) -> Self {
        Guid { s: s }
    }
}

impl Borrow<str> for Guid {
    fn borrow(&self) -> &str {
        &self.s
    }
}

impl AsRef<str> for Guid {
    fn as_ref(&self) -> &str {
        &self.s
    }
}

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
        list_builder.include_snapshots()
            .depth(1)
            .with_elements(vec!["createtxg", "name", "guid", "type"]);

        // - dest will contain some or none of snapshots/bookmarks in source
        // - dest may have previously recieved but later deleted some snapshots
        // - src may have previously sent but later deleted some snapshots. Some of these may be
        //   preserved as bookmarks.
        //
        // for each snapshot in source, we want to send it to dest
        // 
        // simple algorithm:
        //   - generate 2 ordered lists of snapshots (excluding bookmarks). order using createtxg
        //   - use guids to merge the 2 lists into a single list, with 3 markers:
        //     - in both, only in src, only in dest
        //   - iterate through the merged list, generating a `send` for each pair of elements,
        //     excluding those pairs where the second pair exists in both src & dest
        //
        // Problems:
        //  - need to transfer and manage entire list of snapshots. Will this grow too large?


        // NOTE: we only get bookmarks for `src` because they aren't useful on the dst
        //
        // XXX: we could optimize the zfs-cmd-to-zoop datapassing by not asking for "type" in
        // dst_list, but it's easier to include it.
        let src_list =
            src_zfs.list_from_builder(list_builder.clone().include_bookmarks().with_dataset(src_dataset))
            .expect("src list failed");
        let dst_list = dest_zfs.list_from_builder(list_builder.clone().with_dataset(dest_dataset))
            .expect("dst list failed");


        fn to_datasets(list_vecs: Vec<Vec<String>>) ->
            Vec<Dataset>
        {
            let mut dss = Vec::new();

            for mut e in list_vecs.into_iter() {
                let createtxg = e.pop().unwrap();
                let name = e.pop().unwrap();
                let guid = e.pop().unwrap();
                let type_= e.pop().unwrap();

                let ds = Dataset {
                    guid: Guid::from(guid),
                    ds: SubDataset {
                        type_: DatasetType::try_from(type_).unwrap(),
                        name: name.to_owned(),
                        createtxg: CreateTxg::from(createtxg),
                    }
                };

                dss.append(ds)
            }

            dss
        }


        // TODO: determine if dataset has a partial receive, and resume it before proceeding with
        // normal incrimental send

        let src_dss = to_datasets(From::from(&src_list));
        let dst_dss = to_datasets(From::from(&dst_list));


        let mut dst_guid_map: BTreeMap<Guid, Dataset> = BTreeMap::default();
        dst_guid_map.extend(dst_dss.drain());


        let mut merged_dss: BTreeMap<CreateTxg, GlobalDataset> = BTreeMap::default();

        // datasets have guids that persist across send/recv. Use this to identify common
        // elements. guids identify snapshots across pools.
        // merge sets by guid
        //
        // note that bookmarks have a guid equal to the snapshot they were created from
        //
        // for the purposes of syncing src to dst, dst datasets that don't exist in any form on src
        // are irrelevent, so we discard them (they are the items remaining in dst_dss)
        // 
        // merged_dss is ordered by (createtxg, guid). We really just want createtxg. Using the
        // guid lets us avoid having a multimap (a single createtxg may have multiple snaps).
        //
        //  XXX: consider the case where multiple snaps exist in the same createtxg. Is it useful
        //  to have further ordering? Should we include the timestamp here?
        for src_ds in src_dss.drain() {
            let dst = dst_guid_map.remove(&src_ds.guid).map(|x| x.ds);
            let k = (src_ds.createtxg.clone(), src_ds.guid.clone());
            match merged_dss.insert(k,
                    GlobalDataset {
                        guid: src_ds.guid,
                        src: src_ds.ds,
                        dst: dst,
                    }
                ){
                Some(x) => {
                    // continue in a duplicate key case, but warn. This should never happen due to
                    // our use of the guid as a piece of the key.
                    eprintln!("WARNING: duplicate key: {:?}", x)
                },
                None => {},
            }
        }

        // iterate over merged_dss
        //  - send incrimental for each GlobalDataset where `dst` is None and the src type is not a
        //    bookmark
        //
        //  - Use the previous GlobalDataset as the basis for the incremental send

        
        let mut prev_dst_ds = None;
        for ds in merged_dss.drain() {

            match ds.dst {
                None => {
                    if ds.src.type_ == DatasetType::Bookmark {
                        // bookmarks can't be sent, they can only be used as the basis for an
                        // incirmental send. skip.
                        continue;
                    }

                    // send it
                    //
                    // NEED send|recv (pipe) API in zfs-cmd-api

                    // use as prev after send/recv finishes
                },
                Some(x) => {
                    // no transfer needed
                    // let's just us it as a prev
                }
            }

            prev_dst_ds = Some(ds);
        }


        // XXX: bookmarks on the SRC allow deletion of snapshots while still keeping send
        // efficiency. As a result, we should create bookmarks to identify points we'll want to
        // sync from
        //
        // for dataset, find the common base snapshot
        // for each snapshot after the common base
        //  send it snap
        //  common base = sent snap
        //  repeat until all snaps in src are in dest

    } else {
        println!("need a SubCommand");
    }
}
