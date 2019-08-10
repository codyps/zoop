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


extern crate zfs_cmd_api;
extern crate fmt_extra;
extern crate enumflags2;

use enumflags2::BitFlags;
use zfs_cmd_api::{Zfs, ZfsError, ZfsList};
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
            _ => return Err(format!("DatasetType unrecognized: {}", type_str)),
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

/*
struct DrainBTreeMap<K, V> {
    tree: BTreeMap<K, V>,
    iter: std::collections::btree_map::IterMut<K, V>
}

trait ExtendBTreeMap {
    fn drain(&mut self) ->
}

impl<T> Iterator for DrainBTreeMap<T> {
    type Item = T;
    fn next(&mut self) -> Item {
        self.
    }
}
*/

#[derive(Debug)]
pub struct ZcopyOpts {
    pub dry_run: bool,
    pub verbose: bool,
    pub resumable: bool,
}

impl Default for ZcopyOpts {
    fn default() -> Self {
        Self {
            dry_run: false,
            verbose: false,
            resumable: true,
        }
    }
}

pub fn zcopy_recursive(src_zfs: &Zfs, dest_zfs: &Zfs, opts: &ZcopyOpts, src_dataset: &str, dest_dataset: &str)
{
    // XXX: consider if it would be useful to obtain additional info other than name here.
    // XXX: should we match up these src filesystems with dest filesystems?
    let mut enum_ds = zfs_cmd_api::ListBuilder::default();
    enum_ds.include_filesystems()
        .recursive()
        .with_elements(&["name"])
        .with_dataset(src_dataset);

    let dss = src_zfs.list_from_builder(&enum_ds)
        .expect("could not enumerate decendent filesystems");

    // XXX: consider the ordering of this iteration
    for this_src_ds in dss.iter() {
        // XXX: consider if ds names are allowed to be non-utf8
        let this_src_ds = std::str::from_utf8(this_src_ds).unwrap();

        // form a `dest` based on `src_dataset`, `ds`, and `dest_dataset`
        // basically: remove the `src_dataset` prefix on `ds` and append it to `dest_dataset`
        assert!(this_src_ds.starts_with(src_dataset));

        let ds_suffix = &this_src_ds[src_dataset.len()..];
        let this_dest_ds = format!("{}{}", dest_dataset, ds_suffix);

        eprintln!("zcopy: {} to {}", this_src_ds, this_dest_ds);

        zcopy_one(src_zfs, dest_zfs, opts, this_src_ds, this_dest_ds.as_ref());
    }
}

pub fn zcopy_one(src_zfs: &Zfs, dest_zfs: &Zfs, opts: &ZcopyOpts, src_dataset: &str, dest_dataset: &str)
{
    let mut get_receive_resume_token = zfs_cmd_api::ListBuilder::default();
    get_receive_resume_token.include_filesystems()
        .with_elements(&["receive_resume_token"])
        .with_dataset(dest_dataset);

    let mut list_builder = zfs_cmd_api::ListBuilder::default();
    list_builder.include_snapshots()
        .depth(1)
        .with_elements(&["createtxg", "name", "guid", "type"]);

    // flags for both resume and normal send/recv
    let mut send_flags = zfs_cmd_api::SendFlags::EmbedData
        | zfs_cmd_api::SendFlags::Compressed
        | zfs_cmd_api::SendFlags::LargeBlock
        | zfs_cmd_api::SendFlags::Raw;
    let mut recv_flags = BitFlags::default() | zfs_cmd_api::RecvFlags::Force;
    if opts.resumable {
        recv_flags |= zfs_cmd_api::RecvFlags::Resumeable;
    }

    if opts.dry_run {
        // XXX: consider performing a send dry run (optionally) instead, omitting
        // the recv altogether.
        recv_flags |= zfs_cmd_api::RecvFlags::DryRun;
    }
    if opts.verbose {
        recv_flags |= zfs_cmd_api::RecvFlags::Verbose;
        send_flags |= zfs_cmd_api::SendFlags::Verbose;
    }

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

    if opts.resumable {
        // check for resume, and resume before doing the rest of our work
        match dest_zfs.list_from_builder(&get_receive_resume_token) {
            Ok(v) => {
                let res: Vec<Vec<String>> = From::from(&v);
                assert_eq!(res.len(), 1);
                let res = &res[0];
                assert_eq!(res.len(), 1);
                let res = &res[0];
                if res == "-" {
                    // nada
                    if opts.verbose {
                        eprintln!("No recv resume, skip to normal transfer");
                    }
                } else {
                    eprintln!("Resuming partial recv in {}", dest_dataset);

                    let send = src_zfs.send_resume(res, send_flags).unwrap();
                    let recv = dest_zfs.recv(dest_dataset, &[], None, &[], recv_flags).unwrap();

                    // neither src nor dst are encrypted, but the error:
                    // > zoop[102113]: cannot receive incremental stream:
                    // > incompatible embedded data stream feature with encrypted receive
                    // is emitted during a recv of a resumed send.
                    //
                    // seems plausible that we've got something not-quite-right going on.
                    zfs_cmd_api::send_recv(send, recv).unwrap();
                }
            },
            Err(ZfsError::NoDataset{..}) => {
                if opts.verbose {
                    eprintln!("filesystem {} does not exist, not resuming", dest_dataset);
                }
            },
            Err(e) => panic!("dst list failed: {}", e),
        }
    }

    // NOTE: we only get bookmarks for `src` because they aren't useful on the dst
    //
    // XXX: we could optimize the zfs-cmd-to-zoop datapassing by not asking for "type" in
    // dst_list, but it's easier to include it.
    let src_list =
        src_zfs.list_from_builder(list_builder.clone().include_bookmarks().with_dataset(src_dataset))
        .expect("src list failed");
    let dst_list = match dest_zfs.list_from_builder(list_builder.clone().with_dataset(dest_dataset)) {
        Ok(v) => v,
        Err(ZfsError::NoDataset{..}) => ZfsList::default(),
        Err(e) => panic!("dst list failed: {}", e),
    };

    fn to_datasets(list_vecs: Vec<Vec<String>>) ->
        Vec<Dataset>
    {
        let mut dss = Vec::with_capacity(list_vecs.len());

        for mut e in list_vecs.into_iter() {
            let mut e = e.drain(..);
            let createtxg = e.next().unwrap();
            let name = e.next().unwrap();
            let guid = e.next().unwrap();
            let type_= e.next().unwrap();

            let ds = Dataset {
                guid: Guid::from(guid.clone()),
                ds: SubDataset {
                    type_: DatasetType::try_from(&type_[..]).unwrap_or_else(|v| {
                        eprintln!("zfs entry: {:?} {:?} {:?} {:?}",
                                  createtxg, name, guid, type_);

                        panic!("{:?}", v);
                    }),
                    name: name.to_owned(),
                    createtxg: CreateTxg::from(createtxg),
                }
            };

            dss.push(ds)
        }

        dss
    }


    // TODO: determine if dataset has a partial receive, and resume it before proceeding with
    // normal incrimental send

    let src_dss = to_datasets(From::from(&src_list));
    let dst_dss = to_datasets(From::from(&dst_list));


    let mut dst_guid_map: BTreeMap<Guid, Dataset> = BTreeMap::default();
    dst_guid_map.extend(dst_dss.into_iter().map(|v| (v.guid.clone(), v)));


    let mut merged_dss: BTreeMap<(CreateTxg, Guid), GlobalDataset> = BTreeMap::default();

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
    for src_ds in src_dss.into_iter() {
        let dst = dst_guid_map.remove(&src_ds.guid).map(|x| x.ds);
        let k = (src_ds.ds.createtxg.clone(), src_ds.guid.clone());
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
    
    let mut prev_dst_ds: Option<String> = None;
    for (_, ds) in merged_dss.into_iter() {
        if opts.verbose {
            eprintln!("examine: {:?}", ds);
        }

        match &ds.dst {
            None => {
                if ds.src.type_ == DatasetType::Bookmark {
                    // bookmarks can't be sent, they can only be used as the basis for an
                    // incirmental send. skip.
                    continue;
                }

                // send it
                //
                // NEED send|recv (pipe) API in zfs-cmd-api

                let send = src_zfs.send(&ds.src.name[..], prev_dst_ds.as_ref().map(|x| &**x), send_flags).unwrap();
                let recv = dest_zfs.recv(dest_dataset, Vec::new(), None, Vec::new(), recv_flags).unwrap();

                zfs_cmd_api::send_recv(send, recv).unwrap();

                // use as prev after send/recv finishes
            },
            Some(_) => {
                // no transfer needed
                // let's just us it as a prev
            }
        }

        prev_dst_ds = Some(ds.src.name.clone());
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

    // XXX: we currently invoke `zfs` for each snapshot transfer. Using `-I` might allow us to
    // reduce the number of invocations.
    //
    // XXX: consider if we're essentially implimenting `zfs send -I`
    //
    // XXX: consider if our snapshot ordering is still acurate/workable in the face of `zfs rollback` usage.
    // when rollback is used on the src, it will lose more recent snapshots/bookmarks, but the
    // dst keeps them. Because zcopy's ordering is based on the createtxg on the src, and
    // because incrimentals are generated only using src datasets, the zcopy is expected to
    // generate a totally reasonable and normal incrimental send. On the dst side, it will have
    // a "graph" of snapshots. Trying to generate a recv from there may not work. Basically: we
    // would assume the wrong parent snapshot of the new snapshots (sent after a src rollback)
    // because we treat snapshots as a linear history, and rollback makes it non-linear.
    //
    // It's not entirely clear if this can be resolved. We might be able to repeatedly ask for
    // the sizes of incrimental snapshots from dst for various origin datasets, and select the
    // smallest one (this may be useful in general for reducing space usage).

}
