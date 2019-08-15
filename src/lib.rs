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


#[macro_use] extern crate log;
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


    // determine if dataset has a partial receive, and resume it before proceeding with
    // normal incrimental send
    //
    // XXX: unclear what interaction (if any) this has with the destination having newer snapshots
    //    that aren't present on the source (ie: if snapshot creation is enabled on the dest)
    //    possible that we need to catch errors here, trigger snapshot deletion, and re-attempt
    //    resume. Does zfs allow creation of a snapshot while a resume token exists?
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

    // # `zfs-snapshot-recv-order`:
    //
    // zfs requires that snapshots be recv'd in the order they were created. In other words, if
    // the destination lacks _intermediate_ snapshots found in the source, the destination will be
    // unable to recv them without first deleting the snapshots which follow the missing snapshot.
    //
    // because of this, we have a few options:
    //  1. do not attempt to transfer _intermediate_ snapshots on the source to the destination
    //  2. perform a _lot_ of rollback, potentially losing data in the destination (by deleting
    //    snapshots), and then exactly replicate the snapshots present in the source to the dest.
    //  3. somehow recreate the dataset incrimentally using `dest` resources. unclear if this is
    //     possible by using clones, etc. might be necessary to use multiple send/recvs which would
    //     result in duplicate space usage while reconstruction is occuring.
    //
    // Issues to consider:
    //
    //  - our goal is to preserve the most data. deleting data ourselves (by rollback without
    //    keeping old data, ie #2) feels like it goes against this.
    //
    //  - skipping transfering data from the source (#1) also seems somewhat against this, but less so.
    //    Here we can consider that in typical operation, the case where this occurs (missing
    //    intermediates on the dest) it meanas that we've purposfully trimmed the snapshots on the
    //    destination, and no longer care to preserve them.
    //
    //  - that said, it would be useful to allow a "fill-in" type operation for the case where a
    //    user error occurs and they desire to restore their deleted snapshots
    //
    //  - on #3: clones, when promoted, gain the snapshots of their parent that include/proceed
    //    their source snapshot. The parent keeps all snapshots that follow the source snapshot.
    //


    // "kinds" of snapshots
    //
    //  A. exist on src and dest
    //  B. only in dest with none of kind `A` _following_
    //  C. only in dest with some of kind `A` _following_
    //  D. only in src with none of kind `A` _following_
    //  E. only in src with some of kind `A` _following_
    //
    //  `B` _can_ exist when snapshotting is performed on dest. If `B` exists, they can prevent
    //  recv of addition snapshots from src to dest. Having a way to prevent backups from
    //  continuing is bad, and it is desirable to prevent it.
    //
    //  `B` _can_ also exist when the source deletes some snapshots that were previously sent. We
    //  _don't_ want to delete these on dest, as this would push the source retention policy onto
    //  dest in some cases. dest should always be able to manage it's own retention policy.
    //
    //  `B` case 2 (source deletes snapshots):
    //     - src: `@a`, `@b`, `@c`.
    //     - send `@a` to dest.
    //     - send `@b` to dest with `@a` as base.
    //     - delete `@b` on src
    //     - send `@c` to dest with `@a` as base.
    //
    //  Results in:
    //      - with `-F`: dest: `@a`, `@c`
    //      - without `-F`: recv fails, dest: `@a`, `@b`
    //
    //  `B` case 1 (dest creates snapshots):
    //     - src: `@a`, `@b`.
    //     - send `@a` to dest.
    //     - create `@bp` on dest.
    //     - send `@b` to dest with `@a` as base.
    //
    //  Results in:
    //   - with `-F`: dest: `@a`, `@b`
    //   - without `-F`: recv fails, dest: `@a`, `@bp`
    //
    //
    // `C` _can_ exist when there is a snapshot deletion policy on the src that removes some
    // snapshots. These are fine and have no effect on our transfers.
    //
    // `D` _can_ exits when the source creates snapshots. This is totally normal and needs no
    // special handling.
    //
    // `E` _can_ exist when there is a snapshot deletion policy on the dest that removes some
    // snapshots, which the src preserves those same snapshots.
    //
    // `E` case (dest removes snapshots)
    //    - src: `@a`, `@b`, `@c`
    //    - send `@a` to dest.
    //    - send `@b` to dest with `@a` as base.
    //    - send `@c` to dest with `@b` as base.
    //    - delete `@b` on dest
    //    - send `@b` to dest with `@a` as base.
    //
    // Results in:
    //  - recv fails, "cannot receive incremental stream: destination %s has been modified since
    //    most recent snapshot"
    //  - discussed in `zfs-snapshot-recv-order` above.
    //
    // Solutions to `B`:
    //  a. always `recv -F`, causing _both_ the source deleted & dest created snapshots to be lost
    //  b. refuse to transfer in this case, causing source deleted & dest created snapshots to be
    //    preserved, but all backups stop.
    //  c. perform some fancy clone/promote to preserve snapshots (?)
    //
    // We currently do `a`. It may make sense to optionally allow `b` (easy). Figuring out `c` may
    // be difficult.
    // 
    // Solutions to `E`:
    //  - see `zfs-snapshot-recv-order`


    // because of zfs recv ordering requirements, we currently skip over source snapshots that
    // would be intermediate snapshots on the destination.
    //
    // we do this with a pre-processing of merged_dss discarding intermediate-in-dest items
    let mut dss_iter = merged_dss.iter().rev();
    let mut basis_key: Option<(CreateTxg,Guid)> = None;
    loop {
        // iterate backwards over `merged_dss`
        // track when we first hit snapshot that exists in both src & dest
        // after that, terminate when we hit a snapshot that only exists in src
        // only consider the snapshots we've examined for transfer
        match dss_iter.next() {
            Some((k, ds)) => {
                if ds.dst.is_some() {
                    info!("basis found: {:?}", ds);
                    basis_key = Some(k.clone());
                    break;
                } else {
                    trace!("not a basis: {:?}", ds);
                }
            },
            None => {
                info!("no basis found");
                break;
            }
        }
    }

    // iterate over merged_dss
    //  - send incrimental for each GlobalDataset where `dst` is None and the src type is not a
    //    bookmark
    //
    //  - Use the previous GlobalDataset as the basis for the incremental send
    // XXX: ideally, we'd use `dss_iter.rev()` or similar here and just iterate back over the items.
    // Unfortunately, rust's iterators are more like "streams" or "queues": once an element is
    // emitted, it is never emitted again. "cursor"/"marker" iterators have been discussed at a few
    // points [1][2][3], but no options appear presently avaliable on crates.io. So instead we
    // perform an addition BTree lookup.
    //
    // 1: https://codeburst.io/my-journey-with-rust-in-2017-the-good-the-bad-the-weird-f07aa918f4f8?gi=cf14badb5d6c
    // 2: https://stackoverflow.com/questions/38227722/iterating-forward-and-backward
    // 3: https://internals.rust-lang.org/t/pseudo-rfc-cursors-reversible-iterators/386/5
    let dss_iter = match basis_key {
        Some(k) => merged_dss.range(k..),
        None => merged_dss.range(..),
    };

    let mut prev_dst_ds: Option<String> = None;
    for (_, ds) in dss_iter {
        if opts.verbose {
            eprintln!("examine: {:?}", ds);
        }

        match &ds.dst {
            None => {
                if ds.src.type_ == DatasetType::Bookmark {
                    // bookmarks can't be sent, they can only be used as the basis for an
                    // incirmental send.
                    //
                    // This one can't be used as the basis for an incrimental send because the
                    // destination doesn't have the corresponding snapshot.
                    //
                    // skip.
                    continue;
                }

                // send it
                let send = src_zfs.send(&ds.src.name[..], prev_dst_ds.as_ref().map(|x| &**x), send_flags).unwrap();
                let recv = dest_zfs.recv(dest_dataset, &[], None, &[], recv_flags).unwrap();

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
    // discarded snapshots that follow the rollback target. This is basically an issue of
    // `zfs-snapshot-recv-order` caused by zfs's linear history requirement.
}
