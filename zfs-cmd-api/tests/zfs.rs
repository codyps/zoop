extern crate zfs_cmd_api as zfs;

#[test]
fn zfs() {
    let _ = zfs::Zfs::default();
}

#[test]
fn zfs_list() {
    let zfs = zfs::Zfs::default();
    let _ = zfs.list_basic().expect("list failed");
}
