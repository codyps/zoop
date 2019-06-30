# Zoop

Manage zfs snapshot transfer


# zfs snapshot & sync managment tools

 - sanoid
   - https://github.com/jimsalterjrs/sanoid
   - `syncoid`: zfs send, uses `-I` and/or `-i`. Supports resume.
   - no use of zfs bookmarks.
   - creation/deletion policy?
 - zxfer
   - unhappy if snapshots that were transfered to destiation are later deleted
    on source. requires one delete them on the destination before it will sync.
   - Uses `zfs send -i`, no use of `-I`. Commands:
     - `send -nPv`
     - `send -nv`
     - `receive`, `receive -F`
     - `send -nPv -i`
     - `send -i`
     - `list -t filesystem,volume -Ho name -s creation`
     - `list -Hr -o name -s creation -t snapshot`
     - `list -Hr -o name -S creation -t snapshot`
     - `list -t filesystem,volume -Hr -o name`
   - no bookmark handling at all (only works with snapshots)
 - [zrep](http://www.bolthole.com/solaris/zrep/)
   - replication and failover
   - ksh
 - [z3](https://github.com/presslabs/z3/)
   - send zfs snapshots to s3
 - [ZnapZend](https://www.znapzend.org/)
 - [zfsnap](https://www.zfsnap.org/)
 - [pyznap](https://github.com/yboetz/pyznap)

# Various references related to zfs bookmarks & send

 - http://open-zfs.org/wiki/Documentation/ZfsSend
 - https://www.reddit.com/r/zfs/comments/5op68q/can_anyone_here_explain_zfs_bookmarks/
 - https://utcc.utoronto.ca/~cks/space/blog/solaris/ZFSBookmarksWhatFor


# Snapshot Name Styles

 - zfs-auto-snapshot vanilla
   - `<prefix>_<label>_YYYY-MM-DD-HHmm`
     - `<prefix>` is typically `zfs-auto-snap`
     - `<label>` is typically one of `frequent`, `hourly`,
       `daily`, `monthly`, `weekly`.
 - zfs-auto-snapshot sort-mod
   - `<prefix>_YYYY-MM-DD-HHmm_<label>`
     - `<prefix>` is typically `znap`
     - `<label>` is typically one of `frequent`, `hourly`,
		  `daily`, `monthly`, `weekly`.

# Snapshot Policy

 - zfs-auto-snapshot
   - count per category
     - frequent (15m): 4
     - hourly: 24
     - daily: 31
     - weekly: 8
     - monthly: 12
 - Mac Timemachine
   - creation
     - unspecified number/rate of "local" snapshots
     - hourly: 24
     - daily: 31
     - weekly: as many as space permits
   - deletion
     - oldest first
 - snapper
   - time based
     - same "limit per period" that zfs-auto-snapshot & TimeMachine use. Unlike
       zfs-auto-snapshot, it dynamically decides which grouping a snap belongs
       to (they aren't explicitly tagged as daily, etc)
   - boot based
   - before/after
   
