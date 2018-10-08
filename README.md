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
  - Uses `zfs send -i`, no used of `-I`. Commands:
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

# Various references related to zfs bookmarks & send

 - http://open-zfs.org/wiki/Documentation/ZfsSend
 - https://www.reddit.com/r/zfs/comments/5op68q/can_anyone_here_explain_zfs_bookmarks/
 - https://utcc.utoronto.ca/~cks/space/blog/solaris/ZFSBookmarksWhatFor
