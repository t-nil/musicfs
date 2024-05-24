use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::iter;
use std::ops::Not as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Error, Ok, Result};
use fuser::{FileAttr, Filesystem};
use itertools::Itertools;
use libc::{c_int, EBADFD, EIO, ENOENT, ENOSYS, ENOTDIR, EPERM};
use log::{debug, error, warn};
use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use walkdir::WalkDir;

use crate::fs::types::DirEntry;
use crate::media::{is_media_file_by_name, MusicFile};

use self::tree::{Node, Tree};
use self::types::{Fd, FileType, Inode};

const TTL: Duration = Duration::from_secs(1); // 1 second
const BLOCK_SIZE: u32 = 512;
const DIR_ATTR_SIZE: u64 = 0;
const DIR_ATTR_BLOCKS: u64 = 0;
const DIR_ATTR_PERMS: u16 = 0o755;
const DIR_ATTR_NLINK: u32 = 2; // a directory as atleast two hardlinks
const FILE_ATTR_PERMS: u16 = 0o644;
const FILE_ATTR_NLINK: u32 = 1; // a file usually has one hardlink
static STARTUP_TIME: Lazy<std::time::SystemTime> = Lazy::new(|| std::time::SystemTime::now());

type VPath = HashMap<PathBuf, Arc<MusicFile>>;
type Inodes = Vec<PathBuf>;

// TODO make more of the actual implementation immutable, for safety
#[derive(Debug, Clone)]
pub struct MusicFS {
    root: PathBuf,
    source_files: Vec<Arc<MusicFile>>,
    tree: Tree,
    fd_table: HashMap<u64, Fd>,
    fd_index: u64,
}

mod tree {
    use std::{
        collections::{HashMap, HashSet, VecDeque},
        path::Iter,
        sync::Arc,
    };

    use derive_getters::Getters;
    use log::error;

    use crate::media::MusicFile;

    use super::types::Inode;

    #[derive(Debug, Clone)]
    pub enum NodeType {
        Directory(Vec<Node>),
        File(Arc<MusicFile>),
    }

    #[derive(Debug, Clone, Getters)]
    pub struct Node {
        inode: Inode,
        name: String,
        file_type: NodeType,
    }

    #[derive(Debug, Clone, Getters)]
    pub struct Tree {
        root: Node,
    }

    impl Tree {
        pub fn new(root: Node) -> Self {
            Self::validate(&root);
            Self { root }
        }

        fn validate(root: &Node) {
            check_unique_inodes(&root);

            fn check_unique_inodes(root: &Node) {
                let mut inodes = HashSet::<i64>::new();
                let mut tree_iter = TreeIterator::new(root);
                let mut failed = false;
                for node in tree_iter {
                    if inodes.contains(&node.inode) {
                        error!("Duplicate Inodes: {node:?}");
                        failed = true;
                    }
                }

                if failed {
                    panic!("Duplicate inodes found");
                }
            }
        }

        pub fn iter<'a>(&'a self) -> TreeIterator<'a> {
            TreeIterator::new(&self.root)
        }
    }

    struct TreeIterator<'a> {
        queue: VecDeque<&'a Node>,
    }

    impl<'a> TreeIterator<'a> {
        fn new(root: &'a Node) -> Self {
            fn collect_nodes(node: &Node) -> Vec<&Node> {
                match &node.file_type {
                    NodeType::Directory(children) => {
                        children.iter().flat_map(collect_nodes).collect()
                    }
                    NodeType::File(_) => vec![node],
                }
            }
            Self {
                queue: VecDeque::from(collect_nodes(root)),
            }
        }
    }

    impl<'a> Iterator for TreeIterator<'a> {
        type Item = &'a Node;

        fn next(&mut self) -> Option<Self::Item> {
            self.queue.pop_front()
        }
    }
}

mod types {
    use std::{
        ffi::{OsStr, OsString},
        path::PathBuf,
        sync::Arc,
    };

    use derive_more::{Display, Into};

    use crate::media::MusicFile;

    use super::PathLookup;
    #[derive(Debug, Clone, Copy, Display, PartialEq, Eq, PartialOrd, Ord, Into)]
    pub struct Inode(usize);

    impl Inode {
        pub fn new(inode: usize, inode_table: &Vec<PathBuf>) -> Option<Self> {
            if inode_table.len() > inode {
                Some(Self(inode))
            } else {
                None
            }
        }

        pub fn from_u64(inode: u64, inode_table: &Vec<PathBuf>) -> Option<Self> {
            let inode = inode
                .try_into()
                .expect("we got a problem: inode (u64) doesn't fit into usize");
            Self::new(inode, inode_table)
        }
    }

    impl From<Inode> for u64 {
        fn from(value: Inode) -> Self {
            value.0 as u64
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum FileType {
        File(Arc<MusicFile>),
        Directory(PathBuf),
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct DirEntry {
        /// last piece of path
        pub name: OsString,
        pub inode: Inode,
        pub file_type: FileType,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Fd {
        File {
            path: PathBuf,
            inode: Inode,
            offset: usize,
        },
        OpenDir {
            path: PathBuf,
            content: Vec<DirEntry>,
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathLookup {
    Found(FileType),
    NotFound,
    NonexistentVPath,
    MalformedPath,
}

enum Lookup<'a> {
    InodeNotFound,
    ChildNotFound,
    Found(&'a Node),
}

pub fn lookup<'a>(tree: &'a Tree, inode: Inode, name: &str) -> Option<&'a Node> {
    tree.iter().find(|node| node.inode() == inode).
}

impl MusicFS {
    pub fn from_dir(root: impl AsRef<Path>) -> (Self, Vec<Error>) {
        let files_matching_ext = WalkDir::new(&root)
            .same_file_system(true)
            .into_iter()
            .filter_ok(|file| is_media_file_by_name(file.path()))
            .collect_vec();

        let probed_files = files_matching_ext
            .iter()
            .map(Result::as_ref)
            .map_ok(walkdir::DirEntry::path)
            .map_ok(MusicFile::parse_with_symphonia)
            .flatten()
            .map_ok(Arc::new);

        let (source_files, errors): (Vec<Arc<MusicFile>>, Vec<Error>) =
            probed_files.partition_result();

        (
            MusicFS {
                source_files,
                root: root.as_ref().to_path_buf(),
                tree: Tree,
                fd_table: Default::default(),
                fd_index: 0,
            },
            errors,
        )
    }

    pub fn add_vpath(&mut self, name: &str, template: &str) -> Result<()> {
        ensure!(
            self.vpath_table.contains_key(name).not(),
            "vpath {name} already taken"
        );
        self.vpath_table.insert(name.to_owned(), HashMap::new());
        let paths = self
            .vpath_table
            .get_mut(name)
            .expect("just inserted that key ~_~");

        for music_file in self.source_files.iter() {
            let mapped_path = music_file
                .instantiate_template(template)
                .with_context(|| format!("mapping {template} to {name}"))?;
            debug!(
                "{mapped_path:?} => {music_file:?}",
                music_file = music_file.path()
            );
            if let Some(old_file) = paths.insert(mapped_path.clone(), music_file.clone()) {
                bail!(
                    "Path collision ({old_file:?} and {music_file:?} both map to {mapped_path:?}",
                    old_file = old_file.path(),
                    music_file = music_file.path()
                );
            }
        }

        Ok(())
    }

    fn create_inodes(&mut self) {
        let mut inodes = HashSet::<PathBuf>::new();
        let capacity = self
            .vpath_table
            .iter()
            .map(|(_, map)| map.len())
            .sum::<usize>();
        inodes.reserve(capacity);

        for (name, paths) in self.vpath_table.iter() {
            for (path, _) in paths {
                let path = PathBuf::new().join(name).join(path);
                inodes.extend(path.ancestors().map(Path::to_path_buf));
            }
        }

        // "" (empty path) is swimming around somewhere there, let's remove it (it would get put anywhere but at inode 1)
        inodes.remove(&PathBuf::from(""));

        self.inode_table = ["", ""].iter().map(PathBuf::from).collect_vec();
        self.inode_table.extend(inodes);

        debug!("INODES: {:#?}", self.inode_table);
    }

    fn inode_to_path(&self, inode: u64) -> Option<impl AsRef<Path> + '_> {
        self.inode_table.get(
            usize::try_from(inode).expect("we got a problem: inode (u64) doesn't fit into usize"),
        )
    }

    fn path_to_inode(&self, path: impl AsRef<Path>) -> Option<Inode> {
        let path = path.as_ref();
        self.inode_table
            .iter()
            .position(|p| p == path)
            .and_then(|inode| Inode::new(inode, &self.inode_table))
    }

    fn lookup_path(&self, target_path: impl AsRef<Path>) -> PathLookup {
        let target_path = target_path.as_ref();

        if target_path == Path::new("") {
            // we found root
            return PathLookup::Found(FileType::Directory(target_path.to_path_buf()));
        }

        // no component -> malformed
        let Some(vpath_name) = target_path.components().next() else {
            return PathLookup::MalformedPath;
        };

        // vpath name is not UTF-8 -> malformed
        let Some(vpath_name) = vpath_name.as_os_str().to_str() else {
            return PathLookup::MalformedPath;
        };

        // vpath not found in table
        let Some(vpath) = self.vpath_table.get(vpath_name) else {
            return PathLookup::NonexistentVPath;
        };

        // ELSE

        // remove vpath root from path
        // now it's a direct match for the vpath lookup table
        let target_path = target_path.components().skip(1).collect::<PathBuf>();

        if let Some(music_file) = vpath.get(&target_path) {
            return PathLookup::Found(FileType::File(music_file.clone()));
        }
        // we don't need to check if it's a directory, since otherwise
        // it wouldn't be in the inode table. But keeping for sanity.
        //
        // `p` is a child of `target_path` iff p's parent `starts_with` target_path (since starts_with also matches identical paths)
        debug_assert!(vpath.keys().any(|p| p
            .parent()
            .unwrap_or(Path::new(""))
            .starts_with(&target_path)));
        return PathLookup::Found(FileType::Directory(target_path.to_path_buf()));
    }

    // This has to produce a stable ordering for `offset` to work.
    // But `inodes` will never change during runtime, so it should be ok.
    fn list_directory(
        &self,
        inode: Inode,
    ) -> (PathLookup, Option<impl Iterator<Item = DirEntry> + '_>) {
        let Some(path) = self.inode_to_path(inode.into()) else {
            return (PathLookup::NotFound, None);
        };
        let path = path.as_ref();

        let itself = self.lookup_path(path);
        if !(itself == PathLookup::Found(FileType::Directory(path.to_path_buf()))) {
            return (itself, None);
        }
        let self_type = FileType::Directory(path.to_path_buf());

        let real_files = self
            .inode_table
            .iter()
            .enumerate() // carry the real inodes
            .filter({let path = path.to_path_buf(); move |(_, p)| p.parent() == Some(&path)})  // the let + scope syntaxes make the closures capture by clone
            .map({let path = path.to_path_buf(); move |(child_inode, child_path)| {                    // because they are returned and so path would escape its scope
                DirEntry {
                    name: child_path
                        .file_name()
                        .expect("FATAL: entry without file name")
                        .to_os_string(),
                    // TODO could be inode::unsafe_from, since we list above
                    inode: Inode::new(child_inode, &self.inode_table)
                        .expect("Inode not found. Can't be, we just took it from the table."),
                    file_type: match self.lookup_path(child_path){
                        PathLookup::NotFound |
                        PathLookup::NonexistentVPath |
                        PathLookup::MalformedPath => panic!("FATAL: list_directory({inode}): invalid path in inode_table ({child_path:?}, inode {child_inode}, parent {path:?})"), 
                        PathLookup::Found(entry) => entry,
                    },
                }
                /*inode,
                path.file_name().and_then(OsStr::to_str).unwrap_or(".."),
                self.lookup_path(path),*/
            }});
        let mut meta_entries = Vec::<DirEntry>::new();
        meta_entries.push(DirEntry {
            name: ".".into(),
            inode,
            file_type: self_type.clone(),
        });
        match path
            .parent()
            .and_then(|p| self.path_to_inode(p).map(|ino| (p, ino)))
        {
            Some((parent_path, parent_inode)) => meta_entries.push(DirEntry {
                name: "..".into(),
                inode: parent_inode,
                file_type: FileType::Directory(parent_path.to_path_buf()),
            }),

            // either root or (fatal) error
            None => {
                if usize::from(inode) != 1 {
                    panic!("FATAL: list_directory({inode}): {path:?} that matches inode {inode} is not root but has no parent")
                }
            }
        }

        (
            PathLookup::Found(self_type),
            Some(meta_entries.into_iter().chain(real_files)),
        )
    }

    fn new_fd(&mut self) -> u64 {
        let tmp = self.fd_index;
        self.fd_index += 1;
        tmp
    }

    fn _getattr(
        &self,
        ino: u64,
        uid: u32,
        gid: u32,
        file_type: FileType,
    ) -> std::result::Result<FileAttr, libc::c_int> {
        match file_type {
            FileType::Directory(path) => std::result::Result::Ok(FileAttr {
                ino,
                size: DIR_ATTR_SIZE,
                blocks: DIR_ATTR_BLOCKS,
                atime: *STARTUP_TIME,
                mtime: *STARTUP_TIME,
                ctime: *STARTUP_TIME,
                crtime: *STARTUP_TIME,
                kind: fuser::FileType::Directory,
                perm: DIR_ATTR_PERMS,
                nlink: DIR_ATTR_NLINK,
                uid: uid,
                gid: gid,
                rdev: 0,
                blksize: BLOCK_SIZE,
                flags: 0,
            }),
            FileType::File(music_file) => {
                let std::result::Result::Ok(size) =
                    music_file.path().metadata().map(|meta| meta.len())
                else {
                    error!(
                        "getattr(ino: {ino:#x?}): couldn't get file size ({path:?})",
                        path = music_file.path()
                    );
                    return Err(EIO);
                };
                std::result::Result::Ok(FileAttr {
                    ino,
                    size: size,
                    blocks: size.div_ceil(BLOCK_SIZE.into()),
                    atime: *STARTUP_TIME,
                    mtime: *STARTUP_TIME,
                    ctime: *STARTUP_TIME,
                    crtime: *STARTUP_TIME,
                    kind: fuser::FileType::RegularFile,
                    perm: FILE_ATTR_PERMS,
                    nlink: FILE_ATTR_NLINK,
                    uid: uid,
                    gid: gid,
                    rdev: 0,
                    blksize: BLOCK_SIZE,
                    flags: 0,
                })
            }
        }
    }
}

impl Filesystem for MusicFS {
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), c_int> {
        self.create_inodes();
        std::result::Result::Ok(())
    }

    fn destroy(&mut self) {}

    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        debug!("lookup(parent: {:#x?}, name {:?})", parent, name);
        let Some(parent_path) = self.inode_to_path(parent) else {
            warn!(
                "lookup(parent: {:#x?}, name {:?}): parent inode not in inode table",
                parent, name
            );
            reply.error(ENOENT);
            return;
        };
        let path = parent_path.as_ref().join(name);
        let Some(inode) = self.path_to_inode(&path) else {
            warn!(
                "lookup(parent: {:#x?}, name {:?}): path not in inode table",
                parent, name
            );
            reply.error(ENOENT);
            return;
        };
        match self.lookup_path(&path) {
            PathLookup::NotFound | PathLookup::NonexistentVPath | PathLookup::MalformedPath => {
                error!("lookup(iparent: {:#x?}, name {:?}): path {path:?} ref'd by inode is malformed or nonexistent. THIS SHOULDN'T HAPPEN as that means the inode table is inconsistent", parent, name);
                reply.error(ENOENT);
                return;
            }
            PathLookup::Found(file_type) => {
                let attrs = match self._getattr(inode.into(), _req.uid(), _req.gid(), file_type) {
                    std::result::Result::Ok(attrs) => attrs,
                    Err(errno) => {
                        reply.error(errno);
                        return;
                    }
                };
                debug!(
                    "lookup(parent: {:#x?}, name {:?}): {attrs:#?}",
                    parent, name
                );
                reply.entry(&TTL, &attrs, 1);
                return;
            }
        }
    }

    fn forget(&mut self, _req: &fuser::Request<'_>, _ino: u64, _nlookup: u64) {}

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        let Some(path) = self.inode_to_path(ino) else {
            warn!("getattr(ino: {:#x?}): inode not in inode table", ino);
            reply.error(ENOENT);
            return;
        };
        let path = path.as_ref();
        match self.lookup_path(path) {
            PathLookup::NotFound | PathLookup::NonexistentVPath | PathLookup::MalformedPath => {
                error!("getattr(ino: {:#x?}): path {path:?} ref'd by inode is malformed or nonexistent. THIS SHOULDN'T HAPPEN as that means the inode table is inconsistent", ino);
                reply.error(ENOENT);
                return;
            }
            PathLookup::Found(file_type) => {
                let attrs = match self._getattr(ino, _req.uid(), _req.gid(), file_type) {
                    std::result::Result::Ok(attrs) => attrs,
                    Err(errno) => {
                        reply.error(errno);
                        return;
                    }
                };
                reply.attr(&TTL, &attrs);
                return;
            }
        }
        //reply.error(ENOSYS);
    }

    fn setattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        debug!(
            "[Not Implemented] setattr(ino: {:#x?}, mode: {:?}, uid: {:?}, \
            gid: {:?}, size: {:?}, fh: {:?}, flags: {:?})",
            ino, mode, uid, gid, size, fh, flags
        );
        reply.error(ENOSYS);
    }

    fn readlink(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyData) {
        debug!("[Not Implemented] readlink(ino: {:#x?})", ino);
        reply.error(ENOSYS);
    }

    fn mknod(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: fuser::ReplyEntry,
    ) {
        debug!(
            "[Not Implemented] mknod(parent: {:#x?}, name: {:?}, mode: {}, \
            umask: {:#x?}, rdev: {})",
            parent, name, mode, umask, rdev
        );
        reply.error(ENOSYS);
    }

    fn mkdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        debug!(
            "[Not Implemented] mkdir(parent: {:#x?}, name: {:?}, mode: {}, umask: {:#x?})",
            parent, name, mode, umask
        );
        reply.error(ENOSYS);
    }

    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] unlink(parent: {:#x?}, name: {:?})",
            parent, name,
        );
        reply.error(ENOSYS);
    }

    fn rmdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] rmdir(parent: {:#x?}, name: {:?})",
            parent, name,
        );
        reply.error(ENOSYS);
    }

    fn symlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        link_name: &std::ffi::OsStr,
        target: &std::path::Path,
        reply: fuser::ReplyEntry,
    ) {
        debug!(
            "[Not Implemented] symlink(parent: {:#x?}, link_name: {:?}, target: {:?})",
            parent, link_name, target,
        );
        reply.error(EPERM);
    }

    fn rename(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        newparent: u64,
        newname: &std::ffi::OsStr,
        flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] rename(parent: {:#x?}, name: {:?}, newparent: {:#x?}, \
            newname: {:?}, flags: {})",
            parent, name, newparent, newname, flags,
        );
        reply.error(ENOSYS);
    }

    fn link(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        debug!(
            "[Not Implemented] link(ino: {:#x?}, newparent: {:#x?}, newname: {:?})",
            ino, newparent, newname
        );
        reply.error(EPERM);
    }

    fn open(&mut self, _req: &fuser::Request<'_>, _ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        reply.opened(0, 0);
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        warn!(
            "[Not Implemented] read(ino: {:#x?}, fh: {}, offset: {}, size: {}, \
            flags: {:#x?}, lock_owner: {:?})",
            ino, fh, offset, size, flags, lock_owner
        );
        reply.error(ENOSYS);
    }

    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        debug!(
            "[Not Implemented] write(ino: {:#x?}, fh: {}, offset: {}, data.len(): {}, \
            write_flags: {:#x?}, flags: {:#x?}, lock_owner: {:?})",
            ino,
            fh,
            offset,
            data.len(),
            write_flags,
            flags,
            lock_owner
        );
        reply.error(ENOSYS);
    }

    fn flush(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] flush(ino: {:#x?}, fh: {}, lock_owner: {:?})",
            ino, fh, lock_owner
        );
        reply.error(ENOSYS);
    }

    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] fsync(ino: {:#x?}, fh: {}, datasync: {})",
            ino, fh, datasync
        );
        reply.error(ENOSYS);
    }

    fn opendir(
        &mut self,
        _req: &fuser::Request<'_>,
        inode: u64,
        flags: i32,
        reply: fuser::ReplyOpen,
    ) {
        debug!("opendir(ino: {:#x?}, flags: {})", inode, flags);
        let Some(inode) = Inode::from_u64(inode, &self.inode_table) else {
            reply.error(ENOENT);
            return;
        };
        let (path, dir_entries) = match self.list_directory(inode) {
            (PathLookup::Found(FileType::Directory(path)), Some(entries)) => (path, entries),
            (PathLookup::Found(FileType::File(_)), _) => {
                debug!("opendir(ino: {:x?}, flags: {}): found file", inode, flags);
                reply.error(ENOTDIR);
                return;
            }
            (result, query) => {
                debug!(
                    "opendir(ino: {:x?}, flags: {}): inode not found ({result:?}, {query:?})",
                    inode,
                    flags,
                    query = query.is_some()
                );
                reply.error(ENOENT);
                return;
            }
        };
        let dir_entries = dir_entries.collect_vec();
        let fd = self.new_fd();
        let fd_entry = Fd::OpenDir {
            path,
            content: dir_entries,
        };
        debug!(
            "opendir(ino: {:x?}, flags: {}): {fd_entry:#?}",
            inode, flags
        );
        self.fd_table.insert(fd, fd_entry);
        reply.opened(fd, 0);
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        inode: u64,
        fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        debug!(
            "readdir(ino: {:#x?}, fh: {}, offset: {})",
            inode, fh, offset
        );

        let std::result::Result::Ok(offset) = usize::try_from(offset) else {
            error!(
                "readdir(ino: {:#x?}, fh: {}, offset: {}): supplied offset doesn't fit into usize",
                inode, fh, offset
            );
            reply.error(EBADFD);
            return;
        };

        let Some(path) = self.inode_to_path(inode) else {
            warn!(
                "readdir(ino: {:#x?}, fh: {}, offset: {}): no entry for {inode} in inodetable",
                inode, fh, offset
            );
            reply.error(ENOENT);
            return;
        };
        let path = path.as_ref();

        let fill_reply = |mut reply: fuser::ReplyDirectory, dir_entries: &Fd| {
            let Fd::OpenDir { path, content } = dir_entries else {
                reply.error(EBADFD);
                return;
            };
            loop {
                let Some(entry) = content.get(offset) else {
                    // we hit the end of the listing
                    reply.ok();
                    return;
                };
                let file_type = match &entry.file_type {
                    FileType::File(music_file) => fuser::FileType::RegularFile,
                    FileType::Directory(path) => fuser::FileType::Directory,
                };
                if !reply.add(
                    entry.inode.into(),
                    (offset + 1).try_into().unwrap(),
                    file_type,
                    entry.name.as_os_str(),
                ) {
                    reply.ok();
                    return;
                }
                let offset = offset + 1;
            }
        };
        match self.lookup_path(path) {
            PathLookup::MalformedPath | PathLookup::NonexistentVPath | PathLookup::NotFound => {
                error!(
                    "readdir(ino: {:#x?}, fh: {}, offset: {}): path {path:?} ref'd by inode is malformed or nonexistent. THIS SHOULDN'T HAPPEN as that means the inode table is inconsistent",
                    inode, fh, offset
                );
                reply.error(ENOENT)
            }
            PathLookup::Found(FileType::File(music_file)) => reply.error(ENOTDIR),
            PathLookup::Found(FileType::Directory(_)) => {
                let Some(dir_entries) = self.fd_table.get(&fh) else {
                    reply.error(EBADFD);
                    return;
                };
                fill_reply(reply, dir_entries);
                return;
            }
        }
    }

    fn readdirplus(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: fuser::ReplyDirectoryPlus,
    ) {
        debug!(
            "[Not Implemented] readdirplus(ino: {:#x?}, fh: {}, offset: {})",
            ino, fh, offset
        );
        reply.error(ENOSYS);
    }

    fn releasedir(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] fsyncdir(ino: {:#x?}, fh: {}, datasync: {})",
            ino, fh, datasync
        );
        reply.error(ENOSYS);
    }

    fn statfs(&mut self, _req: &fuser::Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }

    fn setxattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        _value: &[u8],
        flags: i32,
        position: u32,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] setxattr(ino: {:#x?}, name: {:?}, flags: {:#x?}, position: {})",
            ino, name, flags, position
        );
        reply.error(ENOSYS);
    }

    fn getxattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        debug!(
            "[Not Implemented] getxattr(ino: {:#x?}, name: {:?}, size: {})",
            ino, name, size
        );
        reply.error(ENOSYS);
    }

    fn listxattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        debug!(
            "[Not Implemented] listxattr(ino: {:#x?}, size: {})",
            ino, size
        );
        reply.error(ENOSYS);
    }

    fn removexattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] removexattr(ino: {:#x?}, name: {:?})",
            ino, name
        );
        reply.error(ENOSYS);
    }

    fn access(&mut self, _req: &fuser::Request<'_>, ino: u64, mask: i32, reply: fuser::ReplyEmpty) {
        debug!("[Not Implemented] access(ino: {:#x?}, mask: {})", ino, mask);
        reply.error(ENOSYS);
    }

    fn create(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        debug!(
            "[Not Implemented] create(parent: {:#x?}, name: {:?}, mode: {}, umask: {:#x?}, \
            flags: {:#x?})",
            parent, name, mode, umask, flags
        );
        reply.error(ENOSYS);
    }

    fn getlk(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        reply: fuser::ReplyLock,
    ) {
        debug!(
            "[Not Implemented] getlk(ino: {:#x?}, fh: {}, lock_owner: {}, start: {}, \
            end: {}, typ: {}, pid: {})",
            ino, fh, lock_owner, start, end, typ, pid
        );
        reply.error(ENOSYS);
    }

    fn setlk(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] setlk(ino: {:#x?}, fh: {}, lock_owner: {}, start: {}, \
            end: {}, typ: {}, pid: {}, sleep: {})",
            ino, fh, lock_owner, start, end, typ, pid, sleep
        );
        reply.error(ENOSYS);
    }

    fn bmap(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        blocksize: u32,
        idx: u64,
        reply: fuser::ReplyBmap,
    ) {
        debug!(
            "[Not Implemented] bmap(ino: {:#x?}, blocksize: {}, idx: {})",
            ino, blocksize, idx,
        );
        reply.error(ENOSYS);
    }

    fn ioctl(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        flags: u32,
        cmd: u32,
        in_data: &[u8],
        out_size: u32,
        reply: fuser::ReplyIoctl,
    ) {
        debug!(
            "[Not Implemented] ioctl(ino: {:#x?}, fh: {}, flags: {}, cmd: {}, \
            in_data.len(): {}, out_size: {})",
            ino,
            fh,
            flags,
            cmd,
            in_data.len(),
            out_size,
        );
        reply.error(ENOSYS);
    }

    fn fallocate(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        length: i64,
        mode: i32,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] fallocate(ino: {:#x?}, fh: {}, offset: {}, \
            length: {}, mode: {})",
            ino, fh, offset, length, mode
        );
        reply.error(ENOSYS);
    }

    fn lseek(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        whence: i32,
        reply: fuser::ReplyLseek,
    ) {
        debug!(
            "[Not Implemented] lseek(ino: {:#x?}, fh: {}, offset: {}, whence: {})",
            ino, fh, offset, whence
        );
        reply.error(ENOSYS);
    }

    fn copy_file_range(
        &mut self,
        _req: &fuser::Request<'_>,
        ino_in: u64,
        fh_in: u64,
        offset_in: i64,
        ino_out: u64,
        fh_out: u64,
        offset_out: i64,
        len: u64,
        flags: u32,
        reply: fuser::ReplyWrite,
    ) {
        debug!(
            "[Not Implemented] copy_file_range(ino_in: {:#x?}, fh_in: {}, \
            offset_in: {}, ino_out: {:#x?}, fh_out: {}, offset_out: {}, \
            len: {}, flags: {})",
            ino_in, fh_in, offset_in, ino_out, fh_out, offset_out, len, flags
        );
        reply.error(ENOSYS);
    }
}
