//! pgfs exposes data inside a PostgreSQL database as files in a fuse filesystem
//! To run as a binary application, provide a config file in toml format
//! (see the docs or examples) to specify the database you want to connect to,
//! and what tables/queries to expose and how.
//!
//! pgfs aims to be fully functional, and allows you to (optionally) read, create, modify, rename and
//! delete files which map to data stored in a PostgreSQL database. Default behaviour is to
//! mirror the filesystem operation in the database (so a read is a select, and a delete deletes etc.),
//! but you can fully configure the SQL queries used for each operation so that, for example, a delete
//! might just mark a status field in a record as archived, or a write might be set to create a new
//! version of a file, leaving the old one in tact (beware some programs do a lot of small writes).
//!
//! This is a FUSE filesystem, and particular attention has not been paid to raw performance.
//! In particular, frequent or large writes to bytea or text fields will perform poorly as Postgres is not designed
//! to write deltas to individual fields. It is not possible to store files larger than the database limit
//! for a bytea field (1 or 2 GB at present depending on version) and you would not want to do so
//! as the performance for writes would be awful. Fuse filesystems tend to write data in 4k blocks which
//! would kill write performance for any typical non-text files (writing a ~10mb file would incur ~2.5k writes)
//! so by default we cache sequential writes up to a configurable size. See ***caching*** for more details or
//! to modify or turn off this feature. A typical slowdown vs local file writing might be 1-2 orders of magnitude.
//! A write of a 10mb file on my laptop with the default cache size of 2mb takes 60 times longer (2 seconds)
//! than a straight cp on the same filesystem.
//!
//! Whilst Postgres can store larger files using blobs
//! there is no explicit blob support, but it might work if you provide the queries in config. If you
//! try it, please let me know how you get on.
//!
//! This is currently experimental software and as such it is not recommended that you use it
//! for production systems, or enable write access to data you are not prepared to see corrupted.
//! Warnings aside, if you configure read only mode, and set up a Postgres user with only read permissions
//! your data should be safe, and this implementation is single threaded and blocking, so should not
//! exhaust your resources easily.
//!
//! Bug reports, bug fixes, pull requests, examples, feature requests and any other feedback is welcome.
//! If you try this out and have 5 minutes to drop me a quick message to tell me what you think that would
//! be great.

mod config;

use libc::{ ENOSYS, ENOENT, ENODATA, EIO,  EROFS, EPERM, EISDIR};
use fuser::*;
use postgres::{Client, NoTls};
use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::{env, cmp};
use std::collections::HashMap;
use bimap::BiMap;
use crate::config::PgfsConfig;
use std::time::SystemTime;
use std::cmp::max;

struct ByteaFileSystem {
    name: OsString,
    next_inode: Inode,
    db_client: Client,
    tables: HashMap<String, Table>,
    inode_file_attrs: HashMap<Inode, FileAttr>,
    table_dir_inodes: BiMap<Inode, String>,
    file_inodes: HashMap<Inode, (Table, PgId)>,
    entries: BiMap<ChildNode, Inode>,
    cache: HashMap<Inode, (i64, Vec<u8>)>,
}

impl ByteaFileSystem {
    fn dir_file_attr(inode: Inode) -> FileAttr {
        FileAttr {
            ino: inode as u64,
            size: 0,
            blocks: 0,
            atime: std::time::UNIX_EPOCH, // 1970-01-01 00:00:00
            mtime: std::time::UNIX_EPOCH,
            ctime: std::time::UNIX_EPOCH,
            crtime: std::time::UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 3,
            uid: 1001,
            gid: 20,
            rdev: 0,
            flags: 0,
            blksize: 512 * 1024,
        }
    }
    fn file_attr(inode: Inode, size: u64, ctime:Option<SystemTime>, mtime:Option<SystemTime>) -> FileAttr {
        FileAttr {
            ino: inode as u64,
            size,
            blocks: (size + 1) / (512*1024),
            atime: std::time::UNIX_EPOCH, // 1970-01-01 00:00:00
            mtime: mtime.unwrap_or(std::time::UNIX_EPOCH),
            ctime: ctime.unwrap_or(std::time::UNIX_EPOCH),
            crtime: std::time::UNIX_EPOCH,
            kind: FileType::RegularFile,
            perm: 0o755,
            nlink: 3,
            uid: 1001,
            gid: 20,
            rdev: 0,
            flags: 0,
            blksize: 512 * 1024,
        }
    }
    pub fn new(name_: &str, db_client: Client, tables: Vec<Table>) -> ByteaFileSystem {

        //root dir will have inode 1, so we create tables from 2
        let mut file_attrs = HashMap::new();
        let mut dir_inodes = BiMap::new();
        for i in 0..tables.len() {
            file_attrs.insert((i + 2) as Inode, ByteaFileSystem::dir_file_attr((i + 2) as Inode));
            dir_inodes.insert((i + 2) as Inode, tables[i].table_name.clone());
        }

        ByteaFileSystem {
            name: name_.to_string().parse().unwrap(),
            next_inode: (tables.len() + 1) as Inode,
            db_client,
            tables: tables.into_iter().map(|t| (t.table_name.clone(), t)).collect(),
            inode_file_attrs: file_attrs,
            table_dir_inodes: dir_inodes,
            entries: BiMap::new(),
            file_inodes: HashMap::new(),
            cache: HashMap::new(),
        }
    }
    pub fn get_next_inode(&mut self) -> Inode {
        self.next_inode += 1;
        self.next_inode
    }

    pub fn flush_internal(&mut self, ino: Inode) {
        self.write_data_to_postgres(ino, None);
    }
    fn create_internal(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr) -> Result<Inode,i32 > {
        dbg!("create_internal");
        //create the file
        //get parent to know what table it is
        if parent == 1 {
            //can't create a file at the top level
            return Err(ENOENT);
        }

        if let Some(table_name) = self.table_dir_inodes.get_by_left(&parent) {
            if let Some(table) = self.tables.get(table_name) {
                if table.read_only {
                    return Err(EROFS)
                }
                self.next_inode += 1;
                let inode = self.next_inode;
                //insert the new record into the db with no data (create is basicly touch)
                let name = name.to_str().unwrap();
                let query = format!("insert into {} ({}) values ($1) returning {}", table_name, table.name_field.as_ref().unwrap(), &table.id_field);
                let id = self.db_client.query_one(query.as_str(), &[&name, ]);
                if id.is_err() {
                    dbg!(id);
                    return Err(ENOSYS);
                } else {
                    let id = id.unwrap().get::<usize, i32>(0) as u64;
                    self.file_inodes.insert(inode, (table.clone(), PgId {
                        table_inode: parent,
                        pg_id: id,
                    }));
                    self.entries.insert(ChildNode { parent, name: name.to_string() }, inode);
                    self.inode_file_attrs.insert(inode, ByteaFileSystem::file_attr(inode, 0, Some(SystemTime::now()), Some(SystemTime::now())));
                    return Ok(inode);
                }
            }
        }
        Err(ENOSYS)
    }
    fn write_data_to_postgres(&mut self, ino:u64, data: Option<(i64, &[u8])> ) -> bool {
        //get cached data and add any optional data
        if let Some((table, pgid)) =  self.file_inodes.get(&ino) {
            let  query_string = format!("update {} set {} = coalesce(overlay({} placing $1 from $2 for $3), $1) where {} = $4", table.table_name, table.bytea_field, table.bytea_field, table.id_field);
            //let mut data_to_write: Option<(i64, &[u8])> = None;
            let mut new_len = None;
            if self.cache.contains_key(&ino) {
                let (offset, mut cached_data) = self.cache.remove(&ino).unwrap();
                if data.is_some() {
                    if offset as usize + cached_data.len() == data.as_ref().unwrap().0 as usize {
                        //existing data precedes new data
                        cached_data.extend_from_slice(data.unwrap().1);
                        //data_to_write = Some((offset, cached_data.as_slice()));
                        if let Err(e) = self.db_client.execute(query_string.as_str(), &[&cached_data, &(offset as i32+1), &(cached_data.len() as i32), &(pgid.pg_id as i32)]) {
                            dbg!(e);
                            return false
                        } else {
                            new_len = Some(offset + cached_data.len() as i64);
                        }
                    } else {
                        //write the cached data then the new data
                        if let Err(e) = self.db_client.execute(query_string.as_str(), &[&cached_data, &(offset as i32+1), &(cached_data.len() as i32), &(pgid.pg_id as i32)]) {
                            dbg!(e);
                            return false
                        } else {
                            new_len = Some(offset as i64 + new_len.unwrap_or(0) + cached_data.len() as i64);
                            let data = data.unwrap();
                            if let Err(e) = self.db_client.execute(query_string.as_str(), &[&data.1, &(data.0 as i32 +1), &(data.1.len() as i32), &(pgid.pg_id as i32)]) {
                                dbg!(e);
                            } else {
                                new_len = Some(offset as i64 + new_len.unwrap_or(0) + cached_data.len() as i64);
                            }
                        }
                    }
                } else {
                    if let Err(e) = self.db_client.execute(query_string.as_str(), &[&cached_data, &(offset as i32+1), &(cached_data.len() as i32), &(pgid.pg_id as i32)]) {
                        dbg!(e);
                        return false
                    } else {
                        new_len = Some((offset as i64 + cached_data.len() as i64) as i64);
                    }
                }
            } else if data.is_some(){
                let (offset, data) = data.unwrap();
                if let Err(e) = self.db_client.execute(query_string.as_str(), &[&data, &(offset as i32+1), &(data.len() as i32), &(pgid.pg_id as i32)]) {
                    dbg!(e);
                    return false
                } else {
                    new_len = Some(data.len() as i64);
                }
            }
            //length is max of offset + new data and existing length
            if let Some(attrs) = self.inode_file_attrs.get_mut(&ino) {
                if let Some(new_len) = new_len {
                    attrs.size = max(attrs.size, new_len as u64);
                    attrs.blocks = (attrs.size+1)/(attrs.blksize as u64)
                }
            }
        }

        true
    }

}

/*
for each table, a path, then file names or path names
find longest matching path (can't have conflics between say,
    customers/<name>/files
    customers/subquery/files
*/

#[derive(Hash, PartialEq, Eq, Copy, Clone)]
struct PgId {
    table_inode: Inode,
    pg_id: u64,
}

#[derive(Hash, PartialOrd, PartialEq, Eq, Debug)]
struct ChildNode {
    parent: Inode,
    name: String,
}

#[derive(Clone, Debug)]
struct Table {
    table_name: String,
    id_field: String,
    length_field: String,
    bytea_field: String,
    name_field: Option<String>,
    query_string: String,
    data_query_string: String,
    read_only:bool,
    delete_query_string:Option<String>,
    created_field:Option<String>,
    modified_field:Option<String>,
}

type Inode = u64;

const TTL: std::time::Duration = std::time::Duration::from_secs(1); // 1 second

const ROOT: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: std::time::UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: std::time::UNIX_EPOCH,
    ctime: std::time::UNIX_EPOCH,
    crtime: std::time::UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 3,
    uid: 1001,
    gid: 20,
    rdev: 0,
    flags: 0,
    blksize: 512,
};

const DUMMY: FileAttr = FileAttr {
    ino: 2,
    size: 0,
    blocks: 8,
    atime: std::time::UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: std::time::UNIX_EPOCH,
    ctime: std::time::UNIX_EPOCH,
    crtime: std::time::UNIX_EPOCH,
    kind: FileType::RegularFile,
    perm: 0o755,
    nlink: 2,
    uid: 1001,
    gid: 20,
    rdev: 0,
    flags: 0,
    blksize: 512 * 1024,
};

impl Filesystem for ByteaFileSystem {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), i32> {
        _config.set_max_write(1024*128);
        Ok(())
    }

    //this is for filesystem exit, not file delete (unlink)
    fn destroy(&mut self) {
        self.inode_file_attrs.clear();
        self.file_inodes.clear();
        self.entries.clear();
        self.tables.clear();
        self.cache.clear();
        //self.db_client.close(); - should do this but need to move
        //could Option it, then acces via fn with expect? should be a better way
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent == 1 { //child of root dir
            dbg!("lookup root");
            if let Some(inode) = self.table_dir_inodes.get_by_right(name.to_str().unwrap_or("")) {
                dbg!("folder", name, inode);
                reply.entry(&TTL, &ByteaFileSystem::dir_file_attr(*inode), 0)
            } else {
                dbg!("request for non-existant file", name);

                reply.error(ENOENT)
            }
        } else {
            dbg!("lookup file {} {}", parent, name);
            if let Some(inode) = self.entries.get_by_left(&ChildNode {
                parent,
                name: name.to_str().unwrap_or("").to_string(),
            }) {
                if let Some(attr) = self.inode_file_attrs.get(inode) {
                    dbg!("found entry");
                  //  dbg!(attr);
                    reply.entry(&TTL, attr, 0);
                } else {
                    dbg!("no file attr entry");
                }
            } else {
                dbg!("file not found");
                reply.error(ENOENT);
            }
            //reply.entry(&TTL, &DUMMY, 0
        }
    }

    fn forget(&mut self, _req: &Request<'_>, _ino: u64, _nlookup: u64) {
        dbg!("forget");
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh:Option<u64>, reply: ReplyAttr) {
        //   dbg!(_req);
        match ino {
            1 => {
                dbg!("getattr root ");
                reply.attr(&TTL, &ROOT);
            }
            _ => {
                dbg!("getattr file");
                if self.inode_file_attrs.contains_key(&ino as &Inode) {
                    dbg!("got attr");
                    reply.attr(&TTL, self.inode_file_attrs.get(&ino as &Inode).unwrap());
                } else {
                    dbg!("not got attr");
                    reply.error(ENOENT);
                }
            }
        }
    }

    fn setattr(&mut self, _req: &Request<'_>,
               ino: u64, _mode: Option<u32>,
               _uid: Option<u32>,
               _gid: Option<u32>,
               size: Option<u64>,
               _atime: Option<TimeOrNow>,
               _mtime: Option<TimeOrNow>,
               _ctime: Option<SystemTime>,
               _fh: Option<u64>,
               _crtime: Option<SystemTime>,
               _chgtime: Option<SystemTime>,
               _bkuptime: Option<SystemTime>,
               _flags: Option<u32>, reply: ReplyAttr) {
        let mut error:Option<i32> = None;
        if let Some(size) = size {
            dbg!("truncate to size", size);

                //truncate the file
                if let Some((table,pgid)) = self.file_inodes.get(&ino) {
                    let query_string = format!("update {} set {} = substring({}, 1, $1) where {}  = $2;", table.table_name, table.bytea_field, table.bytea_field, table.id_field);
                     if self.db_client.execute(query_string.as_str(), &[&(size as i32), &(pgid.pg_id as i32)]).is_err() {
                         error = Some(EIO);
                         dbg!("Failed to truncate: ", query_string);
                     }
                     if let Some(attr) = self.inode_file_attrs.get_mut(&ino) {
                         attr.size = size;
                     }
                }
        }
        if let Some((table,pgid)) = self.file_inodes.get(&ino) {
            if _ctime.is_some() && table.created_field.is_some() {
                let query_string = format!("update {} set {} = $1 where {} = $2", table.table_name, table.created_field.as_ref().unwrap(), table.id_field );
                if self.db_client.execute(query_string.as_str(), &[_ctime.as_ref().unwrap(), &(pgid.pg_id as i32)]).is_err() {
                    error = Some(EPERM); //calling it a permissions error to avoid issues with clients
                }
                if let Some(attr) = self.inode_file_attrs.get_mut(&ino) {
                    attr.ctime = _ctime.unwrap();
                }
            }
        }
        if let Some((table,pgid)) = self.file_inodes.get(&ino) {
            if _mtime.is_some() && table.modified_field.is_some() {
                let time:SystemTime = match _mtime.unwrap() {
                    TimeOrNow::SpecificTime(t) => { t }
                    TimeOrNow::Now => { SystemTime::now() }
                };
                let query_string = format!("update {} set {} = $1 where {} = $2", table.table_name, table.modified_field.as_ref().unwrap(), table.id_field );
                if self.db_client.execute(query_string.as_str(), &[&time, &(pgid.pg_id as i32)]).is_err() {
                    error = Some(EPERM); //calling it a permissions error to avoid issues with clients
                }
                if let Some(attr) = self.inode_file_attrs.get_mut(&ino) {
                    attr.mtime = time;
                }
            }
        }

        if error.is_none() && let Some(attr) = self.inode_file_attrs.get(&ino) {
            reply.attr(&TTL, attr);
        } else {
            reply.error(error.unwrap_or(ENODATA));
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyData) {
        reply.error(ENOSYS)
    }


    fn mkdir(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, _mode: u32, _umask: u32, reply: ReplyEntry) {
        //there is no obvious structure for allowing sub directories.
        //an archive might work, but I'm not building it until someone wants it
        reply.error(EISDIR);
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        //unlink called by vim when trying to delete a swap file it thinks it found
        dbg!("unlink", name);

        if let Some(ino )= self.entries.remove_by_left(&ChildNode{parent, name: name.to_str().unwrap_or("").to_string()})
        {
            let table = self.tables.get(self.table_dir_inodes.get_by_left(&parent).unwrap_or(&"".to_string()));
            let table = table.unwrap();
            let default_delete_query_string =
                format!("delete from {} where {} = $1", table.table_name, table.id_field);
            let delete_query_string =
            table.delete_query_string.as_ref().unwrap_or(
                &default_delete_query_string);
            let ino = ino.1;
            //self.write_data_to_postgres(ino, None);
            if let Some((_,pgid)) = self.file_inodes.get(&ino) {
                self.db_client.execute(delete_query_string.as_str(), &[&(pgid.pg_id as i32)]);
                //if there is something in the cache, then we are deleting a file
                //which has not been fully written. Add a config (default true)
                //to flush first
                self.cache.remove(&ino);
                self.file_inodes.remove(&ino);
                self.inode_file_attrs.remove(&ino);
            } else {
                dbg!("missing file inode");
            }
        }
        reply.ok();
    }

    fn rmdir(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(ENOSYS);
    }

    fn symlink(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, _link: &Path, reply: ReplyEntry) {
        reply.error(ENOSYS);
    }

    fn rename(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, newparent: u64, newname: &OsStr, _flags: u32, reply: ReplyEmpty) {
        //run sql to rename the file
        //change the references in this struct
        if parent != newparent {
            reply.error(ENOSYS);
            return;
        }

        if let Some(table_name) = self.table_dir_inodes.get_by_left(&parent) {
            if let Some(table) = self.tables.get(table_name) {
                let query = format!("update {} set {} = ($1) where {} = $2", table_name, table.name_field.as_ref().unwrap(), &table.name_field.as_ref().unwrap());
                if let Err(_e) = self.db_client.execute(query.as_str(), &[&newname.to_str(), &name.to_str()]) {
                    reply.error(EIO); //could do better with the error here maybe
                    return;
                }
            }
        }
        reply.ok();
    }

    fn link(&mut self, _req: &Request<'_>, _ino: u64, _newparent: u64, _newname: &OsStr, reply: ReplyEntry) {
        reply.error(ENOSYS);

        //need to decide whether to have transient links, database links, or what
        //probably needs to be done in config as both could be useful, as could not allowing links
    }

    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        dbg!("open", _ino, _flags);
        reply.opened(_ino, _flags as u32);
    }

    fn read(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        dbg!("read");
        self.write_data_to_postgres(ino, None);
        if let Some((table, pgid)) = self.file_inodes.get(&ino) {
            let p_row = self.db_client.query_one(table.data_query_string.as_str(), &[&(pgid.pg_id as i32), &(1 + offset as i32), &(size as i32)]);
            if let Ok(res) = p_row {
                let bytes: Option<&[u8]> = res.get(0);
                let empty = Vec::new();
                let data = bytes.unwrap_or(&empty);
                dbg!(data.len());
                reply.data(data)
            } else {
                reply.data(&[])
            }
        } else {
            dbg!(format!("read inode {} offset {} size {}", ino, offset, size));
            reply.error(ENOENT)
        }
    }

    fn write(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, data: &[u8], _write_flags: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyWrite) {
        //lookup ino - will have been created
        dbg!("write" ,data.len());
        if let Some((cached_offset, cached_data)) = self.cache.get_mut(&ino) {
            if *cached_offset + cached_data.len() as i64 == offset && cached_data.len() + data.len() <= 2097152 {
                cached_data.extend_from_slice(data);
                if let Some(attrs) = self.inode_file_attrs.get_mut(&ino) {
                        attrs.size = max(attrs.size, (offset as i64 + (data.len() as i64)) as u64);
                        attrs.blocks = (attrs.size + 1) / (attrs.blksize as u64)
                }
                dbg!("Extended data cache");
            } else {
                self.write_data_to_postgres(ino, Some((offset, data)));
            }
        } else {
            self.cache.insert(ino, (offset, Vec::from(data)));
        }
        reply.written(data.len() as u32);

    }

    fn flush(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        dbg!("flush");
        self.write_data_to_postgres(ino, None);
        reply.ok();
    }



    fn release(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) {
        self.write_data_to_postgres(ino, None);
        reply.ok();
    }


    fn fsync(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        dbg!("fsync");
        self.write_data_to_postgres(ino, None);
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        dbg!("opendir");
        //   dbg!(ino);
        reply.opened(ino, 0)
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        //  dbg!(format!("readdir {} {} {}", ino, fh, offset));
        dbg!("readdir");
        if ino == 1 {
            let default_entries = vec![
                (1, FileType::Directory, "."),
                (1, FileType::Directory, ".."),
            ];
            let mut i = 1;
            let table_entries: Vec<(u64, FileType, &str)> = self.tables.values().map(|tab| {
                i = i + 1;
                (i as u64, FileType::Directory, tab.table_name.as_str())
            }).collect();

            for (i, entry) in default_entries.into_iter().chain(table_entries.into_iter()).enumerate().skip(offset as usize) {
                // i + 1 means the index of the next entry
                if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
                    break;
                }
            }
            reply.ok();
        } else {
            if ino > 1 {
                if let Some(table_name) = self.table_dir_inodes.get_by_left(&ino) {
                    if offset == 0 {
                        reply.add(ino, 1, FileType::Directory, ".");
                    }
                    if offset <= 1 {
                        reply.add(ino, 2, FileType::Directory, "..");
                    }
                    if let Some(table) = self.tables.get(table_name) {
                        let mut i = 3;
                        for row in self.db_client.query(table.query_string.as_str(), &[]).unwrap().into_iter().skip(cmp::max(offset - 2, 0) as usize) {
                            dbg!("add {}", row.get::<&str, String>("name"));
                            let child = ChildNode { parent: ino, name: row.get("name") };
                            if self.entries.contains_left(&child) {
                                let _=reply.add(*self.entries.get_by_left(&child).unwrap(), i, FileType::RegularFile, row.get::<&str, String>("name"));
                                i+=1;
                                continue
                            }
                            let t = Table {
                                table_name: table.table_name.clone(),
                                id_field: table.id_field.clone(),
                                length_field: table.length_field.clone(),
                                bytea_field: table.bytea_field.clone(),
                                name_field: table.name_field.clone(),
                                query_string: table.query_string.clone(),
                                data_query_string: table.data_query_string.clone(),
                                delete_query_string: table.delete_query_string.clone(),
                                read_only:table.read_only,
                                created_field:table.created_field.clone(),
                                modified_field:table.modified_field.clone(),
                            };
                            self.next_inode += 1;
                            let inode = self.next_inode;
                            reply.add(inode, i, FileType::RegularFile, row.get::<&str, String>("name"));
                            let mtime:Option<SystemTime> = match table.modified_field.as_ref() {
                                Some(modified_field) => {(row.get::<&str, Option<SystemTime>>(modified_field.as_str()))},
                                None => {None}
                            };
                            let ctime:Option<SystemTime> = match table.created_field.as_ref() {
                                Some(created_field) => {row.get::<&str, Option<SystemTime>>(created_field.as_str())}
                                None => {None}
                            };
                            self.inode_file_attrs.insert(inode, ByteaFileSystem::file_attr(inode, row.get::<&str, Option<i32>>("length").unwrap_or(0) as u64, ctime, mtime));
                            i += 1;
                            let pgid = PgId {
                                table_inode: ino,
                                pg_id: row.get::<&str, i32>("id") as u64,
                            };
                            self.file_inodes.insert(inode, (t, pgid));
                            self.entries.insert(child, inode);
                            //self.entries.insert(
                            //);
                        }
                    }
                    // dbg!(&reply);
                    reply.ok();
                }
            }
        }
    }

    fn readdirplus(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _offset: i64, reply: ReplyDirectoryPlus) {
        reply.ok()
    }

    fn releasedir(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _flags: i32, reply: ReplyEmpty) {
        dbg!("releasedir");
        reply.ok();
    }

    fn fsyncdir(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        reply.ok();
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        reply.error(ENOSYS);
    }

    fn setxattr(&mut self, _req: &Request<'_>, _ino: u64, _name: &OsStr, _value: &[u8], _flags: i32, _position: u32, reply: ReplyEmpty) {
        reply.error(ENOSYS);
    }

    fn getxattr(&mut self, _req: &Request<'_>, _ino: u64, _name: &OsStr, _size: u32, reply: ReplyXattr) {
        dbg!("getxattr");
        reply.error(ENODATA);
    }

    fn listxattr(&mut self, _req: &Request<'_>, _ino: u64, _size: u32, reply: ReplyXattr) {
        reply.size(0);
    }

    fn removexattr(&mut self, _req: &Request<'_>, _ino: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.ok();
    }

    fn access(&mut self, _req: &Request<'_>, _ino: u64, _mask: i32, reply: ReplyEmpty) {
        dbg!("access");
        reply.ok()
    }




    fn mknod(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, _mode: u32, _umask: u32, _rdev: u32, reply: ReplyEntry) {

        match  self.create_internal( _req, _parent, _name) {
            Ok(r) =>
                reply.entry(&TTL, &ByteaFileSystem::file_attr(r, 0, Some(SystemTime::now()), Some(SystemTime::now())), 0),
            Err(e) =>
                reply.error(e)

        }

    }
    fn create(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, _mode: u32, _umask: u32, _flags: i32, reply: ReplyCreate) {
        dbg!("create");
        //create the file
        //get parent to know what table it is
        if parent == 1 {
            //can't create a file at the top level
            reply.error(ENOENT);
            return;
        }
        if let Some(table_name) = self.table_dir_inodes.get_by_left(&parent) {
            if let Some(table) = self.tables.get(table_name) {
                if table.read_only {
                    reply.error(EROFS);
                    return;
                }
                self.next_inode += 1;
                let inode = self.next_inode;
                //insert the new record into the db with no data (create is basicly touch)
                let name = name.to_str().unwrap();
                let query = format!("insert into {} ({}) values ($1) returning {}", table_name, table.name_field.as_ref().unwrap(), &table.id_field);
                let id = self.db_client.query_one(query.as_str(), &[&name, ]);
                if id.is_err() {
                    reply.error(ENOSYS);
                } else {
                    let id = id.unwrap().get::<usize, i32>(0) as u64;
                    self.file_inodes.insert(inode, (table.clone(), PgId {
                        table_inode: parent,
                        pg_id: id,
                    }));
                    self.entries.insert(ChildNode { parent, name: name.to_string() }, inode);
                    self.inode_file_attrs.insert(inode, ByteaFileSystem::file_attr(inode, 0, Some(SystemTime::now()), Some(SystemTime::now())));
                    reply.created(&TTL, &ByteaFileSystem::file_attr(inode, 0, Some(SystemTime::now()), Some(SystemTime::now())), 0, 0, 0);
                }
            }
        }
    }

    fn getlk(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _lock_owner: u64, _start: u64, _end: u64, _typ: i32, _pid: u32, reply: ReplyLock) {
        todo!()
    }

    fn setlk(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _lock_owner: u64, _start: u64, _end: u64, _typ: i32, _pid: u32, _sleep: bool, reply: ReplyEmpty) {
        todo!()
    }

    fn bmap(&mut self, _req: &Request<'_>, _ino: u64, _blocksize: u32, _idx: u64, reply: ReplyBmap) {
        todo!()
    }

    fn ioctl(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _flags: u32, _cmd: u32, _in_data: &[u8], _out_size: u32, reply: ReplyIoctl) {
        todo!()
    }

    fn fallocate(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _offset: i64, _length: i64, _mode: i32, reply: ReplyEmpty) {
        todo!()
    }

    fn lseek(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _offset: i64, _whence: i32, reply: ReplyLseek) {
        todo!()
    }

    fn copy_file_range(&mut self, _req: &Request<'_>, _ino_in: u64, _fh_in: u64, _offset_in: i64, _ino_out: u64, _fh_out: u64, _offset_out: i64, _len: u64, _flags: u32, reply: ReplyWrite) {
        todo!()
    }
}


struct ConsoleLogger;

impl log::Log for ConsoleLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        println!("{}: {}: {}", record.target(), record.level(), record.args());
    }

    fn flush(&self) {}
}

static LOGGER: ConsoleLogger = ConsoleLogger;


fn main() {
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Debug);

    let args: Vec<String> = env::args().collect();
    let config_file_location = if args.len() < 2 {
        "config.toml".to_string()
    } else {
        args[1].clone()
    };

    use std::fs;
    let config_string = fs::read_to_string(config_file_location).unwrap();

    let cfg: PgfsConfig = PgfsConfig::new(&config_string).unwrap();

    dbg!(&cfg);
    let mountpoint = cfg.mountpoint;
    let mut options = vec![MountOption::RW, MountOption::FSName("pgtest".to_string())];
    options.push(MountOption::AutoUnmount);


    let db_string = cfg.connection_string.expect("Database connection details missing");
    let  client = Client::connect(&db_string, NoTls).expect(format!("Unable to open a connection to database {}", db_string).as_str());
    let mut tables = vec![];
    cfg.table_config.iter().for_each(|(name, fs)| {
        dbg!(name);
        tables.push(Table {
            table_name: fs.table_name.clone(),
            id_field: fs.id_field.clone(),
            length_field: fs.length_field.clone(),
            bytea_field: fs.data_field.clone(),
            name_field: Some(fs.name_field.clone()),
            query_string: fs.data_query.to_string(),
            data_query_string: format!("select substring({}, $2, $3) from {} where ({}=$1);", fs.data_field, fs.table_name, fs.id_field),
            read_only: false,
            delete_query_string: None,
            created_field:fs.created_date_field.clone(),
            modified_field:fs.modified_date_field.clone(),
        });
    });
    let filesystem = ByteaFileSystem::new(
        "pgfs",
        client,
        tables,
    );
    fuser::mount2(filesystem, mountpoint, &options).unwrap();


    //let mut client = Client::connect("host=localhost user=postgres", NoTls).unwrap();
   // let mut client = Client::connect("postgres://paul:test@127.0.0.1/wsf", NoTls)
   //     .expect("Unable to connect to database");

    /*let args: Vec<OsString> = env::args_os().collect();

    if args.len() != 3 {
        println!("usage: {} <target> <mountpoint>", &env::args().next().unwrap());
        ::std::process::exit(-1);
    }
*/

//    let filesystem = ByteaFileSystem::new(
//        args[1].clone().to_str().unwrap_or("filesystem"),
//        client,
//        vec![Table{
//            table_name: "poopics".to_string(),
//            id_field: "id".to_string(),
    //           length_field: "length".to_string(),
    //           bytea_field: "image".to_string(),
    //           query_string: "select id, 'image_'||id || regexp_replace(mime_type, '^.*/','.') as name, length(image) from poopics;".to_string(),
    //           data_query_string: "select substring(image, $2, $3) from poopics where (id=$1);".to_string()
    //       }]
    //   );
    /*
        let fuse_args: Vec<&OsStr> = vec![&OsStr::new("-o"), &OsStr::new("auto_unmount")];

        fuse_mt::mount(fuse_mt::FuseMT::new(filesystem, 1), &args[2], &fuse_args).unwrap();
    */
}
