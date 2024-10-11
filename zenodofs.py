import os
import sys

from argparse import ArgumentParser
import stat
import logging
import errno
import pyfuse3
import trio

import requests
import tarfile
import tempfile
import time

import json

log = logging.getLogger(__name__)

def read_key():
    with open("KEY", "r") as f:
        key = f.read()
    return key
KEY=read_key()

class ZenodoFile:
    def __init__(self, inode, filename, type, content_url, mode, size, timestamp):
        self.inode = inode
        self.filename = filename
        self.size = size
        self.type = type
        self.content_url = content_url
        self.content = b""
        self.entry = pyfuse3.EntryAttributes()

        self.entry.st_mode = mode
        self.entry.st_size = size
        self.entry.st_atime_ns = timestamp
        self.entry.st_ctime_ns = timestamp
        self.entry.st_mtime_ns = timestamp
        self.entry.st_gid = os.getgid()
        self.entry.st_uid = os.getuid()
        self.entry.st_ino = inode

    def download(self):
        if len(self.content) == 0:
            r = requests.get(self.content_url, params={'access_token': KEY})
            self.content = r.content
        return self.content

def add_files(record, inodes):
    r = requests.get(f"https://zenodo.org/api/records/{record}", params={'access_token': KEY})
    data = r.json()
    inodes_nb = [] 
    for file in data["files"]:
        inodes_nb.append(len(inodes))
        inodes.append(ZenodoFile(len(inodes), str.encode(file["key"]),  "file", file["links"]["self"], (stat.S_IFREG | 0o644), file["size"], int(time.time() * 1e9)))
    return inodes_nb

class ZenodoFS(pyfuse3.Operations):
    def __init__(self, record):
        super(ZenodoFS, self).__init__()
        self.record = record
        self.current_folder = pyfuse3.ROOT_INODE

        self.inodes = [None, pyfuse3.ROOT_INODE]
        self.folders = {}
        self.folders[pyfuse3.ROOT_INODE] = add_files(self.record, self.inodes)

    async def getattr(self, inode, ctx=None):
        log.debug(f"[getattr] {inode}")
        if inode in self.folders:
            entry = pyfuse3.EntryAttributes()
            entry.st_mode = (stat.S_IFDIR | 0o755)
            entry.st_size = 0
            stamp = int(time.time() * 1e9)
            entry.st_atime_ns = stamp
            entry.st_ctime_ns = stamp
            entry.st_mtime_ns = stamp
            entry.st_gid = os.getgid()
            entry.st_uid = os.getuid()
            entry.st_ino = inode
            return entry
        else:
            return self.inodes[inode].entry

    async def lookup(self, parent_inode, name, ctx=None):
        log.debug(f"[lookup] {parent_inode} {name}")
        if parent_inode != self.current_folder:# or name != self.hello_name:
            raise pyfuse3.FUSEError(errno.ENOENT)
        log.debug(self.folders[parent_inode])
        for i in self.folders[parent_inode]:
            if self.inodes[i].filename == name:
                log.debug(f"[lookup] `{name}` found!")
                return await self.getattr(i)
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def opendir(self, inode, ctx):
        log.debug(f"[opendir] {inode}")
        if inode > len(self.inodes):
            raise pyfuse3.FUSEError(errno.ENOENT)
        self.current_folder = inode
        return inode

    async def readdir(self, fh, start_id, token):
        log.debug(f"[readdir] {fh} {start_id} {token}")
        assert fh == self.current_folder
        for i in self.folders[fh][start_id:]:
            pyfuse3.readdir_reply(
                token, self.inodes[i].filename, await self.getattr(i), i)

        return

    async def open(self, inode, flags, ctx):
        log.debug(f"[open] {inode} {flags}")
        if flags & os.O_RDWR or flags & os.O_WRONLY:
            raise pyfuse3.FUSEError(errno.EACCES)
        return pyfuse3.FileInfo(fh=inode)

    async def read(self, fh, off, size):
        log.debug(f"[read] {fh} {off} {size}")
        return self.inodes[fh].download()[off:off+size]

    async def _create(self, inode_p, name, mode, ctx, rdev=0, target=None):
        log.debug(f"[_create] {inode_p} {name} {mode}")
        for i in self.folders[inode_p]:
            if self.inodes[i].filename == name and self.inodes[i].type == "directory":
                log.debug(f"[_create] Did not need to create `{name}` !")
                return await self.getattr(i)
        inode = len(self.inodes)
        self.inodes.append(ZenodoFile(inode, name, "directory", "", mode, 0, int(time.time() * 1e9)))
        self.folders[inode] = []
        self.folders[inode_p].append(inode)
        return await self.getattr(inode)

    async def mkdir(self, inode_p, name, mode, ctx):
        log.debug(f"[mkdir] {inode_p} {name} {mode}")
        return await self._create(inode_p, name, mode, ctx)

    async def write(self, fh, offset, buf):
        log.debug(f"[write] {fh} {offset} {len(buf)}")
        current_content = self.inodes[fh].content
        self.inodes[fh].content = current_content[:offset] + buf + current_content[offset+len(buf):]
        self.inodes[fh].entry.st_size += len(buf)
        return len(buf)

    async def create(self, inode_parent, name, mode, flags, ctx):
        log.debug(f"[create] {inode_parent} {name} {mode} {flags}")
        inode = len(self.inodes)
        self.inodes.append(ZenodoFile(inode, name, "file", "", mode, 0, int(time.time() * 1e9)))
        self.folders[inode_parent].append(inode)
        entry = self.inodes[inode].entry
        return (pyfuse3.FileInfo(fh=entry.st_ino), entry)

    async def setattr(self, inode, attr, fields, fh, ctx):
        log.debug(f"[setattr] {inode} {attr} {fields} {fh}")
        if fields.update_size:
            log.debug("update_size")
            self.inodes[inode].entry.st_size = attr.st_size
        if fields.update_mode:
            log.debug("update_mode")
            self.inodes[inode].entry.st_mode = attr.st_mode
        if fields.update_atime:
            log.debug("update atime")
            self.inodes[inode].entry.st_atime_ns = attr.st_atime_ns
        if fields.update_mtime:
            log.debug("update mtime")
            self.inodes[inode].entry.st_mtime_ns = attr.st_mtime_ns
        return await self.getattr(inode)

def init_logging(debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

def parse_args():
    '''Parse command line'''

    parser = ArgumentParser()

    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    parser.add_argument('--debug-fuse', action='store_true', default=False,
                        help='Enable FUSE debugging output')
    parser.add_argument('record', help='Zenodo Record ID')
    return parser.parse_args()


def main():
    options = parse_args()
    init_logging(options.debug)
    record = options.record
    #RECORD="11208389"
    #RECORD="6568218"

    zenodofs = ZenodoFS(record)
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=ZenodoFS')
    if options.debug_fuse:
        fuse_options.add('debug')
    pyfuse3.init(zenodofs, options.mountpoint, fuse_options)
    try:
        trio.run(pyfuse3.main)
    except:
        pyfuse3.close(unmount=False)
        raise
    pyfuse3.close()

if __name__ == '__main__':
    main()
