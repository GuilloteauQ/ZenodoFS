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

class ZenodoDirectory:
    def __init__(self, name, inode):
        self.name = name
        self.inode = inode
        self.files = []
        self.directories = []

class ZenodoFile:
    def __init__(self, filename, size, type, content_url):
        self.filename = filename
        self.size = size
        self.type = type
        self.content_url = content_url
        self.content = None

    def download(self):
        if self.content is None:
            r = requests.get(self.content_url, params={'access_token': KEY})
            self.content = r.content
        return self.content

def get_files(record):
    r = requests.get(f"https://zenodo.org/api/records/{record}", params={'access_token': KEY})
    data = r.json()
    files = []
    with open("result.json", "w") as jf:
        jf.write(json.dumps(data))
    for file in data["files"]:
        print(file["key"])
        files.append(ZenodoFile(str.encode(file["key"]), file["size"], "file", file["links"]["self"]))
    return files


class ZenodoFS(pyfuse3.Operations):
    def __init__(self, record):
        super(ZenodoFS, self).__init__()
        self.record = record

        #self.root = ZenodoDirectory("root", pyfuse3.ROOT_INODE)
        #self.root.files = get_files(self.record)

        self.files = get_files(self.record)
        #self.next_inode = pyfuse3.ROOT_INODE + len(self.files)
        #print(pyfuse3.ROOT_INODE)
        #print(len(self.files))

    async def getattr(self, inode, ctx=None):
        print(f"[getattr] {inode}")
        entry = pyfuse3.EntryAttributes()
        if inode == pyfuse3.ROOT_INODE:
            entry.st_mode = (stat.S_IFDIR | 0o755)
            entry.st_size = 0
        elif inode - pyfuse3.ROOT_INODE > len(self.files):
            print("hophophop")
            raise pyfuse3.FUSEError(errno.ENOENT)
        else:
            file = self.files[inode - pyfuse3.ROOT_INODE - 1]
            if file.type == "file":
                entry.st_mode = (stat.S_IFREG | 0o644)
            elif file.type == "directory":
                entry.st_mode = (stat.S_IFDIR | 0o755)
            else:
                print(f"Unknown file type `{file.type}`")
                raise pyfuse3.FUSEError(errno.ENOENT)

            entry.st_size = file.size

        # TODO: use correct date
        stamp = int(1438467123.985654 * 1e9)
        entry.st_atime_ns = stamp
        entry.st_ctime_ns = stamp
        entry.st_mtime_ns = stamp
        entry.st_gid = os.getgid()
        entry.st_uid = os.getuid()
        entry.st_ino = inode

        return entry

    async def lookup(self, parent_inode, name, ctx=None):
        print(f"[lookup] {parent_inode} {name}")
        if parent_inode != pyfuse3.ROOT_INODE:# or name != self.hello_name:
            raise pyfuse3.FUSEError(errno.ENOENT)
        for (i, f) in enumerate(self.files):
            if f.filename == name:
                print(f"[lookup] `{name}` found!")
                return await self.getattr(pyfuse3.ROOT_INODE+i+1)
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def opendir(self, inode, ctx):
        print(f"[opendir] {inode}")
        if inode != pyfuse3.ROOT_INODE:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return inode

    async def readdir(self, fh, start_id, token):
        print(f"[readdir] {fh} {start_id} {token}")
        assert fh == pyfuse3.ROOT_INODE

        for (file_id, file) in enumerate(self.files[start_id:]):
            pyfuse3.readdir_reply(
                token, file.filename, await self.getattr(pyfuse3.ROOT_INODE + start_id + file_id + 1), 1+start_id+file_id)

        return

    async def open(self, inode, flags, ctx):
        print(f"[open] {inode} {flags}")
        #if inode != self.hello_inode:
        #    raise pyfuse3.FUSEError(errno.ENOENT)
        if flags & os.O_RDWR or flags & os.O_WRONLY:
            raise pyfuse3.FUSEError(errno.EACCES)
        return pyfuse3.FileInfo(fh=inode)

    async def read(self, fh, off, size):
        print(f"[read] {fh} {off} {size}")
        assert fh - pyfuse3.ROOT_INODE - 1 >= 0 and fh - pyfuse3.ROOT_INODE - 1 < len(self.files)
        return self.files[fh - pyfuse3.ROOT_INODE - 1].download()[off:off+size]

    async def _create(self, inode_p, name, mode, ctx, rdev=0, target=None):
        print(f"[_create] {inode_p} {name} {mode}")
        if (await self.getattr(inode_p)).st_nlink == 0:
            log.warning('Attempted to create entry %s with unlinked parent %d',
            name, inode_p)
            raise FUSEError(errno.EINVAL)

        now_ns = int(time.time() * 1e9)
        inode = self.next_inode
        self.next_inode += 1

        entry = pyfuse3.EntryAttributes()
        entry.st_mode = mode
        entry.st_size = 0

        entry.st_atime_ns = now_ns
        entry.st_ctime_ns = now_ns
        entry.st_mtime_ns = now_ns
        entry.st_gid = os.getgid()
        entry.st_uid = os.getuid()
        entry.st_ino = inode

        self.files.append(ZenodoFile(name, 0, "directory", ""))
        #self.cursor.execute('INSERT INTO inodes (uid, gid, mode, mtime_ns, atime_ns, '
        #    'ctime_ns, target, rdev) VALUES(?, ?, ?, ?, ?, ?, ?, ?)',
        #(ctx.uid, ctx.gid, mode, now_ns, now_ns, now_ns, target, rdev))
        
        #self.db.execute("INSERT INTO contents(name, inode, parent_inode) VALUES(?,?,?)",
        #(name, inode, inode_p))
        return await self.getattr(inode)

    async def mkdir(self, inode_p, name, mode, ctx):
        print(f"[mkdir] {inode_p} {name} {mode}")
        return await self._create(inode_p, name, mode, ctx)


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

