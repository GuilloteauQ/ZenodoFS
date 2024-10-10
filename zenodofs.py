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

def get_files(record, next_available_inode):
    r = requests.get(f"https://zenodo.org/api/records/{record}", params={'access_token': KEY})
    data = r.json()
    files = {}
    with open("result.json", "w") as jf:
        jf.write(json.dumps(data))
    for file in data["files"]:
        print(file["key"])
        files[next_available_inode] = ZenodoFile(next_available_inode, str.encode(file["key"]),  "file", file["links"]["self"], (stat.S_IFREG | 0o644), file["size"], int(time.time() * 1e9))
        #files.append(ZenodoFile(next_available_inode, str.encode(file["key"]), file["size"], "file", file["links"]["self"]))
        next_available_inode += 1
    return files, next_available_inode


class ZenodoFS(pyfuse3.Operations):
    def __init__(self, record):
        super(ZenodoFS, self).__init__()
        self.record = record

        #self.root = ZenodoDirectory("root", pyfuse3.ROOT_INODE)
        #self.root.files = get_files(self.record)

        #self.files = get_files(self.record)
        self.next_available_inode = pyfuse3.ROOT_INODE + 1
        self.current_folder = pyfuse3.ROOT_INODE
        self.files = {}
        self.files[pyfuse3.ROOT_INODE], self.next_available_inode = get_files(self.record, self.next_available_inode)


        #self.next_inode = pyfuse3.ROOT_INODE + len(self.files)
        #print(pyfuse3.ROOT_INODE)
        #print(len(self.files))

    async def getattr(self, inode, ctx=None):
        print(f"[getattr] {inode}")
        #if inode == pyfuse3.ROOT_INODE:

        if inode == self.current_folder or inode == pyfuse3.ROOT_INODE:
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
        #elif inode not in self.files[self.current_folder].keys():
        #    print("hophophop")
        #    raise pyfuse3.FUSEError(errno.ENOENT)
        else:
            for k in self.files.keys():
                if inode in self.files[k].keys():
                    return self.files[k][inode].entry
            #return self.files[self.current_folder][inode].entry
        #file = self.files[self.current_folder][inode]
            #if file.type == "file":
            #    entry.st_mode = (stat.S_IFREG | 0o644)
            #elif file.type == "directory":
            #    entry.st_mode = (stat.S_IFDIR | 0o755)
            #else:
            #    print(f"Unknown file type `{file.type}`")
            #    raise pyfuse3.FUSEError(errno.ENOENT)

            #entry.st_size = file.size


        print("WTF?")

    async def lookup(self, parent_inode, name, ctx=None):
        print(f"[lookup] {parent_inode} {name}")
        #if parent_inode != pyfuse3.ROOT_INODE:# or name != self.hello_name:
        if parent_inode != self.current_folder:# or name != self.hello_name:
            raise pyfuse3.FUSEError(errno.ENOENT)
        for (i, f) in self.files[self.current_folder].items():
            #for (i, f) in self.files[parent_inode].items():
            if f.filename == name:
                print(f"[lookup] `{name}` found!")
                return await self.getattr(i)
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def opendir(self, inode, ctx):
        print(f"[opendir] {inode}")
        #if inode != self.current_folder:
        #    raise pyfuse3.FUSEError(errno.ENOENT)
        if inode not in self.files.keys():
            raise pyfuse3.FUSEError(errno.ENOENT)
        self.current_folder = inode
        return inode

    async def readdir(self, fh, start_id, token):
        print(f"[readdir] {fh} {start_id} {token}")
        #assert fh == pyfuse3.ROOT_INODE
        assert fh == self.current_folder

        for (i, file) in list(self.files[self.current_folder].items())[start_id:]:
            pyfuse3.readdir_reply(
                token, file.filename, await self.getattr(i), i)

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
        #assert fh - pyfuse3.ROOT_INODE - 1 >= 0 and fh - pyfuse3.ROOT_INODE - 1 < len(self.files)
        #return self.files[fh - pyfuse3.ROOT_INODE - 1].download()[off:off+size]
        return self.files[self.current_folder][fh].download()[off:off+size]

    async def _create(self, inode_p, name, mode, ctx, rdev=0, target=None):
        print(f"[_create] {inode_p} {name} {mode}")
        if (await self.getattr(inode_p)).st_nlink == 0:
            log.warning('Attempted to create entry %s with unlinked parent %d',
            name, inode_p)
            raise FUSEError(errno.EINVAL)

        inode = self.next_available_inode
        self.next_available_inode += 1

        #if inode_p not in self.files.keys():
        #    self.files[inode_p] = {}
        #self.files[inode_p][inode] = ZenodoFile(inode, name, "directory", "", mode, 0, int(time.time() * 1e9))
        #self.files[self.current_folder][inode] = ZenodoFile(inode, name, "directory", "", mode, 0, int(time.time() * 1e9))
        return await self.getattr(inode)

    async def mkdir(self, inode_p, name, mode, ctx):
        print(f"[mkdir] {inode_p} {name} {mode}")
        return await self._create(inode_p, name, mode, ctx)

    async def write(self, fh, offset, buf):
        print(f"[write] {fh} {offset} {len(buf)}")
        current_content = self.files[self.current_folder][fh].content
        self.files[self.current_folder][fh].content = current_content[:offset] + buf + current_content[offset+len(buf):]
        self.files[self.current_folder][fh].entry.st_size += len(buf)
        return len(buf)

    async def create(self, inode_parent, name, mode, flags, ctx):
        print(f"[create] {inode_parent} {name} {mode} {flags}")
        inode = self.next_available_inode
        self.next_available_inode += 1
        if inode_parent not in self.files.keys():
            self.files[inode_parent] = {}

        self.files[self.current_folder][inode] = ZenodoFile(inode, name, "file", "", mode, 0, int(time.time() * 1e9))
        #self.files[inode_parent][inode] = ZenodoFile(inode, name, "file", "", mode, 0, int(time.time() * 1e9))
        entry = self.files[self.current_folder][inode].entry
        #entry = self.files[inode_parent][inode].entry
        return (pyfuse3.FileInfo(fh=entry.st_ino), entry)

    async def setattr(self, inode, attr, fields, fh, ctx):
        print(f"[setattr] {inode} {attr} {fields} {fh}")
        if fields.update_size:
            print("update_size")
            self.files[self.current_folder][inode].entry.st_size = attr.st_size
        if fields.update_mode:
            print("update_mode")
            self.files[self.current_folder][inode].entry.st_mode = attr.st_mode
        if fields.update_atime:
            print("update atime")
            self.files[self.current_folder][inode].entry.st_atime_ns = attr.st_atime_ns
        if fields.update_mtime:
            print("update mtime")
            self.files[self.current_folder][inode].entry.st_mtime_ns = attr.st_mtime_ns
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

