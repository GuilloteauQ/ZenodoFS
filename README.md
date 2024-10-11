# ZenodoFS

A lazy in-memory FUSE filesystem for locally exploring Zenodo records

## Run

```
nix develop --command python3 zenodofs.py --mnt [MOUNT_POINT] --api-key [ZENODO_API_KEY] [RECORD_ID]
```

For example:

```
nix develop --command python3 zenodofs.py --mnt zenodo_mnt --api-key 1a2b3c4e5f67890 11208389
```

It will mount ZenodoFS at `zenodo_mnt` with the "content" of the record [11208389](https://zenodo.org/records/11208389)


If you don't want to use Nix, you will need Python3, the `requests` and `pyfuse3` python libs.

## How does it work?

Mounting the filesystem will query the information about the files (name, size) but will not query the content.

At every call to `read` on a file the content will be downloaded, and stored *in memory*.

It is also possible to decompress archives.

## Disclaimer

I wrote this as a proof of concept in an evening, I am even surpised it works.

Don't expect performance.

Not all syscalls are implemented yet.
