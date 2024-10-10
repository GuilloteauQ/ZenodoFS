import requests
import tarfile
import tempfile
import time

REPO="https://zenodo.org/records/11208389"
RECORD="11208389"
#RECORD="11775182"

def read_key():
    with open("KEY", "r") as f:
        key = f.read()
    return key
KEY=read_key()

class ZenodoFile:
    def __init__(self, filename, size, type, content_url):
        self.filename = filename
        self.size = size
        self.type = type
        self.content_url = content_url



r = requests.get(f"https://zenodo.org/api/records/{RECORD}", params={'access_token': KEY})
data = r.json()
#r = requests.get(f"https://zenodo.org/api/deposit/depositions/{RECORD}/files",
#                 params={'access_token': KEY})
#data = r.json()
#
#
print(data)

files = []
for file in data["files"]:
    print(file)
    files.append(ZenodoFile(file["key"], file["size"], "directory" if file["key"].endswith((".zip", ".tar.gz")) else "file"), file["links"]["self"])

print(files)


#files = data["files"]
#for file in files:
#    filename = file["key"]
#    if filename[-6:] == "tar.gz":
#        print(filename)
#        with tempfile.NamedTemporaryFile() as fp:
#            r = requests.get(f"https://zenodo.org/api/records/{RECORD}/files/{filename}/content", params={'access_token': KEY})
#            fp.write(r.content)
#            tarf = tarfile.open(fp.name, 'r:gz')
#            print(tarf.getnames())
#            print(tarf.list())
#            print(tarf.getmembers())

