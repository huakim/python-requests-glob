from requests_file import FileAdapter, readTextFile

import math
from sortedcontainers import SortedSet
import glob2 as glob
from os import getcwd
from os.path import join


class F:
    def __init__(self, file):
        self.file = file

    def __getattr__(self, name):
        return getattr(self.file, name)

    def __eq__(self, a) -> bool:
        return (self.begin <= a) and (self.end > a)

    def __gt__(self, a) -> bool:
        return self.begin > a

    def __lt__(self, a) -> bool:
        return not (self >= a)

    def __ge__(self, a) -> bool:
        return (self.begin > a) or (self.end > a)

    def __le__(self, a) -> bool:
        return not (self > a)

    def __ne__(self, a) -> bool:
        return not (self == a)

    def __hash__(self) -> int:
        begin = self.begin
        end = self.end
        return ((begin + end) << 4) + (end - begin)


def FilesIO(file_names):
    files = SortedSet()
    length = 0
    prev = None

    for index, file in enumerate(file_names):
        file = io.open(file, "rb")
        file.index = index
        if prev != None:
            prev.next = file
        file.prev = prev
        file.next = None
        prev = file
        st_size = file.len = os.fstat(file.fileno()).st_size
        file.begin = length
        length += st_size
        file.end = length
        files.add(F(file))

    current_offset = 0
    current_file = files[0]
    isclosed = False
    methods = {}

    def add(func):
        name = func.__name__
        func = staticmethod(func)
        methods[name] = func
        return func

    @add
    def close():
        nonlocal isclosed, files
        if not isclosed:
            for i in files:
                i.close()
            isclosed = True

    @add
    def closed():
        nonlocal isclosed
        return isclosed

    @add
    def readable():
        if closed():
            raise ValueError("I/O operation on closed file")
        return True

    methods["seekable"] = readable

    def tostring(self):
        return "<FilesIO(" + repr(file_names) + ") at " + hex(id(self)) + ">"

    @add
    def fileno():
        return OSError("not supported")

    @add
    def flush():
        pass

    @add
    def isatty():
        return False

    @add
    def tell():
        nonlocal current_offset
        return current_offset

    @add
    def writable():
        return not readable()

    @add
    def seek(offset, whence=0):
        nonlocal current_offset, length
        if whence == 1:
            offset += current_offset
        elif whence == 2:
            offset += length
        if offset < 0:
            offset = 0
        return set_offset(offset)

    def set_offset(offset):
        nonlocal current_offset, current_file, files
        if offset > length:
            current_offset = length
            current_file = files[-1]
        elif offset == 0:
            current_file = files[0]
            current_file.seek(0)
            current_offset = 0
            return 0
        else:
            current_offset = offset
            current_file = search_file(offset)
        current_file.seek(current_offset - current_file.begin)
        return current_offset

    def search_file(offset):
        nonlocal files
        try:
            return files[files.index(offset)]
        except ValueError:
            return files[-1]

    @add
    def readinto(ret):
        nonlocal current_file, current_offset, length
        readable()
        size = len(ret)
        default_size = size
        view = memoryview(ret)
        index = 0
        while size > 0:
            available = current_file.end - current_offset
            if size > available:
                size -= available
                current_file.readinto(view[index : (index + available)])
                index += available
                next_file = current_file.next
                current_offset = current_file.end
                if next_file is None:
                    return default_size - size
                else:
                    next_file.seek(0)
                    current_file = next_file
            else:
                current_file.readinto(view[index : (index + size)])
                current_offset += size
                return default_size

    @add
    def read(size=-1):
        nonlocal current_offset, length
        if size < 0:
            size = length - current_offset
        ret = bytearray(size)
        size = readinto(ret)
        return bytes(memoryview(ret)[:size])

    @add
    def readall():
        nonlocal length
        return read(length)

    methods["len"] = length
    methods["__str__"] = tostring
    methods["__repr__"] = tostring
    return type("FilesIO", (io.RawIOBase,), methods)()


class __GlobAdapter:
    def __init__(self, netloc_paths, **kwargs):
        __def_query = {
            "glob": True,
            "merge": 1,
        }
        __def_query.update(kwargs)
        self.__netloc_paths = netloc_paths
        self.__def_query = __def_query

    def get_flag(self, query, name) -> bool:
        h = str(query.get(name, [""])[-1]).lower()
        if h in ["yes", "enable", "y", "true", "1", "true"]:
            return True
        elif h in ["no", "disable", "n", "false", "0", "false"]:
            return False
        else:
            return bool(self.__def_query.get(name))

    def get_flag_val(self, query, name):
        return query.get(name, [self.__def_query.get(name)])[-1]

    def get_flag_val_strict(self, query, name, value_type=int):
        try:
            return value_type(str(query[name][-1]))
        except Exception:
            val = self.__def_query.get(name, 1)
            if type(val) != value_type:
                return value_type(str(val))
            else:
                return val

    def open_raw(self, resp):
        # Check for query parameters
        query = resp.query_params
        # Check for file path
        path = resp.file_path
        # If got 'current directory' netloc, then set parent dir as current
        if resp.url_netloc == ".":
            func = self.__netloc_paths.get(resp.url_netloc)
            if callable(func):
                func = func()
            if func:
                path = func + path
                resp.file_path = path
                resp.url_netloc = "localhost"
        # get merge query parameter
        merge = self.get_flag_val_strict(query, "merge", int)
        if merge < 1:
            # set merge parameter to infinitive
            merge = math.inf
        # get glob query parameter
        if self.get_flag(query, "glob"):
            # search files with glob
            files = glob.glob(
                str(path),
                include_hidden=self.get_flag(query, "glob_include_hidden"),
                recursive=self.get_flag(query, "glob_recursive"),
            )
            # get length of files
            filelen = len(files)
            if filelen > merge:
                files = files[:merge]
                filelen = merge

            if len(files) == 1:
                resp.file_path = files[0]

            elif len(files) > 1:
                filesio = FilesIO(files)
                readTextFile(resp, filesio, filesio.len)


def createGlobAdapter(
    adapter: FileAdapter, netloc_paths: dict = {}, **kwargs
) -> FileAdapter:
    gl = __GlobAdapter(netloc_paths, **kwargs)
    adapter.add_handler(gl.open_raw)
    return adapter


def GlobAdapter(
    set_content_length: bool = True, netloc_paths: dict = {}, **kwargs
) -> FileAdapter:
    return createGlobAdapter(FileAdapter(set_content_length), netloc_paths, **kwargs)
