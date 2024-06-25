from requests_file import FileAdapter

import math
import glob2 as glob

from sortedcontainers import SortedSet

if not hasattr(FileAdapter, 'open_raw'):
  from requests.adapters import BaseAdapter
  from requests.compat import urlparse, unquote
  from requests import Response, codes
  import errno
  import stat
  import locale
  from urllib.parse import parse_qs as parse_query

  try:
    from io import BytesIO
  except ImportError:
    from StringIO import StringIO as BytesIO

  import io
  import os

  class FileAdapter(BaseAdapter):
    def __init__(self, set_content_length=True, query={}):
        super(FileAdapter, self).__init__()
        self._set_content_length = set_content_length
    def open_raw(self, path, query):
        raw = io.open(path, 'rb')
        resp_stat = os.fstat(raw.fileno())
        raw.len = resp_stat.st_size
        return raw
    def send(self, request, **kwargs):
        if request.method not in ("GET", "HEAD"):
            raise ValueError("Invalid request method %s" % request.method)
        url_parts = urlparse(request.url)
        url_parts_netloc = url_parts.netloc
        if url_parts_netloc and url_parts_netloc != "localhost" and url_parts_netloc != '.':
            raise ValueError("file: URLs with hostname components are not permitted")

        resp = Response()
        resp.request = request
        try:
            path_parts = [unquote(p) for p in url_parts.path.split("/")]
            while path_parts and not path_parts[0]:
                path_parts.pop(0)
            if any(os.sep in p for p in path_parts):
                raise IOError(errno.ENOENT, os.strerror(errno.ENOENT))
            if path_parts and (
                path_parts[0].endswith("|") or path_parts[0].endswith(":")
            ):
                path_drive = path_parts.pop(0)
                if path_drive.endswith("|"):
                    path_drive = path_drive[:-1] + ":"

                while path_parts and not path_parts[0]:
                    path_parts.pop(0)
            else:
                path_drive = ""
            if url_parts_netloc == '.':
                path = os.path.join(*path_parts)
            elif path_drive and not os.path.splitdrive(path):
                path = os.sep + os.path.join(path_drive, *path_parts)
            else:
                path = path_drive + os.sep + os.path.join(*path_parts)
            raw = self.open_raw(path, parse_query(url_parts.query))
            resp.raw = raw
            resp.raw.release_conn = raw.close
        except IOError as e:
            if e.errno == errno.EACCES:
                resp.status_code = codes.forbidden
            elif e.errno == errno.ENOENT:
                resp.status_code = codes.not_found
            else:
                resp.status_code = codes.bad_request
            resp_str = str(e).encode(locale.getpreferredencoding(False))
            raw = resp.raw = BytesIO(resp_str)
            if self._set_content_length:
                resp.headers["Content-Length"] = len(resp_str)
            resp.raw.release_conn = raw.close
        else:
            resp.status_code = codes.ok
            resp.url = request.url
            if self._set_content_length:
                resp.headers["Content-Length"] = raw.len
        return resp
    def close(self):
        pass

class F:
  def __init__(self, file):
    self.file = file
  def __getattr__(self, name):
    return getattr(self.file, name)
  def __eq__(self, a):
    return (self.begin <= a) and (self.end > a)
  def __gt__(self, a):
    return (self.begin > a)
  def __lt__(self, a):
    return not (self >= a)
  def __ge__(self, a):
    return (self.begin > a) or (self.end > a)
  def __le__(self, a):
    return not (self > a)
  def __ne__(self, a):
    return not (self == a)
  def __hash__(self):
    begin = self.begin
    end = self.end
    return ((begin + end) << 4) + (end - begin)

def FilesIO(file_names):
    files = SortedSet()
    length = 0
    prev = None

    for index, file in enumerate(file_names):
        file = io.open(file, 'rb')
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
            raise ValueError('I/O operation on closed file')
        return True
    methods['seekable'] = readable
    def tostring(self):
        return '<FilesIO('+repr(file_names)+') at '+hex(id(self))+'>'
    @add
    def fileno():
        return OSError('not supported')
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
    def read(size = -1):
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
    methods['len']=length
    methods['__str__'] = tostring
    methods['__repr__'] = tostring
    return type('FilesIO', (io.RawIOBase,),methods)()

class GlobAdapter(FileAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        __def_query = {
            'glob_recursive':True,
            'glob_include_hidden':False,
            'glob':True,
            'merge':1,
        }
        __def_query.update(kwargs)
        self.__def_query = __def_query

    def get_flag(self, query, name):
        h = str(query.get(name, [''])[-1]).lower()
        if h in ['yes','enable','y','true','1','true']:
            return True
        elif h in ['no','disable','n','false','0','false']:
            return False
        else:
            return self.__def_query.get(name)

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

    def open_raw(self, path, query):
        merge = self.get_flag_val_strict(query, 'merge', int)
        if merge < 1:
            merge = math.inf
        if (self.get_flag(query, 'glob')):
            files = glob.glob(path,
                include_hidden = self.get_flag(query, 'glob_include_hidden'),
                recursive = self.get_flag(query, 'glob_recursive'))
            filelen = len(files)
            if filelen > merge:
                files = files[:merge]
                filelen = merge
            if filelen == 0:
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
        else:
            files = [path]
        if len(files) == 1:
            return super().open_raw(files[0], query)
        else:
            return FilesIO(files)
