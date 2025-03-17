from requests.adapters import BaseAdapter
from requests.compat import urlparse, unquote
from urllib.parse import parse_qs
from requests import Response, codes
import errno
import os
import stat
import locale
import io
import math
from sortedcontainers import SortedSet
import glob2 as glob
from io import BytesIO


class FuncStr:
    def __init__(this, func):
        this.func = func

    def __str__(this):
        return this.func()


def setPath(resp, path):
    path = str(path) + resp.file_path
    resp.file_path = path
    resp.url_netloc = "localhost"


def readExceptionObject(resp, e, status_code=codes.internal_server_error):
    """Wraps an Exception object text in a Response object.

    :param resp: The Response` being "sent".
    :param e: The Exception object
    :returns: a Response object containing the file
    """
    # Wrap the error message in a file-like object
    # The error message will be localized, try to convert the string
    # representation of the exception into a byte stream
    resp_str = str(e).encode(locale.getpreferredencoding(False))

    resp.raw = BytesIO(resp_str)
    resp.reason = resp_str
    # set error object
    resp.error = e

    if resp._set_content_length:
        resp.headers["Content-Length"] = len(resp_str)

    # Add release_conn to the BytesIO object
    resp.raw.release_conn = resp.raw.close

    stat_code = False
    try:
        stat_code = not (resp.status_code is None)
    except AttributeError:
        pass

    if not stat_code:
        resp.status_code = status_code

    return resp


def readTextFile(resp, raw=None, length=None):
    """Wraps a file, described in request, in a Response object.

    :param resp: The Response` being "sent".
    :returns: a Response object containing the file text
    """
    # Use io.open since we need to add a release_conn method, and
    # methods can't be added to file objects in python 2.
    if raw is None:
        raw = io.open(resp.file_path, "rb")

    resp.raw = raw
    resp.raw.release_conn = resp.raw.close

    resp.status_code = codes.ok

    # If it's a regular file, set the Content-Length
    if resp._set_content_length:
        if length is None:
            resp_stat = os.fstat(resp.raw.fileno())
            if stat.S_ISREG(resp_stat.st_mode):
                length = resp_stat.st_size
        resp.headers["Content-Length"] = length

    return resp


class FileAdapter(BaseAdapter):
    def __init__(self, set_content_length=True, netloc_paths={}):
        super(FileAdapter, self).__init__()
        self._handlers = []
        self._netlocs = {}
        self._set_content_length = set_content_length
        for key, value in netloc_paths.items():
            self.add_netloc(key, value)

    def add_handler(self, func):
        """Add custom handler for modify response on the fly

        :param func: The handler function being added.
        """
        if callable(func):
            self._handlers.append(func)

    def add_netloc(self, name, func):
        """Add custom netloc handler for monify response on the fly

        :param name: The netloc name
        :param func: The handler function being added
        """
        if callable(func):
            self._netlocs[name] = func
        else:
            self._netlocs[name] = lambda resp: setPath(resp, func)

    def send(self, request, **kwargs):
        """Wraps a file, described in request, in a Response object.

        :param request: The PreparedRequest` being "sent".
        :returns: a Response object containing the file
        """

        # Parse the URL
        url_parts = urlparse(request.url)

        url_netloc = url_parts.netloc

        resp = Response()
        resp.request = request
        resp.url = request.url
        resp.query_params = parse_qs(url_parts.query)
        resp._set_content_length = self._set_content_length

        # Open the file, translate certain errors into HTTP responses
        # Use urllib's unquote to translate percent escapes into whatever
        # they actually need to be
        try:
            # Split the path on / (the URL directory separator) and decode any
            # % escapes in the parts
            path_parts = [unquote(p) for p in url_parts.path.split("/")]

            # Strip out the leading empty parts created from the leading /'s
            while path_parts and not path_parts[0]:
                path_parts.pop(0)

            # If os.sep is in any of the parts, someone fed us some shenanigans.
            # Treat is like a missing file.
            if any(os.sep in p for p in path_parts):
                raise IOError(errno.ENOENT, os.strerror(errno.ENOENT))

            # Look for a drive component. If one is present, store it separately
            # so that a directory separator can correctly be added to the real
            # path, and remove any empty path parts between the drive and the path.
            # Assume that a part ending with : or | (legacy) is a drive.
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

            # Try to put the path back together
            # Join the drive back in, and stick os.sep in front of the path to
            # make it absolute.
            path = path_drive + os.sep + os.path.join(*path_parts)

            # Check if the drive assumptions above were correct. If path_drive
            # is set, and os.path.splitdrive does not return a drive, it wasn't
            # really a drive. Put the path together again treating path_drive
            # as a normal path component.
            if path_drive and not os.path.splitdrive(path):
                path = os.sep + os.path.join(path_drive, *path_parts)

            # Add file_path and url_netloc attributes for using with adapters
            resp.file_path = path
            resp.url_netloc = url_netloc or "localhost"
            resp.raw = None

            func = self._netlocs.get(resp.url_netloc)
            if callable(func):
                func(resp)

            for func in self._handlers:
                func(resp)

            if resp.raw is None:
                method = request.method
                url_netloc = resp.url_netloc
                # Check that the method makes sense. Only support GET
                if method not in ("GET", "HEAD"):
                    resp.status_code = codes.method_not_allowed
                    raise ValueError("Invalid request method %s" % method)
                # Reject URLs with a hostname component
                if url_netloc != "localhost":
                    resp.status_code = codes.forbidden
                    raise ValueError(
                        "file: URLs with hostname components are not permitted"
                    )
                resp = readTextFile(resp)
            return resp
        except IOError as e:
            if e.errno == errno.EACCES:
                status_code = codes.forbidden
            elif e.errno == errno.ENOENT:
                status_code = codes.not_found
            else:
                status_code = codes.bad_request
            # Wrap the error message in a file-like object
            # The error message will be localized, try to convert the string
            # representation of the exception into a byte stream
            return readExceptionObject(resp, e, status_code)
        except Exception as e:
            return readExceptionObject(resp, e)

    def close(self):
        pass


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
        if prev is not None:
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
    def __init__(self, **kwargs):
        __def_query = {
            "glob": True,
            "merge": 1,
        }
        __def_query.update(kwargs)
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
    gl = __GlobAdapter(**kwargs)
    for key, value in netloc_paths.items():
        adapter.add_netloc(key, value)
    adapter.add_handler(gl.open_raw)
    return adapter


def GlobAdapter(
    set_content_length: bool = True, netloc_paths: dict = {}, **kwargs
) -> FileAdapter:
    return createGlobAdapter(FileAdapter(set_content_length), netloc_paths, **kwargs)
