
import os
import socket
import threading
import time
import errno

"""
NFS ready file lock based on Lockfile (http://code.google.com/p/pylockfile/)
"""

class Error(Exception):
    """
    Base class for other exceptions.

    >>> try:
    ...   raise Error
    ... except Exception:
    ...   pass
    """
    pass

class LockError(Error):
    """
    Base class for error arising from attempts to acquire the lock.

    >>> try:
    ...   raise LockError
    ... except Error:
    ...   pass
    """
    pass


class UnlockError(Error):
    """
    Base class for errors arising from attempts to release the lock.

    >>> try:
    ...   raise UnlockError
    ... except Error:
    ...   pass
    """
    pass

class LockFailed(LockError):
    """Lock file creation failed for some other reason.

    >>> try:
    ...   raise LockFailed
    ... except LockError:
    ...   pass
    """
    pass


class NotLocked(UnlockError):
    """Raised when an attempt is made to unlock an unlocked file.

    >>> try:
    ...   raise NotLocked
    ... except UnlockError:
    ...   pass
    """
    pass


class NotMyLock(UnlockError):
    """Raised when an attempt is made to unlock an unlocked file.

    >>> try:
    ...   raise NotLocked
    ... except UnlockError:
    ...   pass
    """
    pass


class LockFile:
    """Lock file by creating a directory."""
    def __init__(self, path, threaded=True):
        """
        >>> lock = LinkLockFile('somefile')
        >>> lock = LinkLockFile('somefile', threaded=False)
        """
        self.path = path
        self.lock_file = os.path.abspath(path) + "@lock"
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        if threaded:
            t = threading.current_thread()
            # Thread objects in Python 2.4 and earlier do not have ident
            # attrs.  Worm around that.
            ident = getattr(t, "ident", hash(t))
            self.tname = "-%x" % (ident & 0xffffffff)
        else:
            self.tname = ""

        self.unique_name  = "%s.%s.%s%s" % (self.lock_file,
                                            self.hostname,
                                            self.tname,
                                            self.pid)

    def acquire(self, timeout=None):
        end_time = time.time()
        if timeout is not None and timeout > 0:
            end_time += timeout

        if timeout is None:
            wait = 0.1
        else:
            wait = max(0, timeout / 10)

        while True:
            try:
                fp = open(self.unique_name, 'w')
                fp.write(self.unique_name)
            finally:
                fp.close()
            try:                
                os.link(self.unique_name, self.lock_file)
                return
            except OSError as error:            
                if error.errno == errno.ENOENT:                
                    if timeout is not None and time.time() > end_time:
                        if timeout > 0:
                            raise LockTimeout("Timeout waiting to acquire"
                                              " lock for %s" %
                                              self.path)
                        else:
                            raise AlreadyLocked("%s is already locked" %
                                                self.path)
                    time.sleep(timeout/10 if timeout is not None else 0.1)
                #if os.stat(self.unique_name).st_nlink == 2:
                #     return
                
            try:
                os.unlink(self.unique_name)
            except OSError:
                pass
        

    def release(self):
        if not self.is_locked():
            raise NotLocked
        elif not os.path.exists(self.unique_name):
            raise NotMyLock
        try:
            os.unlink(self.lock_file)
        except OSError:
            pass
        try:
            os.unlink(self.unique_name)
        except OSError:
            pass
        
    def is_locked(self):
        return os.path.exists(self.lock_file)

    def i_am_locking(self):
        return (self.is_locked() and
                os.path.exists(self.unique_name))

    def break_lock(self):
        if os.path.exists(self.lock_file):
            for name in os.listdir(self.lock_file):
                os.unlink(os.path.join(self.lock_file, name))
           
            
    def __enter__(self):
        """
        Context manager support.
        """
        self.acquire()
        return self

    def __exit__(self, *_exc):
        """
        Context manager support.
        """
        self.release()
