"""Exclusive local acquisition writer and atomic file replacement."""
import os
import uuid
from contextlib import contextmanager
from pathlib import Path

from msharesift.model import Unavailable


class WriterLock:
    """Token-owned lock; stale locks are never stolen automatically."""
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.locked = False
        self.writer_token = None
        self.retain_lock = False

    def _owns_lock(self):
        try:
            with self.lock_path.open("rb") as stream:
                return self.writer_token is not None and stream.read(33) == self.writer_token.encode()
        except OSError:
            return False

    @contextmanager
    def writer(self):
        try:
            handle = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            raise Unavailable("writer_lock_present") from exc
        self.writer_token = uuid.uuid4().hex
        self.retain_lock = False
        try:
            with os.fdopen(handle, "wb") as stream:
                stream.write(self.writer_token.encode())
                stream.flush()
            self.locked = True
            yield self
        finally:
            self.locked = False
            if not self.retain_lock and self._owns_lock():
                self.lock_path.unlink(missing_ok=True)
            self.writer_token = None

def atomic_write(path: Path, data: bytes):
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
