import sys, json, time, io
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from botocore.exceptions import ClientError
import normalizer.handler as h

class FakeS3:
    def __init__(self): self.store = {}
    def put_object(self, Bucket, Key, Body, IfNoneMatch=None):
        if IfNoneMatch == "*" and Key in self.store:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.store[Key] = Body
    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.store[Key].encode())}
    def delete_object(self, Bucket, Key): self.store.pop(Key, None)

h.s3_client = FakeS3()
assert h.acquire_lock("normalize", 60) is True                       # first run takes it
assert h.acquire_lock("normalize", 60) is False                      # overlapping run is refused
assert h.acquire_lock("compact", 60) is True                         # different job, independent
h.release_lock("normalize"); assert h.acquire_lock("normalize", 60)  # released -> available again
k = h._lock_key("normalize"); h.s3_client.store[k] = json.dumps({"expires": time.time() - 5})
assert h.acquire_lock("normalize", 60) is True                       # stale lock (crashed run) taken over
h.s3_client.store[k] = "not json"; assert h.acquire_lock("normalize", 60) is True   # unreadable -> stale
# a run that fails still releases the lock
h.release_lock("normalize")
class Ctx: get_remaining_time_in_millis = staticmethod(lambda: 900000)
def boom(): raise RuntimeError("x")
try: h._run_locked("normalize", Ctx(), boom)
except RuntimeError: pass
assert k not in h.s3_client.store, "lock must be released after a failure"
# skipped result while held
assert h.acquire_lock("normalize", 60); r = h._run_locked("normalize", Ctx(), lambda: (_ for _ in ()).throw(AssertionError("must not run")))
assert r == {"status": "skipped"}, r
print("lock tests passed; key:", k)
