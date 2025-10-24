import time
import asyncio

class Stats:
    def __init__(self):
        self.start = time.time()
        self.lock = asyncio.Lock()
        self.received = 0
        self.unique_processed = 0
        self.duplicate_dropped = 0

    async def incr_received(self, n=1):
        async with self.lock:
            self.received += n

    async def incr_unique(self, n=1):
        async with self.lock:
            self.unique_processed += n

    async def incr_dup(self, n=1):
        async with self.lock:
            self.duplicate_dropped += n

    def snapshot(self):
        return {
            "received": self.received,
            "unique_processed": self.unique_processed,
            "duplicate_dropped": self.duplicate_dropped,
            "uptime_seconds": int(time.time() - self.start)
        }

