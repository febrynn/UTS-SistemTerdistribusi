import asyncio
import json
import logging
from .store import DedupStore
from .stats import Stats

logger = logging.getLogger("consumer")
logger.setLevel(logging.INFO)

class Consumer:
    def __init__(self, queue: asyncio.Queue, store: DedupStore, stats: Stats):
        self.queue = queue
        self.store = store
        self.stats = stats
        self._task = None
        self._running = False

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._running = False
        if self._task:
            await self._task

    async def _run(self):
        while self._running:
            try:
                event = await self.queue.get()
            except asyncio.CancelledError:
                break
            try:
                topic = event["topic"]
                event_id = event["event_id"]
                timestamp = event["timestamp"]
                source = event.get("source", "")
                payload_json = json.dumps(event.get("payload", {}), ensure_ascii=False)
                inserted = await self.store.mark_processed(topic, event_id, timestamp, source, payload_json)
                if inserted:
                    await self.stats.incr_unique(1)
                    logger.info(f"Processed event: {topic} {event_id}")
                    # Here you would call downstream processors (e.g., write to aggregations)
                else:
                    await self.stats.incr_dup(1)
                    logger.warning(f"Duplicate detected and dropped: {topic} {event_id}")
            except Exception as e:
                logger.exception("Error processing event")
            finally:
                self.queue.task_done()
