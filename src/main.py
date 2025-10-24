from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timezone
import asyncio
import json
import os

from src.store import DedupStore 

app = FastAPI()
DB_PATH = os.getenv("DEDUP_DB", "dedup.db")

store = DedupStore(DB_PATH)
lock = asyncio.Lock()

def get_store() -> DedupStore:
    return store

class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: Dict[str, Any]

class EventsBatch(BaseModel):
    events: List[Event]

@app.on_event("startup")
async def startup_event():
    await store.init()

@app.on_event("shutdown")
async def shutdown_event():
    await store.close()

@app.post("/publish")
async def publish(
    data: Union[Event, EventsBatch],
    store: DedupStore = Depends(get_store),
):
    events = [data] if isinstance(data, Event) else data.events
    
    unique_count = 0
    duplicate_count = 0

    async with lock:
        for ev in events:
            try:
                datetime.fromisoformat(ev.timestamp.replace("Z", "+00:00"))
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid timestamp")

            is_unique = await store.mark_processed(
                ev.topic,
                ev.event_id,
                ev.timestamp,
                ev.source,
                json.dumps(ev.payload),
            )
            
            if is_unique:
                unique_count += 1
            else:
                duplicate_count += 1

    return {
        "status": "ok",
        "count": len(events),
        "unique_processed": unique_count,
        "duplicate_dropped": duplicate_count
    }

@app.get("/events")
async def get_events(
    topic: Optional[str] = None,
    store: DedupStore = Depends(get_store),
):
    return await store.list_events(topic)

@app.get("/stats")
async def get_stats(store: DedupStore = Depends(get_store)):
    topics = await store.topics()
    received, unique, duplicates = await store.get_stats() 
    return {
        "received": received,
        "unique_processed": unique,
        "duplicate_dropped": duplicates,
        "uptime": datetime.now(timezone.utc).isoformat(),
        "topics": topics
    }
