import pytest
import pytest_asyncio
import asyncio
from httpx import AsyncClient
from datetime import datetime, timezone
from src.main import app, get_store
from src.store import DedupStore

def make_event(topic="t1", eid="e1"):
    return {
        "topic": topic,
        "event_id": eid,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {"x": 1}
    }

@pytest_asyncio.fixture
async def client(tmp_path):
    db_file = tmp_path / "dedup_test.db"
    test_store = DedupStore(str(db_file))
    await test_store.init()
    app.dependency_overrides[get_store] = lambda: test_store

    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac

    await test_store.close()

@pytest.mark.asyncio
async def test_dedup_basic(client):
    e = make_event()
    r = await client.post("/publish", json=e)
    assert r.status_code == 200
    r = await client.post("/publish", json=e)  # duplicate
    assert r.status_code == 200

    await asyncio.sleep(0.1)
    stats = (await client.get("/stats")).json()
    assert stats["received"] == 2
    assert stats["unique_processed"] == 1
    assert stats["duplicate_dropped"] == 1

@pytest.mark.asyncio
async def test_persistence_across_restart(tmp_path):
    db_file = tmp_path / "dedup_test.db"

    # --- Instance pertama
    store1 = DedupStore(str(db_file))
    await store1.init()
    app.dependency_overrides[get_store] = lambda: store1

    async with AsyncClient(app=app, base_url="http://test") as client:
        e = make_event(eid="persist1")
        await client.post("/publish", json=e)
        await asyncio.sleep(0.1)

    await store1.close()  # flush ke disk

    # --- Simulate restart
    store2 = DedupStore(str(db_file))
    await store2.init()
    app.dependency_overrides[get_store] = lambda: store2

    async with AsyncClient(app=app, base_url="http://test") as client:
        e = make_event(eid="persist1")
        await client.post("/publish", json=e)
        await asyncio.sleep(0.1)

        stats = (await client.get("/stats")).json()
        print("STATS AFTER PERSISTENCE TEST:", stats)
        assert stats["unique_processed"] == 1
        assert stats["duplicate_dropped"] >= 1
