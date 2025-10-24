import pytest
import pytest_asyncio
import asyncio
from httpx import AsyncClient
from datetime import datetime, timezone
from src.main import app, get_store
from src.store import DedupStore

# ============================
# Helper: buat event timezone-aware
# ============================
def make_event(topic="t1", eid="e1"):
    return {
        "topic": topic,
        "event_id": eid,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {"x": 1}
    }

# ============================
# Fixture: client FastAPI + DB sementara
# ============================
@pytest_asyncio.fixture
async def client(tmp_path):
    db_file = tmp_path / "dedup_test.db"
    test_store = DedupStore(str(db_file))
    await test_store.init()

    # Override dependency injection
    app.dependency_overrides[get_store] = lambda: test_store

    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac

    await test_store.close()

# ============================
# Test deduplication & persistence
# ============================
@pytest.mark.asyncio
async def test_persistence_across_restart(client):
    # Kirim event pertama
    e = make_event(eid="persist1")
    r = await client.post("/publish", json=e)
    assert r.status_code == 200

    # Tunggu sebentar agar processing selesai
    await asyncio.sleep(0.1)

    # Kirim event yang sama lagi → harus terdeteksi duplicate
    r = await client.post("/publish", json=e)
    assert r.status_code == 200

    # Tunggu sebentar
    await asyncio.sleep(0.1)

    # Ambil stats
    stats = (await client.get("/stats")).json()
    print("STATS AFTER PERSISTENCE TEST:", stats)

    # Hanya 1 unique event yang diproses
    assert stats["unique_processed"] == 1
    assert stats["duplicate_dropped"] >= 1
