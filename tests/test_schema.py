import pytest

@pytest.mark.asyncio
async def test_invalid_timestamp(client):
    bad = {
        "topic": "t",
        "event_id": "id",
        "timestamp": "not-a-time",
        "source": "s",
        "payload": {}
    }

    # ✅ gunakan await
    r = await client.post("/publish", json=bad)
    assert r.status_code in (400, 422)
