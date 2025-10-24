import pytest, asyncio, time, random
from datetime import datetime

@pytest.mark.asyncio
async def test_stress_small(client):
    N = 5000
    dup_rate = 0.2
    events = []
    for i in range(N):
        eid = f"evt-{i if random.random()>dup_rate else (i - (i%10))}"
        events.append({
            "topic": "stress",
            "event_id": eid,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": "stress",
            "payload": {"i": i}
        })
    start = time.time()
    r = await client.post("/publish", json={"events": events})
    assert r.status_code == 200
    # wait until queue drained (max timeout)
    await asyncio.sleep(2)  # increase if needed on real env
    elapsed = time.time() - start
    assert elapsed < 30  # should be responsive; in CI might adjust
