import asyncio
import httpx
from datetime import datetime, timezone
import os
import random

AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://aggregator:8000/publish")

TOTAL_EVENTS = 5000           # jumlah total event
DUPLICATE_RATIO = 0.2         # 20% duplikat

async def send_event(event):
    """Mengirim 1 event ke Aggregator dengan retry sederhana"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(AGGREGATOR_URL, json=event)
            print(f"✅ Sent: {event['event_id']} | Status: {response.status_code}")
    except Exception as e:
        print(f"❌ Error kirim event {event['event_id']}: {e}")

async def main():
    print(f"🚀 Memulai uji skala besar ke {AGGREGATOR_URL}")
    print(f"Total event: {TOTAL_EVENTS}, Duplikasi: {int(TOTAL_EVENTS * DUPLICATE_RATIO)}\n")

    # buat daftar event_id unik
    unique_count = int(TOTAL_EVENTS * (1 - DUPLICATE_RATIO))
    unique_events = [f"event-{i}" for i in range(unique_count)]

    # tambahkan duplikat dari sebagian event_id yang sama
    duplicate_events = random.choices(unique_events, k=int(TOTAL_EVENTS * DUPLICATE_RATIO))
    all_events = unique_events + duplicate_events
    random.shuffle(all_events)  # acak urutannya

    start_time = datetime.now()

    # kirim event satu per satu
    for i, eid in enumerate(all_events, start=1):
        event = {
            "topic": "load.test",
            "event_id": eid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "load-tester",
            "payload": {"seq": i}
        }
        asyncio.create_task(send_event(event))  # kirim asinkron (lebih cepat)
        await asyncio.sleep(0.001)  # jeda kecil agar tidak terlalu membanjiri

    # tunggu semua task selesai
    await asyncio.sleep(5)
    duration = datetime.now() - start_time
    print(f"\n🏁 Selesai kirim {TOTAL_EVENTS} event dalam {duration.total_seconds():.2f} detik.")

if __name__ == "__main__":
    asyncio.run(main())
