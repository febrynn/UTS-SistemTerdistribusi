# import asyncio
# import httpx
# from datetime import datetime

# async def main():
#     events = []
#     for i in range(5000):
#         event_id = str(i if i < 4000 else i - 1000)  # 20% duplikat
#         events.append({
#             "topic": "test",
#             "event_id": event_id,
#             "timestamp": datetime.utcnow().isoformat() + "Z",
#             "source": "generator",
#             "payload": {"value": i}
#         })

#     async with httpx.AsyncClient() as client:
#         r = await client.post("http://127.0.0.1:8000/publish", json={"events": events})
#         print(r.status_code, r.json())

# asyncio.run(main())
