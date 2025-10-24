import aiosqlite
import asyncio
import os
from datetime import datetime, timezone

class DedupStore:
    def __init__(self, db_path=None):
        self.db_path = db_path or os.getenv("DEDUP_DB", "./data/dedup.db")
        self.conn = None
        self._init_lock = asyncio.Lock()

    async def init(self):
        async with self._init_lock:
            if self.conn is None:
                # KUNCI PERBAIKAN: Hanya panggil os.makedirs jika path direktori tidak kosong
                db_dir = os.path.dirname(self.db_path)
                if db_dir: 
                    os.makedirs(db_dir, exist_ok=True)
                    
                self.conn = await aiosqlite.connect(self.db_path)
                
                # 1. Membuat tabel 'events' (untuk data unik)
                await self.conn.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        topic TEXT NOT NULL,
                        event_id TEXT NOT NULL,
                        timestamp TEXT,
                        source TEXT,
                        payload TEXT,
                        processed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (event_id) -- Hanya event_id sebagai kunci utama, agar duplikasi di seluruh topik terdeteksi
                    )
                """)
                
                # 2. Membuat tabel 'stats' (untuk metrik counter)
                await self.conn.execute("""
                    CREATE TABLE IF NOT EXISTS stats (
                        name TEXT PRIMARY KEY,
                        value INTEGER
                    )
                """)
                
                # 3. Memastikan counter awal ada di tabel stats
                await self._ensure_stats_counters()
                await self.conn.commit()

    async def _ensure_stats_counters(self):
        """Memastikan semua counter statistik (received, duplicates) ada."""
        counters = ["received", "duplicate_dropped"]
        for name in counters:
            # Gunakan INSERT OR IGNORE agar tidak menimpa jika sudah ada
            await self.conn.execute(
                "INSERT OR IGNORE INTO stats (name, value) VALUES (?, 0)",
                (name,)
            )

    async def close(self):
        if self.conn:
            # Penting untuk persistensi: commit terakhir sebelum menutup
            await self.conn.commit()
            await self.conn.close()
            self.conn = None

    async def mark_processed(self, topic, event_id, timestamp, source, payload_json):
        await self.init()
        is_unique = False
        
        # PERBAIKAN UTAMA: Selalu hitung 'received' (sebelum deduplikasi)
        await self._increment_stat("received")

        try:
            # Mencoba menyisipkan event unik
            await self.conn.execute(
                "INSERT INTO events (topic, event_id, timestamp, source, payload) VALUES (?, ?, ?, ?, ?)",
                (topic, event_id, timestamp, source, payload_json),
            )
            is_unique = True
            
        except aiosqlite.IntegrityError:
            # Duplikat, tidak perlu lakukan apa-apa di tabel events
            # PERBAIKAN: Hitung 'duplicate_dropped'
            await self._increment_stat("duplicate_dropped")
        
        await self.conn.commit()
        return is_unique

    async def _increment_stat(self, name):
        """Helper untuk menaikkan nilai counter di tabel stats."""
        await self.conn.execute(
            "UPDATE stats SET value = value + 1 WHERE name = ?", 
            (name,)
        )

    async def list_events(self, topic=None):
        await self.init()
        query = "SELECT topic, event_id, timestamp, source, payload, processed_at FROM events"
        params = ()
        if topic:
            query += " WHERE topic=?"
            params = (topic,)
        cur = await self.conn.execute(query, params)
        rows = await cur.fetchall()
        return [
            {
                "topic": r[0],
                "event_id": r[1],
                "timestamp": r[2],
                "source": r[3],
                "payload": r[4],
                "processed_at": r[5],
            }
            for r in rows
        ]

    async def topics(self):
        await self.init()
        cur = await self.conn.execute("SELECT DISTINCT topic FROM events")
        rows = await cur.fetchall()
        return [r[0] for r in rows]

    async def get_stats(self):
        await self.init()
        
        # 1. Ambil unique_processed (COUNT dari tabel events)
        async with self.conn.execute("SELECT COUNT(*) FROM events") as cur:
             unique = (await cur.fetchone())[0]

        # 2. Ambil received dan duplicate_dropped dari tabel stats
        stats_map = {}
        async with self.conn.execute("SELECT name, value FROM stats") as cur:
            rows = await cur.fetchall()
            for name, value in rows:
                stats_map[name] = value

        received = stats_map.get("received", 0)
        duplicates = stats_map.get("duplicate_dropped", 0)

        # CATATAN: Karena 'unique' dihitung dari COUNT(events) dan 'received' 
        # dan 'duplicates' dari tabel stats, kita bisa memastikan konsistensi:
        # unique_processed = received - duplicate_dropped
        
        return received, unique, duplicates
