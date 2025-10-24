import asyncio
import os
import tempfile
import pytest
import pytest_asyncio
from httpx import AsyncClient
from src.main import app
import shutil


@pytest.fixture  # ✅ sinkron, pakai pytest biasa
def tmp_db_dir(tmp_path):
    d = tmp_path / "data"
    d.mkdir()
    yield d


@pytest_asyncio.fixture
async def client(tmp_path, monkeypatch):
    db_file = tmp_path / "dedup.db"
    monkeypatch.setenv("DEDUP_DB", str(db_file))  # ✅ ini penting
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac

