import asyncio
from datetime import datetime

import pytest
from app import iotdb_client
from app.config import settings
from app.iotdb_client import IoTDBClient
from app.models import SensorReading


def test_iotdb_client_retries_until_success(monkeypatch):
    open_attempts = {"count": 0}

    class FakeSession:
        def open(self):
            open_attempts["count"] += 1
            if open_attempts["count"] < 2:
                raise ConnectionError("IoTDB not ready")

        def close(self):
            pass

    def fake_session_factory(host, port, user, password):
        return FakeSession()

    monkeypatch.setattr(iotdb_client, "Session", fake_session_factory)
    monkeypatch.setattr(settings, "IOTDB_CONNECT_RETRIES", 3)
    monkeypatch.setattr(settings, "IOTDB_CONNECT_BACKOFF_SECONDS", 0.0)

    client = IoTDBClient(host="iotdb", port=6667, user="root", password="root")
    asyncio.run(client.connect())

    assert client.session is not None
    assert open_attempts["count"] == 2


def test_iotdb_client_raises_after_max_retries(monkeypatch):
    class FakeSession:
        def open(self):
            raise ConnectionError("IoTDB not ready")

        def close(self):
            pass

    def fake_session_factory(host, port, user, password):
        return FakeSession()

    monkeypatch.setattr(iotdb_client, "Session", fake_session_factory)
    monkeypatch.setattr(settings, "IOTDB_CONNECT_RETRIES", 2)
    monkeypatch.setattr(settings, "IOTDB_CONNECT_BACKOFF_SECONDS", 0.0)

    client = IoTDBClient(host="iotdb", port=6667, user="root", password="root")

    with pytest.raises(RuntimeError, match="Unable to connect to IoTDB"):
        asyncio.run(client.connect())


def test_iotdb_client_write_batch_calls_insert_records(monkeypatch):
    recorded = {}

    class FakeSession:
        def set_storage_group(self, group_name):
            recorded["storage_group"] = group_name

        def insert_records(self, device_ids, times, measurements_lst, types_lst, values_lst):
            recorded["device_ids"] = device_ids
            recorded["times"] = times
            recorded["measurements_lst"] = measurements_lst
            recorded["types_lst"] = types_lst
            recorded["values_lst"] = values_lst

        def close(self):
            pass

    client = IoTDBClient(host="iotdb", port=6667, user="root", password="root")
    client.session = FakeSession()

    reading = SensorReading(
        device_id="sensor1",
        timestamp=datetime(2026, 4, 18, 0, 11, 23),
        temperature=12.3,
        humidity=45.6,
        pressure=1013.2,
    )

    asyncio.run(client.write_batch([reading]))

    assert recorded["storage_group"] == "root.factory"
    assert recorded["device_ids"] == ["root.factory.sensor1"]
    assert recorded["times"] == [int(reading.timestamp.timestamp() * 1000)]
    assert recorded["measurements_lst"] == [["temperature", "humidity", "pressure"]]
    assert recorded["types_lst"] == [[3, 3, 3]]
    assert recorded["values_lst"] == [[12.3, 45.6, 1013.2]]


def test_iotdb_client_ignores_existing_storage_group(monkeypatch):
    recorded = {"insert_called": False}

    class FakeSession:
        def set_storage_group(self, group_name):
            raise Exception("root.factory has already been created as database")

        def insert_records(self, device_ids, times, measurements_lst, types_lst, values_lst):
            recorded["insert_called"] = True

        def close(self):
            pass

    client = IoTDBClient(host="iotdb", port=6667, user="root", password="root")
    client.session = FakeSession()

    reading = SensorReading(
        device_id="sensor1",
        timestamp=datetime(2026, 4, 18, 0, 11, 23),
        temperature=12.3,
        humidity=45.6,
        pressure=1013.2,
    )

    asyncio.run(client.write_batch([reading]))

    assert recorded["insert_called"] is True
