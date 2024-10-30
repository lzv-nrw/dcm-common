"""
Test module for the flask-middleware implementation for shared key-value
store usage.
"""

from uuid import uuid4
import threading
from time import sleep

import pytest
import requests

from dcm_common.db import (
    key_value_store_app_factory, JSONFileStore, MemoryStore
)
from dcm_common.services.tests.fixtures import run_service, external_service


@pytest.fixture(name="db_dir")
def _db_dir(temporary_directory):
    return temporary_directory / "db"


@pytest.fixture(name="db")
def _db(db_dir):
    return JSONFileStore(db_dir / str(uuid4()))


def test_db_post(db: JSONFileStore, run_service):
    """Test /db/<key>-POST endpoint."""
    run_service(key_value_store_app_factory(db, "test-db"), port=8080)
    assert db.read("key1") is None
    requests.post("http://localhost:8080/db/key1", json="value1", timeout=1)
    assert db.read("key1") == "value1"


def test_db_push(db: JSONFileStore, run_service):
    """Test /db-POST endpoint."""
    run_service(key_value_store_app_factory(db, "test-db"), port=8080)
    assert len(db.keys()) == 0
    key = requests.post(
        "http://localhost:8080/db", json="value1", timeout=1
    ).text
    assert key
    assert db.read(key) == "value1"


@pytest.mark.parametrize(
    "pop",
    ["", "?pop="],
    ids=["no-pop", "pop"]
)
def test_db_get_key(pop, db: JSONFileStore, run_service):
    """Test /db/<key>-GET endpoint."""
    run_service(key_value_store_app_factory(db, "test-db"), port=8080)
    assert db.read("key1") is None
    # empty db
    response = requests.get("http://localhost:8080/db/key1", timeout=1)
    assert response.status_code == 404
    # single record
    requests.post("http://localhost:8080/db/key1", json="value1", timeout=1)
    response = requests.get(f"http://localhost:8080/db/key1{pop}", timeout=1)
    assert response.json() == "value1"
    response = requests.get("http://localhost:8080/db/key1", timeout=1)
    if pop:
        assert response.status_code == 404
    else:
        assert response.status_code == 200


@pytest.mark.parametrize(
    "pop",
    ["", "?pop="],
    ids=["no-pop", "pop"]
)
def test_db_get(pop, db: JSONFileStore, run_service):
    """Test /db-GET endpoint."""
    run_service(key_value_store_app_factory(db, "test-db"), port=8080)
    assert db.read("key1") is None
    # empty db
    response = requests.get("http://localhost:8080/db", timeout=1)
    assert response.status_code == 404
    # single record
    requests.post("http://localhost:8080/db/key1", json="value1", timeout=1)
    response = requests.get(f"http://localhost:8080/db{pop}", timeout=1)
    assert response.json()["key"] == "key1"
    assert response.json()["value"] == "value1"
    response = requests.get("http://localhost:8080/db/key1", timeout=1)
    if pop:
        assert response.status_code == 404
    else:
        assert response.status_code == 200


def test_db_options(db: JSONFileStore, run_service):
    """Test /db-OPTIONS endpoint."""
    run_service(key_value_store_app_factory(db, "test-db"), port=8080)
    assert len(db.keys()) == 0
    # empty db
    response = requests.options("http://localhost:8080/db", timeout=1)
    assert response.json() == []
    # single record
    requests.post("http://localhost:8080/db/key1", json="value1", timeout=1)
    response = requests.options("http://localhost:8080/db", timeout=1)
    assert response.json() == ["key1"]
    requests.post("http://localhost:8080/db/key2", json="value2", timeout=1)
    response = requests.options("http://localhost:8080/db", timeout=1)
    assert sorted(response.json()) == sorted(["key1", "key2"])


def test_db_delete(db: JSONFileStore, run_service):
    """Test /db/<key>-DELETE endpoint."""
    run_service(key_value_store_app_factory(db, "test-db"), port=8080)
    assert db.read("key1") is None
    # empty db
    response = requests.delete("http://localhost:8080/db/key1", timeout=1)
    assert response.status_code == 200
    # first add then delete single record
    requests.post("http://localhost:8080/db/key1", json="value1", timeout=1)
    response = requests.get("http://localhost:8080/db/key1", timeout=1)
    assert response.json() == "value1"
    response = requests.delete("http://localhost:8080/db/key1", timeout=1)
    assert response.status_code == 200
    response = requests.get("http://localhost:8080/db/key1", timeout=1)
    assert response.status_code == 404


def test_config(db: JSONFileStore, run_service):
    """Test /config-GET endpoint."""
    run_service(key_value_store_app_factory(db, "test-db"), port=8080)
    json = requests.get("http://localhost:8080/config", timeout=1).json()
    assert json["cors"] is False
    assert json["database"]["backend"] == db.__class__.__name__
    assert json["database"]["dir"] == str(db.dir.resolve())


def test_api(db: JSONFileStore, run_service):
    """Test /api-GET endpoint."""
    run_service(key_value_store_app_factory(db, "test-db"), port=8080)
    response = requests.get("http://localhost:8080/api", timeout=1)
    assert response.headers["content-type"] == "application/yaml"
    assert "LZV.nrw - KeyValueStore-API" in response.text


def test_high_load(run_service):
    """Test handling of concurrent requests."""
    run_service(
        key_value_store_app_factory(MemoryStore(), "test-db"),
        port=8080
    )
    nthreads = 100
    nmessages = 10
    def producer(index):
        def _(n):
            for task in range(n):
                token = f"{index}.{task}"
                requests.post(
                    f"http://localhost:8080/db/{token}",
                    json=token,
                    timeout=1
                )
        return _
    consumed = {}
    def consumer():
        def _():
            exit_counter = 0
            while True:
                response = requests.get(
                    "http://localhost:8080/db?pop=", timeout=1
                )
                if response.status_code == 404:
                    exit_counter += 1
                    if exit_counter > 5:
                        return
                    continue
                json = response.json()
                assert json["key"] == json["value"]
                consumed[json["key"]] = json["value"]
        return _
    producers = [
        threading.Thread(target=producer(t), args=(nmessages,), daemon=True)
        for t in range(nthreads)
    ]
    consumers = [
        threading.Thread(target=consumer(), daemon=True)
        for t in range(nthreads)
    ]
    for p, c in zip(producers, consumers):
        p.run()
        c.run()

    while any(t.is_alive() for t in producers + consumers):
        sleep(0.01)

    assert requests.options("http://localhost:8080/db", timeout=1).json() == []
    assert len(set(consumed.keys())) == nthreads * nmessages
