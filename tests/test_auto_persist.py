"""
Test for reactive/bridge.py auto_persist_effect — requires real PG.

This specifically tests that auto_persist_effect uses the Active Record API
correctly (using obj.save() instead of the old conn.update() method).
"""

import tempfile
from dataclasses import dataclass

import pytest
from reactive import computed
from reactive.bridge import auto_persist_effect
from store.admin import StoreServer
from store.base import Storable
from store.connection import UserConnection


@dataclass
class Sensor(Storable):
    name: str = ""
    value: float = 0.0
    threshold: float = 100.0

    @computed
    def above_threshold(self) -> bool:
        return self.value > self.threshold


@pytest.fixture(scope="module")
def server():
    tmp_dir = tempfile.mkdtemp(prefix="test_autopersist_")
    srv = StoreServer(data_dir=tmp_dir, admin_password="test_admin_pw")
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture(scope="module")
def conn_info(server):
    return server.conn_info()


@pytest.fixture(scope="module")
def _provision_users(server):
    server.provision_user("ap_user", "ap_pw")


@pytest.fixture()
def conn(conn_info, _provision_users):
    c = UserConnection(
        user="ap_user", password="ap_pw",
        host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
    )
    yield c
    c.close()


class TestAutoPersistEffect:

    def test_auto_persist_creates_effects(self, conn):
        """auto_persist_effect returns one Effect per @computed."""
        s = Sensor(name="temp", value=50.0, threshold=100.0)
        s.save()
        effects = auto_persist_effect(s, store_conn=conn)
        assert len(effects) >= 1

    def test_auto_persist_persists_value(self, conn):
        """The effect's update path must use save(), not conn.update().

        We call the effect's internal function and verify the value
        was actually persisted to the database. If auto_persist_effect
        still calls conn.update(), the except block swallows the
        AttributeError and the value won't be persisted.
        """
        s = Sensor(name="persist_test", value=50.0, threshold=100.0)
        s.save()
        eid = s.entity_id

        effects = auto_persist_effect(s, store_conn=conn)
        assert len(effects) >= 1

        # Mutate the signal so @computed changes
        s.value = 200.0

        # Directly invoke the effect function to force the persist path
        for eff in effects:
            eff._fn()

        # Verify the change was actually persisted to the database
        refreshed = Sensor.get(eid)  # type: ignore[arg-type]
        assert refreshed.value == 200.0, (
            "auto_persist_effect failed to persist — "
            "likely still calling conn.update() instead of obj.save()"
        )
