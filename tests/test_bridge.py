"""
Tests for the Deephaven ↔ Store Bridge.

All tests use REAL Deephaven (in-process JVM) and REAL embedded PostgreSQL.
No mocks.

Covers:
- Type mapping: infer_schema, extract_row
- StoreBridge: register, table, event dispatch, predicate filters
- Full round-trip: Storable.save() → PG NOTIFY → bridge → DH table
"""

import os
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# DH JVM is started by conftest.py before test collection — safe to import.
from bridge.store_bridge import StoreBridge
from bridge.type_mapping import extract_row, infer_schema
from store.admin import StoreServer
from store.base import Storable
from store.connection import UserConnection
from store.subscriptions import ChangeEvent
from streaming import flush as streaming_flush


def _flush_dh():
    """Flush the streaming update graph so write_row() results are visible."""
    streaming_flush()
    time.sleep(0.2)


# ── Test models ───────────────────────────────────────────────────────────

@dataclass
class Widget(Storable):
    name: str = ""
    color: str = ""
    weight: float = 0.0


@dataclass
class Gadget(Storable):
    label: str = ""
    price: float = 0.0
    quantity: int = 0


@dataclass
class RichItem(Storable):
    title: str = ""
    amount: float = 0.0
    created: datetime | None = None
    active: bool = True
    notes: str | None = None


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def store_server():
    tmp_dir = tempfile.mkdtemp(prefix="test_bridge_")
    srv = StoreServer(data_dir=tmp_dir, admin_password="test_admin_pw")
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture(scope="module")
def conn_info(store_server):
    return store_server.conn_info()


@pytest.fixture(scope="module")
def _provision_users(store_server):
    store_server.provision_user("bridge_user", "bridge_pw")


@pytest.fixture(scope="module")
def client(conn_info, _provision_users):
    c = UserConnection(
        user="bridge_user", password="bridge_pw",
        host=conn_info["host"], port=conn_info["port"], dbname=conn_info["dbname"],
    )
    yield c
    c.close()


# ═════════════════════════════════════════════════════════════════════════
# Type Mapping Tests
# ═════════════════════════════════════════════════════════════════════════

class TestTypeMapping:

    def test_infer_schema_basic_types(self):
        schema = infer_schema(Widget)
        # Domain columns
        assert schema["name"] is str
        assert schema["color"] is str
        assert schema["weight"] is float

    def test_infer_schema_int_type(self):
        schema = infer_schema(Gadget)
        assert schema["quantity"] is int
        assert schema["price"] is float
        assert schema["label"] is str

    def test_infer_schema_optional_decimal_datetime(self):
        schema = infer_schema(RichItem)
        assert schema["amount"] is float               # field is float, not Decimal
        assert schema["created"] is datetime          # Optional[datetime] → datetime
        assert schema["active"] is bool
        assert schema["notes"] is str                  # Optional[str] → str

    def test_infer_schema_metadata_columns(self):
        schema = infer_schema(Widget)
        keys = list(schema.keys())
        # Metadata columns come first
        assert keys[0] == "EntityId"
        assert keys[1] == "Version"
        assert keys[2] == "EventType"
        assert keys[3] == "State"
        assert keys[4] == "UpdatedBy"
        assert keys[5] == "TxTime"
        # Then domain columns
        assert "name" in keys
        assert "color" in keys
        assert "weight" in keys

    def test_extract_row_values(self, client):
        w = Widget(name="bolt", color="silver", weight=0.5)
        w.save()
        columns = ["EntityId", "Version", "EventType", "State",
                    "UpdatedBy", "TxTime", "name", "color", "weight"]
        row = extract_row(w, columns)
        assert row[0] == w.entity_id   # EntityId
        assert row[1] == 1                     # Version
        assert row[2] == "CREATED"             # EventType
        assert row[6] == "bolt"                # name
        assert row[7] == "silver"              # color
        assert row[8] == 0.5                   # weight


# ═════════════════════════════════════════════════════════════════════════
# Bridge + Real Deephaven Tests
# ═════════════════════════════════════════════════════════════════════════

class TestBridgeDH:

    def test_register_creates_dh_table(self, conn_info, _provision_users):
        bridge = StoreBridge(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bridge_user", password="bridge_pw",
            subscriber_id=None,
        )
        bridge.register(Widget)
        tbl = bridge.table(Widget)
        assert tbl is not None
        # Table should exist but be empty
        assert tbl.size == 0

    def test_unregistered_type_raises(self, conn_info, _provision_users):
        bridge = StoreBridge(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bridge_user", password="bridge_pw",
            subscriber_id=None,
        )
        with pytest.raises(KeyError):
            bridge.table(Widget)

    def test_event_writes_to_dh_table(self, conn_info, _provision_users):
        bridge = StoreBridge(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bridge_user", password="bridge_pw",
            subscriber_id=None,
        )
        bridge.register(Widget)
        tbl = bridge.table(Widget)

        # Manually emit a ChangeEvent to the bridge's internal bus
        # (bypasses PG NOTIFY — tests the dispatch logic directly)
        client = UserConnection(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="gear", color="blue", weight=1.2)
        w.save()

        # Start bridge (creates its own UserConnection for read-back)
        bridge.start()

        # Emit event manually
        event = ChangeEvent(
            entity_id=w.entity_id,  # type: ignore[arg-type]
            version=1,
            event_type="CREATED",
            type_name=Widget.type_name(),
            updated_by="bridge_user",
            state=None,
            tx_time=w.tx_time,  # type: ignore[arg-type]
        )
        bridge._dispatch(event)
        _flush_dh()

        # Verify row in DH table
        assert tbl.size == 1
        df = tbl.snapshot()
        assert df["name"].iloc[0] == "gear"
        assert df["color"].iloc[0] == "blue"
        assert abs(df["weight"].iloc[0] - 1.2) < 0.001
        assert df["EntityId"].iloc[0] == w.entity_id

        bridge.stop()
        client.close()

    def test_unregistered_type_ignored(self, conn_info, _provision_users):
        bridge = StoreBridge(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bridge_user", password="bridge_pw",
            subscriber_id=None,
        )
        bridge.register(Widget)
        tbl = bridge.table(Widget)

        client = UserConnection(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        g = Gadget(label="phone", price=999.0, quantity=5)
        g.save()

        bridge.start()

        # Dispatch a Gadget event — Widget bridge should ignore it
        event = ChangeEvent(
            entity_id=g.entity_id,  # type: ignore[arg-type]
            version=1,
            event_type="CREATED",
            type_name=Gadget.type_name(),
            updated_by="bridge_user",
            state=None,
            tx_time=g.tx_time,  # type: ignore[arg-type]
        )
        bridge._dispatch(event)
        _flush_dh()
        assert tbl.size == 0

        bridge.stop()
        client.close()

    def test_filter_passes(self, conn_info, _provision_users):
        bridge = StoreBridge(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bridge_user", password="bridge_pw",
            subscriber_id=None,
        )
        bridge.register(Widget, filter=lambda d: d.get("color") == "red")
        tbl = bridge.table(Widget)

        client = UserConnection(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="valve", color="red", weight=0.3)
        w.save()

        bridge.start()
        event = ChangeEvent(
            entity_id=w.entity_id, version=1,  # type: ignore[arg-type]
            event_type="CREATED", type_name=Widget.type_name(),
            updated_by="bridge_user", state=None, tx_time=w.tx_time,  # type: ignore[arg-type]
        )
        bridge._dispatch(event)
        _flush_dh()

        assert tbl.size == 1
        df = tbl.snapshot()
        assert df["name"].iloc[0] == "valve"

        bridge.stop()
        client.close()

    def test_filter_blocks(self, conn_info, _provision_users):
        bridge = StoreBridge(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bridge_user", password="bridge_pw",
            subscriber_id=None,
        )
        bridge.register(Widget, filter=lambda d: d.get("color") == "red")
        tbl = bridge.table(Widget)

        client = UserConnection(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="pipe", color="green", weight=2.0)
        w.save()

        bridge.start()
        event = ChangeEvent(
            entity_id=w.entity_id, version=1,  # type: ignore[arg-type]
            event_type="CREATED", type_name=Widget.type_name(),
            updated_by="bridge_user", state=None, tx_time=w.tx_time,  # type: ignore[arg-type]
        )
        bridge._dispatch(event)
        _flush_dh()

        assert tbl.size == 0

        bridge.stop()
        client.close()

    def test_custom_columns(self, conn_info, _provision_users):
        # User provides their own column schema (Python types)
        custom_schema = infer_schema(Widget)

        bridge = StoreBridge(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bridge_user", password="bridge_pw",
            subscriber_id=None,
        )
        bridge.register(Widget, columns=custom_schema)
        tbl = bridge.table(Widget)

        client = UserConnection(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="nut", color="brass", weight=0.1)
        w.save()

        bridge.start()
        event = ChangeEvent(
            entity_id=w.entity_id, version=1,  # type: ignore[arg-type]
            event_type="CREATED", type_name=Widget.type_name(),
            updated_by="bridge_user", state=None, tx_time=w.tx_time,  # type: ignore[arg-type]
        )
        bridge._dispatch(event)
        _flush_dh()

        assert tbl.size == 1
        df = tbl.snapshot()
        assert df["name"].iloc[0] == "nut"

        bridge.stop()
        client.close()


# ═════════════════════════════════════════════════════════════════════════
# Full Round-Trip Tests (real PG NOTIFY → bridge → real DH table)
# ═════════════════════════════════════════════════════════════════════════

class TestFullRoundTrip:

    def test_store_write_to_dh_table(self, conn_info, _provision_users):
        """Storable.save() → PG NOTIFY → bridge → row in DH table."""
        bridge = StoreBridge(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bridge_user", password="bridge_pw",
            subscriber_id=None,
        )
        bridge.register(Widget)
        bridge.start()
        tbl = bridge.table(Widget)

        # Give listener a moment to set up LISTEN
        time.sleep(0.3)

        # Write via a SEPARATE client (simulates another process)
        writer_client = UserConnection(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="spring", color="steel", weight=0.05)
        w.save()

        # Wait for NOTIFY → bridge dispatch → DH update
        time.sleep(1.0)
        _flush_dh()

        assert tbl.size >= 1
        df = tbl.snapshot()
        matched = df[df["name"] == "spring"]
        assert len(matched) == 1
        assert matched["color"].iloc[0] == "steel"

        bridge.stop()
        writer_client.close()

    def test_store_update_appends_to_dh_table(self, conn_info, _provision_users):
        """Storable.save() → new row appends to DH table."""
        bridge = StoreBridge(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bridge_user", password="bridge_pw",
            subscriber_id=None,
        )
        bridge.register(Gadget)
        bridge.start()
        tbl = bridge.table(Gadget)
        time.sleep(0.3)

        writer_client = UserConnection(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        g = Gadget(label="tablet", price=499.0, quantity=10)
        g.save()
        time.sleep(1.0)

        # Update the gadget
        g.price = 449.0
        g.save()
        time.sleep(1.0)

        # Both versions should be in the raw table
        _flush_dh()
        assert tbl.size >= 2
        df = tbl.snapshot()
        tablet_rows = df[df["label"] == "tablet"]
        assert len(tablet_rows) >= 2
        # Latest version should have updated price
        latest = tablet_rows.sort_values("Version").iloc[-1]
        assert abs(latest["price"] - 449.0) < 0.01

        bridge.stop()
        writer_client.close()

    def test_multiple_types_route_correctly(self, conn_info, _provision_users):
        """Events for different types go to different DH tables."""
        bridge = StoreBridge(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bridge_user", password="bridge_pw",
            subscriber_id=None,
        )
        bridge.register(Widget)
        bridge.register(Gadget)
        bridge.start()

        widget_tbl = bridge.table(Widget)
        gadget_tbl = bridge.table(Gadget)
        time.sleep(0.3)

        writer_client = UserConnection(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="cam", color="black", weight=0.8)
        g = Gadget(label="lens", price=200.0, quantity=3)
        w.save()
        g.save()
        time.sleep(1.0)

        # Widget table should have the widget, gadget table should have the gadget
        _flush_dh()
        w_df = widget_tbl.snapshot()
        g_df = gadget_tbl.snapshot()
        assert any(w_df["name"] == "cam")
        assert any(g_df["label"] == "lens")
        # No cross-contamination
        assert "label" not in w_df.columns or not any(w_df.get("label", []) == "lens")

        bridge.stop()
        writer_client.close()

    def test_last_by_gives_latest(self, conn_info, _provision_users):
        """table.last_by('EntityId') returns the latest version."""
        bridge = StoreBridge(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bridge_user", password="bridge_pw",
            subscriber_id=None,
        )
        bridge.register(Widget)
        bridge.start()
        raw_tbl = bridge.table(Widget)
        live_tbl = raw_tbl.last_by("EntityId")
        time.sleep(0.3)

        writer_client = UserConnection(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )

        w = Widget(name="shaft", color="grey", weight=3.0)
        w.save()
        time.sleep(1.0)

        w.color = "chrome"
        w.save()
        time.sleep(1.0)

        # Raw table has 2 rows, live table has 1 (latest)
        _flush_dh()
        df_live = live_tbl.snapshot()
        shaft_live = df_live[df_live["name"] == "shaft"]
        assert len(shaft_live) == 1
        assert shaft_live["color"].iloc[0] == "chrome"
        assert shaft_live["Version"].iloc[0] == 2

        bridge.stop()
        writer_client.close()

    def test_durable_checkpoint(self, conn_info, _provision_users):
        """Bridge with subscriber_id uses durable checkpoint."""
        # Start bridge, write an event, stop
        bridge1 = StoreBridge(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bridge_user", password="bridge_pw",
            subscriber_id="test_checkpoint_bridge",
        )
        bridge1.register(Widget)
        bridge1.start()
        tbl1 = bridge1.table(Widget)
        time.sleep(0.3)

        writer_client = UserConnection(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w1 = Widget(name="pin1", color="gold", weight=0.01)
        w1.save()
        time.sleep(1.0)

        _flush_dh()
        initial_count = tbl1.size
        assert initial_count >= 1
        bridge1.stop()

        # Start a NEW bridge with the same subscriber_id
        # It should NOT replay the old event (checkpoint saved)
        bridge2 = StoreBridge(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bridge_user", password="bridge_pw",
            subscriber_id="test_checkpoint_bridge",
        )
        bridge2.register(Widget)
        bridge2.start()
        tbl2 = bridge2.table(Widget)
        time.sleep(0.5)

        # Write a NEW event
        w2 = Widget(name="pin2", color="silver", weight=0.02)
        w2.save()
        time.sleep(1.0)

        # tbl2 should have pin2 but NOT pin1 (checkpoint skipped it)
        _flush_dh()
        df2 = tbl2.snapshot()
        assert any(df2["name"] == "pin2")
        assert not any(df2["name"] == "pin1")

        bridge2.stop()
        writer_client.close()
