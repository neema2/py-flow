"""
Tests for the Deephaven ↔ Store Bridge.

All tests use REAL Deephaven (in-process JVM) and REAL embedded PostgreSQL.
No mocks.

Covers:
- Type mapping: infer_dh_schema, extract_row
- StoreBridge: register, table, event dispatch, Expr filters
- Full round-trip: StoreClient.write() → PG NOTIFY → bridge → DH table
"""

import os
import sys
import time
import tempfile
import pytest
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# DH JVM is started by conftest.py before test collection — safe to import.
import deephaven.dtypes as dht
from deephaven import DynamicTableWriter
from deephaven import pandas as dhpd
from deephaven.execution_context import get_exec_ctx

from store.base import Storable
from store.server import ObjectStoreServer
from store.client import StoreClient
from store.schema import provision_user
from store.subscriptions import EventBus, ChangeEvent
from bridge.type_mapping import infer_dh_schema, extract_row
from bridge.store_bridge import StoreBridge
from reactive.expr import Field, Const


def _flush_dh():
    """Flush the DH update graph so write_row() results are visible."""
    get_exec_ctx().update_graph.j_update_graph.requestRefresh()
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
    created: Optional[datetime] = None
    active: bool = True
    notes: Optional[str] = None


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def store_server():
    tmp_dir = tempfile.mkdtemp(prefix="test_bridge_")
    srv = ObjectStoreServer(data_dir=tmp_dir, admin_password="test_admin_pw")
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture(scope="module")
def conn_info(store_server):
    return store_server.conn_info()


@pytest.fixture(scope="module")
def _provision_users(store_server):
    admin_conn = store_server.admin_conn()
    provision_user(admin_conn, "bridge_user", "bridge_pw")
    admin_conn.close()


@pytest.fixture(scope="module")
def client(conn_info, _provision_users):
    c = StoreClient(
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
        schema = infer_dh_schema(Widget)
        # Domain columns
        assert schema["name"] == dht.string
        assert schema["color"] == dht.string
        assert schema["weight"] == dht.double

    def test_infer_schema_int_type(self):
        schema = infer_dh_schema(Gadget)
        assert schema["quantity"] == dht.int64
        assert schema["price"] == dht.double
        assert schema["label"] == dht.string

    def test_infer_schema_optional_decimal_datetime(self):
        schema = infer_dh_schema(RichItem)
        assert schema["amount"] == dht.double       # Decimal → double
        assert schema["created"] == dht.Instant      # Optional[datetime] → Instant
        assert schema["active"] == dht.bool_
        assert schema["notes"] == dht.string          # Optional[str] → string

    def test_infer_schema_metadata_columns(self):
        schema = infer_dh_schema(Widget)
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
        client.write(w)
        columns = ["EntityId", "Version", "EventType", "State",
                    "UpdatedBy", "TxTime", "name", "color", "weight"]
        row = extract_row(w, columns)
        assert row[0] == w._store_entity_id   # EntityId
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
        client = StoreClient(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="gear", color="blue", weight=1.2)
        client.write(w)

        # Start bridge (creates its own StoreClient for read-back)
        bridge.start()

        # Emit event manually
        event = ChangeEvent(
            entity_id=w._store_entity_id,
            version=1,
            event_type="CREATED",
            type_name=Widget.type_name(),
            updated_by="bridge_user",
            state=None,
            tx_time=w._store_tx_time,
        )
        bridge._dispatch(event)
        _flush_dh()

        # Verify row in DH table
        assert tbl.size == 1
        df = dhpd.to_pandas(tbl)
        assert df["name"].iloc[0] == "gear"
        assert df["color"].iloc[0] == "blue"
        assert abs(df["weight"].iloc[0] - 1.2) < 0.001
        assert df["EntityId"].iloc[0] == w._store_entity_id

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

        client = StoreClient(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        g = Gadget(label="phone", price=999.0, quantity=5)
        client.write(g)

        bridge.start()

        # Dispatch a Gadget event — Widget bridge should ignore it
        event = ChangeEvent(
            entity_id=g._store_entity_id,
            version=1,
            event_type="CREATED",
            type_name=Gadget.type_name(),
            updated_by="bridge_user",
            state=None,
            tx_time=g._store_tx_time,
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
        bridge.register(Widget, filter=Field("color") == Const("red"))
        tbl = bridge.table(Widget)

        client = StoreClient(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="valve", color="red", weight=0.3)
        client.write(w)

        bridge.start()
        event = ChangeEvent(
            entity_id=w._store_entity_id, version=1,
            event_type="CREATED", type_name=Widget.type_name(),
            updated_by="bridge_user", state=None, tx_time=w._store_tx_time,
        )
        bridge._dispatch(event)
        _flush_dh()

        assert tbl.size == 1
        df = dhpd.to_pandas(tbl)
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
        bridge.register(Widget, filter=Field("color") == Const("red"))
        tbl = bridge.table(Widget)

        client = StoreClient(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="pipe", color="green", weight=2.0)
        client.write(w)

        bridge.start()
        event = ChangeEvent(
            entity_id=w._store_entity_id, version=1,
            event_type="CREATED", type_name=Widget.type_name(),
            updated_by="bridge_user", state=None, tx_time=w._store_tx_time,
        )
        bridge._dispatch(event)
        _flush_dh()

        assert tbl.size == 0

        bridge.stop()
        client.close()

    def test_custom_writer(self, conn_info, _provision_users):
        # User provides their own DynamicTableWriter
        custom_schema = infer_dh_schema(Widget)
        custom_writer = DynamicTableWriter(custom_schema)

        bridge = StoreBridge(
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
            user="bridge_user", password="bridge_pw",
            subscriber_id=None,
        )
        bridge.register(Widget, writer=custom_writer)

        # The table should be the custom writer's table
        assert bridge.table(Widget) is custom_writer.table

        client = StoreClient(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="nut", color="brass", weight=0.1)
        client.write(w)

        bridge.start()
        event = ChangeEvent(
            entity_id=w._store_entity_id, version=1,
            event_type="CREATED", type_name=Widget.type_name(),
            updated_by="bridge_user", state=None, tx_time=w._store_tx_time,
        )
        bridge._dispatch(event)
        _flush_dh()

        assert custom_writer.table.size == 1
        df = dhpd.to_pandas(custom_writer.table)
        assert df["name"].iloc[0] == "nut"

        bridge.stop()
        client.close()


# ═════════════════════════════════════════════════════════════════════════
# Full Round-Trip Tests (real PG NOTIFY → bridge → real DH table)
# ═════════════════════════════════════════════════════════════════════════

class TestFullRoundTrip:

    def test_store_write_to_dh_table(self, conn_info, _provision_users):
        """StoreClient.write() → PG NOTIFY → bridge → row in DH table."""
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
        writer_client = StoreClient(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="spring", color="steel", weight=0.05)
        writer_client.write(w)

        # Wait for NOTIFY → bridge dispatch → DH update
        time.sleep(1.0)
        _flush_dh()

        assert tbl.size >= 1
        df = dhpd.to_pandas(tbl)
        matched = df[df["name"] == "spring"]
        assert len(matched) == 1
        assert matched["color"].iloc[0] == "steel"

        bridge.stop()
        writer_client.close()

    def test_store_update_appends_to_dh_table(self, conn_info, _provision_users):
        """StoreClient.update() → new row appends to DH table."""
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

        writer_client = StoreClient(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        g = Gadget(label="tablet", price=499.0, quantity=10)
        writer_client.write(g)
        time.sleep(1.0)

        # Update the gadget
        g.price = 449.0
        writer_client.update(g)
        time.sleep(1.0)

        # Both versions should be in the raw table
        _flush_dh()
        assert tbl.size >= 2
        df = dhpd.to_pandas(tbl)
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

        writer_client = StoreClient(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w = Widget(name="cam", color="black", weight=0.8)
        g = Gadget(label="lens", price=200.0, quantity=3)
        writer_client.write(w)
        writer_client.write(g)
        time.sleep(1.0)

        # Widget table should have the widget, gadget table should have the gadget
        _flush_dh()
        w_df = dhpd.to_pandas(widget_tbl)
        g_df = dhpd.to_pandas(gadget_tbl)
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

        writer_client = StoreClient(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )

        w = Widget(name="shaft", color="grey", weight=3.0)
        writer_client.write(w)
        time.sleep(1.0)

        w.color = "chrome"
        writer_client.update(w)
        time.sleep(1.0)

        # Raw table has 2 rows, live table has 1 (latest)
        _flush_dh()
        df_live = dhpd.to_pandas(live_tbl)
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

        writer_client = StoreClient(
            user="bridge_user", password="bridge_pw",
            host=conn_info["host"], port=conn_info["port"],
            dbname=conn_info["dbname"],
        )
        w1 = Widget(name="pin1", color="gold", weight=0.01)
        writer_client.write(w1)
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
        writer_client.write(w2)
        time.sleep(1.0)

        # tbl2 should have pin2 but NOT pin1 (checkpoint skipped it)
        _flush_dh()
        df2 = dhpd.to_pandas(tbl2)
        assert any(df2["name"] == "pin2")
        assert not any(df2["name"] == "pin1")

        bridge2.stop()
        writer_client.close()
