#!/usr/bin/env python3
"""
Demo: Three-Tier State Machine Side-Effects

Shows all three tiers of side-effects firing on a single state transition:

  Tier 1 (action):         Atomic with DB — rolls back if it fails.
  Tier 2 (on_enter/on_exit): Fire-and-forget after commit.
  Tier 3 (start_workflow): Durable workflow dispatch after commit.

Usage:
    python demo_three_tiers.py
"""

import tempfile
from dataclasses import dataclass

# ── Platform imports (admin tier) ────────────────────────────────────────
from store.admin import StoreServer
from workflow.admin import WorkflowServer
from workflow import create_engine

# ── User imports ─────────────────────────────────────────────────────────
from store import Storable, connect, StateMachine, Transition


# ---------------------------------------------------------------------------
# 1. Define a settlement workflow (Tier 3)
# ---------------------------------------------------------------------------

_settlement_log = []


def settlement_workflow(entity_id):
    """Durable workflow: runs to completion even if process restarts."""
    _settlement_log.append(f"[TIER 3] Settlement workflow started for {entity_id}")
    # In production, each of these would be engine.step() for exactly-once:
    _settlement_log.append(f"[TIER 3]   → Step 1: Notify clearing house")
    _settlement_log.append(f"[TIER 3]   → Step 2: Update position book")
    _settlement_log.append(f"[TIER 3]   → Step 3: Send confirmation")
    _settlement_log.append(f"[TIER 3] Settlement complete for {entity_id}")


# ---------------------------------------------------------------------------
# 2. Define the state machine with all three tiers
# ---------------------------------------------------------------------------

_action_log = []
_hook_log = []


def _book_settlement(obj, from_state, to_state):
    """Tier 1: Runs inside DB transaction. If this fails, state rolls back."""
    _action_log.append(
        f"[TIER 1] Booking settlement for {obj.symbol} "
        f"qty={obj.quantity} @ ${obj.price:.2f} ({from_state}→{to_state})"
    )


def _log_exit(obj, from_state, to_state):
    """Tier 2: Fire-and-forget after commit."""
    _hook_log.append(f"[TIER 2] on_exit: Leaving {from_state}")


def _log_enter(obj, from_state, to_state):
    """Tier 2: Fire-and-forget after commit."""
    _hook_log.append(f"[TIER 2] on_enter: Entered {to_state}")


class OrderLifecycle(StateMachine):
    initial = "PENDING"
    transitions = [
        Transition("PENDING", "FILLED",
                   guard=lambda obj: obj.quantity > 0,
                   action=_book_settlement,                # Tier 1: atomic
                   on_exit=_log_exit,                      # Tier 2: fire-and-forget
                   on_enter=_log_enter,                    # Tier 2: fire-and-forget
                   start_workflow=settlement_workflow),     # Tier 3: durable
        Transition("PENDING", "CANCELLED",
                   on_exit=_log_exit,
                   on_enter=_log_enter,
                   allowed_by=["risk_manager"]),
        Transition("FILLED", "SETTLED",
                   action=lambda obj, f, t: _action_log.append(
                       f"[TIER 1] Final settlement recorded"),
                   on_enter=lambda obj, f, t: _hook_log.append(
                       f"[TIER 2] on_enter: Trade fully settled")),
    ]


@dataclass
class Order(Storable):
    symbol: str = ""
    quantity: int = 0
    price: float = 0.0
    side: str = ""


Order._state_machine = OrderLifecycle


# ---------------------------------------------------------------------------
# 3. Run the demo
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("  Three-Tier State Machine Side-Effects Demo")
    print("=" * 70)

    # ── Platform setup ────────────────────────────────────────────────
    tmp_dir = tempfile.mkdtemp(prefix="demo_tiers_")

    store = StoreServer(data_dir=f"{tmp_dir}/store", admin_password="admin_pw")
    store.start()
    store.register_alias("demo")
    store.provision_user("trader", "trader_pw")

    wf = WorkflowServer(data_dir=f"{tmp_dir}/workflow")
    wf.start()
    wf.register_alias("demo")

    engine = create_engine("demo", name="demo-tiers")
    engine.launch()

    # Wire Tier 3
    Order._workflow_engine = engine

    # ── User code ─────────────────────────────────────────────────────
    db = connect("demo", user="trader", password="trader_pw")

    # ── Demo 1: All three tiers on PENDING → FILLED ──────────────────
    print("\n── Demo 1: Transition PENDING → FILLED ─────────────────────")
    print("   (Tier 1: action, Tier 2: on_exit+on_enter, Tier 3: workflow)")
    print()

    order = Order(symbol="AAPL", quantity=100, price=228.50, side="BUY")
    entity_id = order.save()
    print(f"Created order {entity_id[:8]}… symbol=AAPL qty=100 @ $228.50")
    print(f"State: {order._store_state}")
    print()

    print("Transitioning to FILLED…")
    order.transition("FILLED")
    print(f"State: {order._store_state}")
    print()

    # Show what fired
    for msg in _action_log:
        print(f"  {msg}")
    for msg in _hook_log:
        print(f"  {msg}")
    for msg in _settlement_log:
        print(f"  {msg}")

    # ── Demo 2: Tier 1 rollback on failure ───────────────────────────
    print("\n── Demo 2: Tier 1 Rollback (action failure) ────────────────")
    print("   Action raises → state change rolls back")
    print()

    class FailLifecycle(StateMachine):
        initial = "NEW"
        transitions = [
            Transition("NEW", "DONE",
                       action=lambda obj, f, t: (_ for _ in ()).throw(
                           ValueError("Settlement system unavailable!"))),
        ]

    order2 = Order(symbol="MSFT", quantity=50, price=415.00, side="SELL")
    order2._state_machine = FailLifecycle
    order2.save()
    print(f"Created order {order2._store_entity_id[:8]}… symbol=MSFT")
    print(f"State before: {order2._store_state}")

    try:
        order2.transition("DONE")
    except ValueError as e:
        print(f"  Action raised: {e}")

    fresh = Order.find(order2._store_entity_id)
    print(f"State after:  {fresh._store_state}  ← rolled back!")

    # ── Demo 3: Tier 2 failure is swallowed ──────────────────────────
    print("\n── Demo 3: Tier 2 Swallowed (hook failure) ─────────────────")
    print("   on_enter raises → state still committed")
    print()

    class FragileLifecycle(StateMachine):
        initial = "ALPHA"
        transitions = [
            Transition("ALPHA", "BETA",
                       on_enter=lambda obj, f, t: (_ for _ in ()).throw(
                           RuntimeError("Notification service down!"))),
        ]

    order3 = Order(symbol="GOOG", quantity=25, price=175.00, side="BUY")
    order3._state_machine = FragileLifecycle
    order3.save()
    print(f"Created order {order3._store_entity_id[:8]}… symbol=GOOG")
    print(f"State before: {order3._store_state}")

    order3.transition("BETA")
    print(f"State after:  {order3._store_state}  ← committed despite hook failure!")

    # ── Summary ──────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  Summary")
    print("=" * 70)
    print("""
  Tier 1 (action=):         Atomic with DB transaction.
                             If it fails → state rolls back.

  Tier 2 (on_enter=/on_exit=): Fire-and-forget after commit.
                             If it fails → swallowed, state is safe.

  Tier 3 (start_workflow=): Durable workflow via WorkflowEngine.
                             Survives crashes, tracked to completion.

  All declared on the Transition — one place, one DSL.
""")

    # Cleanup
    db.close()
    engine.destroy()
    wf.stop()
    store.stop()


if __name__ == "__main__":
    main()
