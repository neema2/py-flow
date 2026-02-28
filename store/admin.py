"""
store.admin — Platform API for the Object Store
=================================================
Start/stop embedded PostgreSQL, provision users, register aliases.

Platform usage::

    from store.admin import StoreServer

    server = StoreServer(data_dir="data/demo")
    server.start()
    server.register_alias("demo")
    server.provision_user("alice", "pw")

User code uses ``store.connect("demo", user="alice", password="pw")``.
"""

from store.server import StoreServer

__all__ = ["StoreServer"]
