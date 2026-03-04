"""
Lakehouse RLS Server — Arrow Flight SQL Gateway with Row-Level Security
=========================================================================
Intercepts SQL queries, rewrites them to enforce row-level access control
via ACL table joins, and streams results back as Arrow record batches.

Usage::

    from lakehouse.rls_server import RLSFlightServer, RLSPolicy, serve

    server = RLSFlightServer(
        duckdb_conn=conn,
        policies=[
            RLSPolicy(
                table_name="sales_data",
                acl_table="sales_acl",
                join_column="row_id",
                user_column="user_token",
            ),
        ],
        users={"alice-token": "alice", "bob-token": "bob"},
    )
    server.serve()
"""

from __future__ import annotations

import logging
import struct
import threading
from collections.abc import Generator
from dataclasses import dataclass, field

import duckdb
import pyarrow as pa
import pyarrow.flight as flight
import sqlglot
from sqlglot import expressions as exp

logger = logging.getLogger(__name__)


# ── RLS Policy ──────────────────────────────────────────────────────────────


@dataclass
class RLSPolicy:
    """Defines row-level security for a single table.

    Args:
        table_name:  The protected table (unqualified name, e.g. "sales_data").
        acl_table:   The ACL table containing row-level grants.
        join_column: Column shared between the data table and the ACL table.
        user_column: Column in the ACL table that holds the user token/identity.
    """

    table_name: str
    acl_table: str
    join_column: str
    user_column: str = "user_token"


# ── SQL Rewriter ────────────────────────────────────────────────────────────


class RLSRewriter:
    """Rewrites SQL to inject ACL joins on protected tables.

    Stateless — instantiate once and call ``rewrite()`` for each query.
    Uses sqlglot for robust AST manipulation (dialect-agnostic).
    """

    def __init__(self, policies: list[RLSPolicy]) -> None:
        self._policies: dict[str, RLSPolicy] = {p.table_name: p for p in policies}

    @property
    def protected_tables(self) -> set[str]:
        """Return the set of table names with RLS policies."""
        return set(self._policies.keys())

    def rewrite(self, sql: str, user_token: str) -> str:
        """Rewrite SQL to enforce RLS.

        For each reference to a protected table, injects a JOIN to the ACL
        table with a filter on the user token.

        Args:
            sql:        The original SQL query.
            user_token: The authenticated user's token (injected into WHERE).

        Returns:
            The rewritten SQL string.
        """
        try:
            parsed = sqlglot.parse(sql, dialect="duckdb")
        except sqlglot.errors.ParseError:
            logger.warning("sqlglot parse failed, passing SQL through: %s", sql[:200])
            return sql

        rewritten_parts = []
        for statement in parsed:
            if statement is None:
                continue
            rewritten_parts.append(self._rewrite_statement(statement, user_token))

        return "; ".join(rewritten_parts)

    def _rewrite_statement(self, statement: exp.Expression, user_token: str) -> str:
        """Rewrite a single parsed statement."""
        if not isinstance(statement, exp.Select):
            return statement.sql(dialect="duckdb")

        # Find all table references in the FROM/JOIN clauses
        tables_to_protect: list[tuple[exp.Table, RLSPolicy]] = []

        for table_node in statement.find_all(exp.Table):
            table_name = table_node.name
            if table_name in self._policies:
                tables_to_protect.append((table_node, self._policies[table_name]))

        if not tables_to_protect:
            return statement.sql(dialect="duckdb")

        # Rewrite SELECT * to SELECT <data_alias>.* to avoid ACL columns leaking
        select_exprs = statement.expressions
        has_star = any(isinstance(e, exp.Star) for e in select_exprs)

        # Track ACL aliases to avoid collisions
        acl_counter = 0
        data_aliases = []

        for table_node, policy in tables_to_protect:
            acl_counter += 1
            acl_alias = f"_acl{acl_counter}" if acl_counter > 1 else "_acl"

            # Determine the alias for the data table
            data_alias = table_node.alias or table_node.name

            # Ensure the data table has an alias for unambiguous references
            if not table_node.alias:
                table_node.set("alias", exp.TableAlias(this=exp.to_identifier(data_alias)))

            data_aliases.append(data_alias)

            # Build the ACL join:
            #   JOIN <acl_table> AS _acl
            #     ON <data_alias>.<join_col> = _acl.<join_col>
            #     AND _acl.<user_col> = '<user_token>'

            # Build the fully qualified ACL table reference
            # Use the same catalog/schema as the data table
            acl_table_node = exp.Table(
                this=exp.to_identifier(policy.acl_table),
                db=table_node.args.get("db"),
                catalog=table_node.args.get("catalog"),
                alias=exp.TableAlias(this=exp.to_identifier(acl_alias)),
            )

            join_condition = exp.and_(
                exp.EQ(
                    this=exp.column(policy.join_column, table=data_alias),
                    expression=exp.column(policy.join_column, table=acl_alias),
                ),
                exp.EQ(
                    this=exp.column(policy.user_column, table=acl_alias),
                    expression=exp.Literal.string(user_token),
                ),
            )

            join_node = exp.Join(
                this=acl_table_node,
                on=join_condition,
                kind="INNER",
            )

            statement.append("joins", join_node)

        # Replace bare SELECT * with SELECT <data_alias>.* for each data table
        if has_star and data_aliases:
            new_exprs = []
            for alias in data_aliases:
                new_exprs.append(
                    exp.Column(this=exp.Star(), table=exp.to_identifier(alias))
                )
            statement.set("expressions", new_exprs)

        return statement.sql(dialect="duckdb")

    def needs_rewrite(self, sql: str) -> bool:
        """Check if a SQL query references any protected table (fast check)."""
        try:
            parsed = sqlglot.parse(sql, dialect="duckdb")
        except sqlglot.errors.ParseError:
            return False

        for statement in parsed:
            if statement is None:
                continue
            for table_node in statement.find_all(exp.Table):
                if table_node.name in self._policies:
                    return True
        return False

    @staticmethod
    def extract_tables(sql: str) -> set[str]:
        """Extract all table names referenced in a SQL query."""
        tables: set[str] = set()
        try:
            parsed = sqlglot.parse(sql, dialect="duckdb")
        except sqlglot.errors.ParseError:
            return tables

        for statement in parsed:
            if statement is None:
                continue
            for table_node in statement.find_all(exp.Table):
                tables.add(table_node.name)
        return tables


# ── Auth Handler ────────────────────────────────────────────────────────────


class TokenServerAuthHandler(flight.ServerAuthHandler):
    """Simple bearer-token authentication handler.

    Accepts tokens from a pre-configured user registry dict.
    In production, this would validate JWTs or call an auth service.
    """

    def __init__(self, users: dict[str, str]) -> None:
        """
        Args:
            users: Mapping of token → username (e.g. {"alice-token": "alice"}).
        """
        super().__init__()
        self._users = users
        self._tokens: dict[bytes, str] = {}  # session token → username

    def authenticate(self, outgoing: flight.ServerAuthSender,
                     incoming: flight.ServerAuthReader) -> None:
        """Validate the bearer token from the client handshake."""
        token = incoming.read()
        token_str = token.decode("utf-8") if isinstance(token, bytes) else str(token)

        if token_str not in self._users:
            raise flight.FlightUnauthenticatedError(f"Invalid token: {token_str[:20]}...")

        username = self._users[token_str]
        # Send the token back as the session token
        outgoing.write(token)
        self._tokens[token] = username
        logger.info("Authenticated user '%s'", username)

    def is_valid(self, token: bytes) -> str:
        """Check if a session token is still valid. Returns the peer identity.

        Returns the raw token string (not username) because the SQL rewriter
        needs the token value for ACL table WHERE clauses.
        """
        if token in self._tokens:
            # Return the raw token string for use in SQL rewriting
            token_str = token.decode("utf-8") if isinstance(token, bytes) else str(token)
            return token_str

        # Try direct token lookup (for stateless re-auth)
        token_str = token.decode("utf-8") if isinstance(token, bytes) else str(token)
        if token_str in self._users:
            self._tokens[token] = self._users[token_str]
            return token_str

        raise flight.FlightUnauthenticatedError("Invalid or expired session token")


# ── Flight SQL Server ──────────────────────────────────────────────────────


class RLSFlightServer(flight.FlightServerBase):
    """Arrow Flight server that enforces row-level security via SQL rewriting.

    Supported operations:
        - ``do_get``   → Execute SQL with RLS and stream results
        - ``list_flights`` → Discover available tables
        - ``do_action("get_protected_tables")`` → Get set of RLS-protected tables
    """

    def __init__(
        self,
        duckdb_conn: duckdb.DuckDBPyConnection,
        policies: list[RLSPolicy] | None = None,
        users: dict[str, str] | None = None,
        host: str = "localhost",
        port: int = 8815,
    ) -> None:
        location = flight.Location.for_grpc_tcp(host, port)
        self._users = users or {}
        self._auth_handler = TokenServerAuthHandler(self._users)
        super().__init__(location, auth_handler=self._auth_handler)

        self._conn = duckdb_conn
        self._rewriter = RLSRewriter(policies or [])
        self._host = host
        self._port = port
        self._lock = threading.Lock()

        logger.info(
            "RLSFlightServer initialized: host=%s, port=%d, protected_tables=%s",
            host, port, self._rewriter.protected_tables,
        )

    @property
    def protected_tables(self) -> set[str]:
        """Tables with RLS policies."""
        return self._rewriter.protected_tables

    # ── Flight RPC methods ──────────────────────────────────────────────

    def do_get(self, context: flight.ServerCallContext,
               ticket: flight.Ticket) -> flight.RecordBatchStream:
        """Execute a SQL query with RLS enforcement and stream results."""
        # Extract the user token from the peer identity
        token = self._get_user_token(context)

        sql = ticket.ticket.decode("utf-8")
        logger.info("do_get: user=%s, sql=%s", token, sql[:200])

        # Rewrite the SQL to inject ACL joins
        rewritten = self._rewriter.rewrite(sql, token)
        if rewritten != sql:
            logger.info("Rewritten SQL: %s", rewritten[:300])

        # Execute against the server-side DuckDB
        with self._lock:
            try:
                result = self._conn.execute(rewritten)
                table = result.fetch_arrow_table()
            except Exception as e:
                logger.error("Query execution failed: %s — %s", rewritten[:200], e)
                raise flight.FlightInternalError(str(e)) from e

        return flight.RecordBatchStream(table)

    def get_flight_info(self, context: flight.ServerCallContext,
                        descriptor: flight.FlightDescriptor) -> flight.FlightInfo:
        """Return schema info for a SQL query (used by ADBC clients)."""
        token = self._get_user_token(context)

        if descriptor.descriptor_type == flight.DescriptorType.CMD:
            sql = descriptor.command.decode("utf-8")

            # Rewrite to get the correct schema
            rewritten = self._rewriter.rewrite(sql, token)

            with self._lock:
                try:
                    result = self._conn.execute(rewritten)
                    table = result.fetch_arrow_table()
                    schema = table.schema
                except Exception as e:
                    raise flight.FlightInternalError(str(e)) from e

            # Encode byte-size as 8-byte little-endian for total_bytes
            ticket = flight.Ticket(sql.encode("utf-8"))
            endpoint = flight.FlightEndpoint(
                ticket, [flight.Location.for_grpc_tcp(self._host, self._port)]
            )
            return flight.FlightInfo(
                schema, descriptor, [endpoint], table.num_rows, table.nbytes
            )

        raise flight.FlightInternalError("Only CMD descriptors are supported")

    def list_flights(self, context: flight.ServerCallContext,
                     criteria: bytes) -> Generator[flight.FlightInfo, None, None]:
        """List available tables as FlightInfo entries."""
        with self._lock:
            try:
                tables_result = self._conn.execute(
                    "SELECT table_catalog, table_schema, table_name "
                    "FROM information_schema.tables "
                    "WHERE table_schema NOT IN ('information_schema', 'pg_catalog') "
                    "ORDER BY table_catalog, table_schema, table_name"
                )
                rows = tables_result.fetchall()
            except Exception as e:
                logger.error("list_flights failed: %s", e)
                rows = []

        for catalog, schema, table_name in rows:
            fqn = f"{catalog}.{schema}.{table_name}" if catalog else f"{schema}.{table_name}"
            descriptor = flight.FlightDescriptor.for_command(
                f"SELECT * FROM {fqn}".encode("utf-8")
            )
            # Minimal FlightInfo — schema discovery on demand
            info = flight.FlightInfo(
                pa.schema([]),
                descriptor,
                [],
                -1,
                -1,
            )
            yield info

    def do_action(self, context: flight.ServerCallContext,
                  action: flight.Action) -> Generator[flight.Result, None, None]:
        """Handle custom actions.

        Supported actions:
            - "get_protected_tables" — returns JSON list of protected table names
        """
        if action.type == "get_protected_tables":
            import json
            tables = sorted(self._rewriter.protected_tables)
            body = json.dumps(tables).encode("utf-8")
            yield flight.Result(body)
        else:
            raise flight.FlightInternalError(f"Unknown action: {action.type}")

    def list_actions(self, context: flight.ServerCallContext) -> list:
        """List supported custom actions."""
        return [
            ("get_protected_tables", "Get the set of RLS-protected table names"),
        ]

    # ── Helpers ─────────────────────────────────────────────────────────

    def _get_user_token(self, context: flight.ServerCallContext) -> str:
        """Extract the user's bearer token from the call context."""
        peer_identity = context.peer_identity()
        if not peer_identity:
            raise flight.FlightUnauthenticatedError("No authentication token provided")
        return peer_identity.decode("utf-8") if isinstance(peer_identity, bytes) else str(peer_identity)


# ── Convenience launcher ───────────────────────────────────────────────────


def serve(
    duckdb_conn: duckdb.DuckDBPyConnection,
    policies: list[RLSPolicy] | None = None,
    users: dict[str, str] | None = None,
    host: str = "localhost",
    port: int = 8815,
    block: bool = True,
) -> RLSFlightServer:
    """Start the RLS Flight SQL server.

    Args:
        duckdb_conn: Server-side DuckDB connection (with Iceberg attached).
        policies:    List of RLS policies for protected tables.
        users:       Token → username mapping for authentication.
        host:        Bind address.
        port:        Bind port.
        block:       If True, block until shutdown. If False, return the server.

    Returns:
        The server instance (useful when ``block=False``).
    """
    server = RLSFlightServer(
        duckdb_conn=duckdb_conn,
        policies=policies,
        users=users,
        host=host,
        port=port,
    )
    logger.info("RLS Flight SQL server starting on %s:%d", host, port)

    if block:
        server.serve()
    return server
