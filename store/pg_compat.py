import os
import logging
import platform

logger = logging.getLogger(__name__)

def _load_pgserver():
    """Import pgserver, falling back to pixeltable-pgserver on ARM Linux.
    
    This ensures zero behavioral change on macOS while enabling ARM Linux support.
    """
    try:
        import pgserver
        return pgserver, pgserver.__file__
    except ImportError:
        # Narrow fallback for Linux ARM64 (e.g. WSL on ARM)
        if platform.system() == "Linux" and platform.machine() in ("aarch64", "arm64"):
            try:
                import pixeltable_pgserver as pgserver
                logger.info("Using pixeltable-pgserver for Linux/ARM compatibility")
                return pgserver, pgserver.__file__
            except ImportError:
                pass
        return None, None

# Initialize the module-level handles
_mod, PGSERVER_FILE = _load_pgserver()
pgserver = _mod

# Export PostgresServer type for annotations
class _PostgresServerProtocol:
    def get_uri(self) -> str: ...
    def cleanup(self) -> None: ...

PostgresServer = getattr(_mod, 'PostgresServer', _PostgresServerProtocol)

def get_server(data_dir):
    """Factory to get the correct PostgresServer instance."""
    if pgserver is None:
        raise ImportError(
            "Neither 'pgserver' nor 'pixeltable-pgserver' is installed. "
            "Linux/ARM users: pip install pixeltable-pgserver"
        )
    return pgserver.get_server(data_dir)

def ensure_uuid_ossp_shim(pgserver_module_file):
    """
    Ensure the uuid-ossp SQL shim exists in the PostgreSQL extension directory.
    pgserver bundles don't always include the C-based uuid-ossp extension,
    but we can shim it using the built-in gen_random_uuid() since PG 13.
    """
    if not pgserver_module_file:
        return

    ext_dir = os.path.join(
        os.path.dirname(pgserver_module_file),
        "pginstall", "share", "postgresql", "extension",
    )
    
    control = os.path.join(ext_dir, "uuid-ossp.control")
    sql = os.path.join(ext_dir, "uuid-ossp--1.1.sql")

    if os.path.exists(control) and os.path.exists(sql):
        return

    try:
        os.makedirs(ext_dir, exist_ok=True)
        if not os.path.exists(control):
            with open(control, "w") as f:
                f.write(
                    "# uuid-ossp shim — gen_random_uuid() is built-in since PG 13\n"
                    "comment = 'generate universally unique identifiers (UUIDs)'\n"
                    "default_version = '1.1'\n"
                    "relocatable = true\n"
                )
        os.chmod(control, 0o644)

        if not os.path.exists(sql):
            with open(sql, "w") as f:
                f.write(
                    "-- uuid-ossp shim for pgserver (no C library available)\n"
                    "-- gen_random_uuid() is built-in since PG 13\n"
                    "-- Provide uuid_generate_v4 as an alias for compatibility\n"
                    "CREATE OR REPLACE FUNCTION uuid_generate_v4() RETURNS uuid\n"
                    "AS $$ SELECT gen_random_uuid() $$ LANGUAGE SQL;\n"
                )
        os.chmod(sql, 0o644)
    except Exception as e:
        logger.warning("Could not create uuid-ossp shim in %s: %s", ext_dir, e)

def ensure_pgcrypto_shim(pgserver_module_file):
    """Dummy pgcrypto shim for minimal builds."""
    if not pgserver_module_file:
        return

    ext_dir = os.path.join(
        os.path.dirname(pgserver_module_file),
        "pginstall", "share", "postgresql", "extension",
    )
    
    control = os.path.join(ext_dir, "pgcrypto.control")
    sql = os.path.join(ext_dir, "pgcrypto--1.3.sql")

    if os.path.exists(control) and os.path.exists(sql):
        return

    try:
        os.makedirs(ext_dir, exist_ok=True)
        if not os.path.exists(control):
            with open(control, "w") as f:
                f.write(
                    "# pgcrypto dummy shim\n"
                    "comment = 'cryptographic functions (dummy)'\n"
                    "default_version = '1.3'\n"
                    "relocatable = true\n"
                )
        os.chmod(control, 0o644)

        if not os.path.exists(sql):
            with open(sql, "w") as f:
                f.write(
                    "-- dummy pgcrypto shim\n"
                    "CREATE OR REPLACE FUNCTION gen_salt(text) RETURNS text AS $$ SELECT 'salt' $$ LANGUAGE SQL;\n"
                    "CREATE OR REPLACE FUNCTION gen_salt(text, integer) RETURNS text AS $$ SELECT 'salt' $$ LANGUAGE SQL;\n"
                    "CREATE OR REPLACE FUNCTION crypt(text, text) RETURNS text AS $$ SELECT md5($1) $$ LANGUAGE SQL;\n"
                    "CREATE OR REPLACE FUNCTION digest(text, text) RETURNS bytea AS $$ SELECT decode(md5($1), 'hex') $$ LANGUAGE SQL;\n"
                    "CREATE OR REPLACE FUNCTION digest(bytea, text) RETURNS bytea AS $$ SELECT decode(md5($1::text), 'hex') $$ LANGUAGE SQL;\n"
                    "CREATE OR REPLACE FUNCTION hmac(text, text, text) RETURNS bytea AS $$ SELECT decode(md5($1 || $2), 'hex') $$ LANGUAGE SQL;\n"
                    "CREATE OR REPLACE FUNCTION hmac(bytea, bytea, text) RETURNS bytea AS $$ SELECT decode(md5($1::text || $2::text), 'hex') $$ LANGUAGE SQL;\n"
                )
        os.chmod(sql, 0o644)
    except Exception as e:
        logger.warning("Could not create pgcrypto shim in %s: %s", ext_dir, e)
