"""
Lakehouse Service Managers — EmbeddedPG + Lakekeeper + ObjectStore
==================================================================
Auto-download, start, health-check, and stop Lakekeeper (Iceberg REST catalog)
and object storage (via ``objectstore.configure()``).
"""

from __future__ import annotations

import asyncio
import logging
import os
import platform
import stat
import subprocess
import tarfile
import tempfile
import zipfile
from pathlib import Path

import httpx
from objectstore import ObjectStore

logger = logging.getLogger(__name__)

# ── Embedded PostgreSQL (zonkyio binaries) ─────────────────────────────────
#
# pgserver's bundled PG is minimal (no ICU, no OpenSSL, no pgcrypto).
# Lakekeeper requires pgcrypto + ICU. We use zonkyio embedded-postgres-binaries
# which ship a full PG 16 build with all contrib extensions.

ZONKYIO_PG_VERSION = "16.6.0"
ZONKYIO_BASE_URL = "https://repo1.maven.org/maven2/io/zonky/test/postgres"

_ZONKYIO_ARTIFACTS = {
    ("darwin", "arm64"): "embedded-postgres-binaries-darwin-arm64v8",
    ("darwin", "aarch64"): "embedded-postgres-binaries-darwin-arm64v8",
    ("darwin", "x86_64"): "embedded-postgres-binaries-darwin-amd64",
    ("linux", "x86_64"): "embedded-postgres-binaries-linux-amd64",
    ("linux", "aarch64"): "embedded-postgres-binaries-linux-arm64v8",
    ("linux", "arm64"): "embedded-postgres-binaries-linux-arm64v8",
}


def _zonkyio_jar_url() -> str:
    """Get the zonkyio JAR download URL for the current platform."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    key = (system, machine)
    if key not in _ZONKYIO_ARTIFACTS:
        raise RuntimeError(f"No zonkyio PG binaries for {system}/{machine}")
    artifact = _ZONKYIO_ARTIFACTS[key]
    return (
        f"{ZONKYIO_BASE_URL}/{artifact}/{ZONKYIO_PG_VERSION}/"
        f"{artifact}-{ZONKYIO_PG_VERSION}.jar"
    )


class EmbeddedPGManager:
    """
    Manages a full PostgreSQL instance using zonkyio embedded-postgres-binaries.
    Unlike pgserver (which ships a minimal PG without ICU/OpenSSL/pgcrypto),
    zonkyio binaries include the full contrib suite. This PG instance is used
    exclusively by Lakekeeper as its catalog backend.
    Lifecycle: download → initdb → pg_ctl start (TCP) → pg_ctl stop.
    """

    def __init__(
        self,
        data_dir: str = "data/lakehouse/postgres",
        port: int = 5488,
        user: str = "postgres",
    ) -> None:
        self._data_dir = Path(data_dir).resolve()
        self._port = port
        self._user = user
        self._pgdata = self._data_dir / "pgdata"
        self._pg_home = self._data_dir / "pg"

    @property
    def pg_url(self) -> str:
        """Connection URL for Lakekeeper."""
        return f"postgresql://{self._user}@localhost:{self._port}/postgres"

    @property
    def port(self) -> int:
        return self._port

    def _pg_ctl(self) -> Path:
        return self._pg_home / "bin" / "pg_ctl"

    def _initdb(self) -> Path:
        return self._pg_home / "bin" / "initdb"

    async def start(self) -> None:
        """Start PG using zonkyio binaries."""
        self._ensure_binaries()

        if not (self._pgdata / "PG_VERSION").exists():
            self._run_initdb()

        # Check if already running
        pid_file = self._pgdata / "postmaster.pid"
        if pid_file.exists():
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host="localhost", port=self._port,
                    user=self._user, dbname="postgres",
                    connect_timeout=2,
                )
                conn.close()
                logger.info("Embedded PG already running on port %d", self._port)
                return
            except Exception:
                # Stale pid file - stop and restart
                logger.info("Stale postmaster.pid found, stopping...")
                subprocess.run(
                    [str(self._pg_ctl()), "stop", "-D", str(self._pgdata), "-m", "fast"],
                    capture_output=True,
                )
                await asyncio.sleep(1)

        # Start with TCP and ICU support
        logger.info("Starting Embedded PG on port %d...", self._port)
        log_file = self._data_dir / "pg.log"
        result = subprocess.run(
            [str(self._pg_ctl()), "start", "-D", str(self._pgdata), "-w",
             "-o", f"-h localhost -p {self._port} -k {self._pgdata}",
             "-l", str(log_file)],
            capture_output=True,
            timeout=30,
        )
        if result.returncode != 0:
            stderr = result.stderr.decode()[:500]
            raise RuntimeError(f"Failed to start embedded PG: {stderr}")

        logger.info("Embedded PG started on port %d", self._port)

    async def stop(self) -> None:
        """Stop PG via pg_ctl."""
        if (self._pgdata / "postmaster.pid").exists():
            subprocess.run(
                [str(self._pg_ctl()), "stop", "-D", str(self._pgdata), "-m", "fast"],
                capture_output=True,
            )
            logger.info("Embedded PG stopped")

    def _run_initdb(self) -> None:
        """Initialize pgdata directory with ICU support."""
        self._pgdata.mkdir(parents=True, exist_ok=True)
        initdb = self._initdb()
        result = subprocess.run(
            [str(initdb), "-D", str(self._pgdata),
             "-U", self._user, "--auth=trust",
             "-E", "UTF8", "--locale-provider=icu", "--icu-locale=en-US"],
            capture_output=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(f"initdb failed: {result.stderr.decode()[:500]}")

        # Minimize shared memory usage so many concurrent PG instances
        # don't exhaust macOS kern.sysv.shm* limits during parallel tests.
        conf = self._pgdata / "postgresql.conf"
        with open(conf, "a") as f:
            f.write("\n# Added by py-flow: minimize SysV IPC for parallel test suites\n")
            f.write("shared_buffers = 8MB\n")
            f.write("dynamic_shared_memory_type = posix\n")

        logger.info("Initialized pgdata at %s", self._pgdata)

    def _ensure_binaries(self) -> Path:
        """Ensure zonkyio PG binaries are available, downloading if needed."""
        pg_ctl = self._pg_ctl()
        if pg_ctl.exists() and os.access(pg_ctl, os.X_OK):
            return pg_ctl

        # Use project-level cache to avoid downloading per-test
        project_root = Path(__file__).resolve().parent.parent
        cache_dir = Path(os.environ.get("PY_FLOW_CACHE_DIR", project_root / ".cache" / "lakehouse"))
        global_pg_home = cache_dir / f"pg-{ZONKYIO_PG_VERSION}"
        global_pg_ctl = global_pg_home / "bin" / "pg_ctl"

        if not (global_pg_ctl.exists() and os.access(global_pg_ctl, os.X_OK)):
            url = _zonkyio_jar_url()
            logger.info("Zonkyio PG binaries not found in cache, downloading from %s...", url)
    
            global_pg_home.parent.mkdir(parents=True, exist_ok=True)
            jar_path = global_pg_home.parent / "pg.jar"
    
            import time
            for attempt in range(5):
                try:
                    with httpx.stream("GET", url, follow_redirects=True, timeout=httpx.Timeout(300.0, read=300.0)) as resp:
                        resp.raise_for_status()
                        with open(jar_path, "wb") as f:
                            for chunk in resp.iter_bytes(chunk_size=65536):
                                f.write(chunk)
                    break
                except Exception as e:
                    logger.warning("Download failed on attempt %d: %s", attempt + 1, e)
                    if attempt == 4:
                        raise
                    time.sleep(2)
    
            # Extract the JAR to find the .txz archive inside
            extract_dir = global_pg_home.parent / "tmp_jar"
            extract_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(jar_path, "r") as zf:
                txz_names = [n for n in zf.namelist() if n.endswith(".txz")]
                if not txz_names:
                    raise RuntimeError("No .txz file found in zonkyio JAR")
                zf.extract(txz_names[0], extract_dir)
                txz_path = extract_dir / txz_names[0]
    
            # Extract the .txz into global_pg_home
            global_pg_home.mkdir(parents=True, exist_ok=True)
            with tarfile.open(txz_path, "r:xz") as tf:
                tf.extractall(path=global_pg_home)
    
            # Make binaries executable
            bin_dir = global_pg_home / "bin"
            if bin_dir.exists():
                for binary in bin_dir.iterdir():
                    if binary.is_file():
                        binary.chmod(binary.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP)
    
            jar_path.unlink()
            import shutil
            shutil.rmtree(extract_dir)

            logger.info("Installed zonkyio PG %s to cache %s", ZONKYIO_PG_VERSION, global_pg_home)

        if not global_pg_ctl.exists():
            raise FileNotFoundError(f"pg_ctl not found after extraction: {global_pg_ctl}")

        # Symlink or copy to the test instance pg_home
        self._pg_home.parent.mkdir(parents=True, exist_ok=True)
        # Handle symlinks safely - Windows might not support it without admin
        try:
            if self._pg_home.exists():
                import shutil
                if self._pg_home.is_symlink():
                    self._pg_home.unlink()
                else:
                    shutil.rmtree(self._pg_home)
            os.symlink(global_pg_home, self._pg_home, target_is_directory=True)
        except OSError:
            import shutil
            shutil.copytree(global_pg_home, self._pg_home)

        return pg_ctl


def ensure_pgcrypto() -> bool:
    """Legacy helper — pgcrypto is included in zonkyio binaries by default.

    This function is kept for backward compatibility but is a no-op when using
    EmbeddedPGManager (which ships full contrib). Only useful if you're trying
    to add pgcrypto to pgserver's minimal build, which also lacks ICU and is
    therefore insufficient for Lakekeeper.
    """
    logger.debug("ensure_pgcrypto() called — zonkyio binaries include pgcrypto by default")
    return False


# ── Unified Lakehouse Lifecycle ────────────────────────────────────────────


class LakehouseStack:
    """Running lakehouse infrastructure.

    Properties:
        pg_url: PostgreSQL connection URL (for SyncEngine).
        catalog_url: Lakekeeper REST catalog URL.
        s3_endpoint: S3-compatible endpoint URL.
    """

    def __init__(
        self,
        pg: EmbeddedPGManager,
        lakekeeper: LakekeeperManager,
        s3_store: ObjectStore,
    ) -> None:
        self._pg = pg
        self._lakekeeper = lakekeeper
        self._s3_store = s3_store

    @property
    def pg_url(self) -> str:
        return self._pg.pg_url

    @property
    def catalog_url(self) -> str:
        return self._lakekeeper.catalog_url

    @property
    def s3_endpoint(self) -> str:
        return self._s3_store.endpoint


async def start_lakehouse(
    data_dir: str = "data/lakehouse",
    pg_port: int = 5488,
    lakekeeper_port: int = 8181,
    s3_api_port: int = 9002,
    s3_console_port: int = 9003,
    warehouse: str = "lakehouse",
    bucket: str = "lakehouse",
) -> LakehouseStack:
    """
    Start the full lakehouse stack: EmbeddedPG → ObjectStore → Lakekeeper.

    Downloads binaries on first run (~30s each for PG, Lakekeeper, object store).
    Subsequent starts are fast (< 5s total).

    Returns a LakehouseStack with all managers and connection info.
    """
    import objectstore

    pg = EmbeddedPGManager(data_dir=f"{data_dir}/postgres", port=pg_port)
    lk = LakekeeperManager(
        data_dir=f"{data_dir}/lakekeeper",
        port=lakekeeper_port,
    )

    # Start PG first (Lakekeeper depends on it)
    await pg.start()

    # Start object store (Lakekeeper warehouse depends on it)
    s3_store = await objectstore.configure(
        "minio",
        data_dir=f"{data_dir}/objectstore",
        api_port=s3_api_port,
        console_port=s3_console_port,
    )
    await s3_store.ensure_bucket(bucket)

    # Start Lakekeeper (depends on both PG and object store)
    await lk.start(pg_url=pg.pg_url)
    await lk.bootstrap()
    await lk.create_warehouse(
        name=warehouse,
        bucket=bucket,
        s3_endpoint=s3_store.endpoint,
    )

    logger.info(
        "Lakehouse stack running: PG=%d, Lakekeeper=%d, S3=%d",
        pg_port, lakekeeper_port, s3_api_port,
    )
    stack = LakehouseStack(pg=pg, lakekeeper=lk, s3_store=s3_store)
    return stack


async def stop_lakehouse(stack: LakehouseStack) -> None:
    """Stop all lakehouse services in reverse order."""
    await stack._lakekeeper.stop()
    # Object store cleanup handled by atexit
    await stack._pg.stop()
    logger.info("Lakehouse stack stopped")


# ── Lakekeeper ──────────────────────────────────────────────────────────────

LAKEKEEPER_VERSION = "0.11.2"
LAKEKEEPER_BASE_URL = "https://github.com/lakekeeper/lakekeeper/releases/download"

_LAKEKEEPER_TARGETS = {
    ("darwin", "arm64"): "lakekeeper-aarch64-apple-darwin.tar.gz",
    ("darwin", "aarch64"): "lakekeeper-aarch64-apple-darwin.tar.gz",
    ("darwin", "x86_64"): "lakekeeper-aarch64-apple-darwin.tar.gz",  # fallback
    ("linux", "x86_64"): "lakekeeper-x86_64-unknown-linux-gnu.tar.gz",
    ("linux", "aarch64"): "lakekeeper-aarch64-unknown-linux-gnu.tar.gz",
    ("linux", "arm64"): "lakekeeper-aarch64-unknown-linux-gnu.tar.gz",
}


def _lakekeeper_archive_name() -> str:
    """Detect the correct Lakekeeper archive for the current platform."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    key = (system, machine)
    if key not in _LAKEKEEPER_TARGETS:
        raise RuntimeError(
            f"No Lakekeeper binary for {system}/{machine}. "
            "Use Docker instead: docker run -p 8181:8181 lakekeeper/lakekeeper"
        )
    return _LAKEKEEPER_TARGETS[key]


class LakekeeperManager:
    """Manages Lakekeeper binary lifecycle: download, migrate, serve, stop."""

    def __init__(
        self,
        data_dir: str = "data/lakehouse/lakekeeper",
        port: int = 8181,
        pg_url: str | None = None,
        encryption_key: str = "py-flow-dev-key-do-not-use-in-prod",
    ) -> None:
        self._data_dir = Path(data_dir).resolve()
        self._port = port
        self._pg_url = pg_url
        self._encryption_key = encryption_key
        self._process: subprocess.Popen | None = None

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    @property
    def catalog_url(self) -> str:
        return f"http://localhost:{self._port}/catalog"

    async def start(self, pg_url: str | None = None) -> None:
        """Download if needed, run migrate, then start serving."""
        if await self.health():
            logger.info("Lakekeeper already running on port %d", self._port)
            return

        pg = pg_url or self._pg_url
        if not pg:
            raise ValueError(
                "Lakekeeper requires a PG connection URL. "
                "Pass pg_url or set LAKEKEEPER_PG_URL."
            )

        binary = self._ensure_binary()
        self._data_dir.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        env["LAKEKEEPER__PG_DATABASE_URL_READ"] = pg
        env["LAKEKEEPER__PG_DATABASE_URL_WRITE"] = pg
        env["LAKEKEEPER__PG_ENCRYPTION_KEY"] = self._encryption_key
        env["LAKEKEEPER__LISTEN_PORT"] = str(self._port)
        env["LAKEKEEPER__PG_SSL_MODE"] = "disable"
        env["LAKEKEEPER__METRICS_PORT"] = str(self._port + 1000)

        # Run migrate first
        logger.info("Running Lakekeeper migrate...")
        migrate = subprocess.run(
            [str(binary), "migrate"],
            env=env,
            capture_output=True,
            timeout=30,
        )
        if migrate.returncode != 0:
            stderr = migrate.stderr.decode()[:500]
            raise RuntimeError(f"Lakekeeper migrate failed: {stderr}")
        logger.info("Lakekeeper migrate complete")

        # Start serve
        logger.info("Starting Lakekeeper on port %d...", self._port)
        self._process = subprocess.Popen(
            [str(binary), "serve"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        for _attempt in range(30):
            await asyncio.sleep(1)
            if await self.health():
                logger.info("Lakekeeper started on port %d", self._port)
                return
            if self._process.poll() is not None:
                stderr = self._process.stderr.read().decode() if self._process.stderr else ""
                raise RuntimeError(f"Lakekeeper exited during startup: {stderr[:500]}")

        raise RuntimeError(f"Lakekeeper failed to start within 20s (port {self._port})")

    async def stop(self) -> None:
        """Gracefully stop Lakekeeper."""
        if self._process and self._process.poll() is None:
            logger.info("Stopping Lakekeeper (pid=%d)", self._process.pid)
            self._process.terminate()
            try:
                self._process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logger.warning("Lakekeeper did not stop gracefully, killing")
                self._process.kill()
                self._process.wait(timeout=5)
            logger.info("Lakekeeper stopped")
        self._process = None

    async def health(self) -> bool:
        """Check Lakekeeper health."""
        url = f"http://localhost:{self._port}/health"
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                resp = await client.get(url)
                return resp.status_code == 200
        except Exception:
            return False

    async def bootstrap(self) -> None:
        """Bootstrap Lakekeeper (first-time setup). Idempotent."""
        url = f"http://localhost:{self._port}/management/v1/bootstrap"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(url, json={"accept-terms-of-use": True})
                if resp.status_code in (200, 201, 409):
                    logger.info("Lakekeeper bootstrapped (status=%d)", resp.status_code)
                else:
                    logger.warning("Lakekeeper bootstrap returned %d: %s",
                                   resp.status_code, resp.text[:200])
        except Exception as e:
            logger.warning("Lakekeeper bootstrap failed: %s", e)

    async def create_warehouse(
        self,
        name: str = "lakehouse",
        bucket: str = "lakehouse",
        s3_endpoint: str = "http://localhost:9002",
        s3_access_key: str = "minioadmin",
        s3_secret_key: str = "minioadmin",
        s3_region: str = "us-east-1",
    ) -> None:
        """Create a warehouse in Lakekeeper pointing at S3 storage. Idempotent."""
        url = f"http://localhost:{self._port}/management/v1/warehouse"
        payload = {
            "warehouse-name": name,
            "project-id": None,
            "storage-profile": {
                "type": "s3",
                "bucket": bucket,
                "region": s3_region,
                "endpoint": s3_endpoint,
                "path-style-access": True,
                "flavor": "s3-compat",
                "sts-enabled": True,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": s3_access_key,
                "aws-secret-access-key": s3_secret_key,
            },
        }
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code in (200, 201):
                    logger.info("Lakekeeper warehouse '%s' created", name)
                elif resp.status_code == 409:
                    logger.info("Lakekeeper warehouse '%s' already exists", name)
                else:
                    logger.warning(
                        "Lakekeeper warehouse creation returned %d: %s",
                        resp.status_code, resp.text[:300],
                    )
        except Exception as e:
            logger.warning("Lakekeeper warehouse creation failed: %s", e)

    def _ensure_binary(self) -> Path:
        """Ensure Lakekeeper binary is available, downloading if needed."""
        bin_dir = self._data_dir / "bin"
        binary = bin_dir / "lakekeeper"
        if binary.exists() and os.access(binary, os.X_OK):
            return binary

        # Use project-level cache to avoid downloading per-test
        project_root = Path(__file__).resolve().parent.parent
        cache_dir = Path(os.environ.get("PY_FLOW_CACHE_DIR", project_root / ".cache" / "lakehouse"))
        global_bin_dir = cache_dir / f"lakekeeper-{LAKEKEEPER_VERSION}"
        global_binary = global_bin_dir / "lakekeeper"

        if not (global_binary.exists() and os.access(global_binary, os.X_OK)):
            logger.info("Lakekeeper binary not found in cache, downloading v%s...", LAKEKEEPER_VERSION)
            archive_name = _lakekeeper_archive_name()
            url = f"{LAKEKEEPER_BASE_URL}/v{LAKEKEEPER_VERSION}/{archive_name}"

            global_bin_dir.mkdir(parents=True, exist_ok=True)
            archive_path = global_bin_dir / archive_name

            import time
            for attempt in range(5):
                try:
                    with httpx.stream("GET", url, follow_redirects=True, timeout=httpx.Timeout(300.0, read=300.0)) as resp:
                        resp.raise_for_status()
                        with open(archive_path, "wb") as f:
                            for chunk in resp.iter_bytes(chunk_size=65536):
                                f.write(chunk)
                    break
                except Exception as e:
                    logger.warning("Download failed on attempt %d: %s", attempt + 1, e)
                    if attempt == 4:
                        raise
                    time.sleep(2)
            logger.info("Downloaded %s to cache", archive_name)

            with tarfile.open(archive_path, "r:gz") as tf:
                tf.extractall(path=global_bin_dir)

            archive_path.unlink()

            # Find the binary (may be in a subdirectory)
            if not global_binary.exists():
                for candidate in global_bin_dir.rglob("lakekeeper"):
                    if candidate.is_file():
                        candidate.rename(global_binary)
                        break

            if global_binary.exists():
                global_binary.chmod(global_binary.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

        if not global_binary.exists():
            raise FileNotFoundError(f"Lakekeeper binary not found after extraction in {global_bin_dir}")

        bin_dir.mkdir(parents=True, exist_ok=True)
        # Symlink or copy to the test instance
        try:
            if binary.exists():
                binary.unlink()
            os.symlink(global_binary, binary)
        except OSError:
            import shutil
            shutil.copy2(global_binary, binary)
            
        logger.info("Lakekeeper v%s installed to %s", LAKEKEEPER_VERSION, binary)
        return binary
