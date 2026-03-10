"""
QuestDB Manager — Binary Lifecycle
===================================
Downloads, starts, health-checks, and stops a QuestDB instance.
Same pattern as pgserver wrapping PostgreSQL.
"""

from __future__ import annotations

import asyncio
import logging
import os
import platform
import shutil
import subprocess
import tarfile
import zipfile
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

# QuestDB release to download if binary not found
QUESTDB_VERSION = "8.2.1"
QUESTDB_BASE_URL = "https://github.com/questdb/questdb/releases/download"


def _detect_archive_name() -> str:
    """Detect the correct QuestDB archive for the current platform."""
    system = platform.system().lower()
    machine = platform.machine().lower()

    if system == "darwin":
        if machine in ("arm64", "aarch64"):
            return f"questdb-{QUESTDB_VERSION}-no-jre-bin.tar.gz"
        return f"questdb-{QUESTDB_VERSION}-no-jre-bin.tar.gz"
    elif system == "linux":
        if machine in ("aarch64", "arm64"):
            # Use the architecture-independent 'no-jre' binary on ARM Linux
            # to avoid bundled x86_64 JRE crashes.
            return f"questdb-{QUESTDB_VERSION}-no-jre-bin.tar.gz"
        return f"questdb-{QUESTDB_VERSION}-rt-linux-amd64.tar.gz"
    elif system == "windows":
        return f"questdb-{QUESTDB_VERSION}-no-jre-bin.tar.gz"
    else:
        return f"questdb-{QUESTDB_VERSION}-no-jre-bin.tar.gz"


class QuestDBManager:
    """Manages QuestDB binary lifecycle: download, start, health, stop."""

    def __init__(
        self,
        data_dir: str = "data/questdb",
        host: str = "localhost",
        http_port: int = 9000,
        ilp_port: int = 9009,
        pg_port: int = 8812,
    ) -> None:
        self._data_dir = Path(data_dir).resolve()
        self._host = host
        self._http_port = http_port
        self._ilp_port = ilp_port
        self._pg_port = pg_port
        self._process: subprocess.Popen | None = None

    @property
    def is_running(self) -> bool:
        """Check if the QuestDB process is alive."""
        return self._process is not None and self._process.poll() is None

    async def start(self) -> None:
        """Download QuestDB if needed, start the subprocess, wait for health."""
        # Check if already running
        if await self.health():
            logger.info("QuestDB already running on port %d", self._http_port)
            return

        bin_path = self._ensure_binary()
        self._data_dir.mkdir(parents=True, exist_ok=True)

        # Build command — QuestDB requires Java 17-21 (not 25+)
        java_path = self._find_java()
        questdb_jar = self._find_jar(bin_path)

        cmd = [
            java_path,
            "--enable-native-access=io.questdb",
            "-p", str(questdb_jar),
            "-m", "io.questdb/io.questdb.ServerMain",
            "-d", str(self._data_dir),
        ]

        # Write server.conf to control all ports
        conf_dir = self._data_dir / "conf"
        conf_dir.mkdir(parents=True, exist_ok=True)
        conf_file = conf_dir / "server.conf"
        conf_file.write_text(
            f"http.net.bind.to=0.0.0.0:{self._http_port}\n"
            f"http.min.net.bind.to=0.0.0.0:{self._http_port + 3}\n"
            f"line.tcp.net.bind.to=0.0.0.0:{self._ilp_port}\n"
            f"pg.net.bind.to=0.0.0.0:{self._pg_port}\n"
            f"metrics.enabled=false\n"
        )

        env = os.environ.copy()

        logger.info("Starting QuestDB: %s", " ".join(cmd))
        self._process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Wait for health
        for _attempt in range(30):
            await asyncio.sleep(1)
            if await self.health():
                logger.info(
                    "QuestDB started (HTTP=%d, ILP=%d, PG=%d)",
                    self._http_port, self._ilp_port, self._pg_port,
                )
                return
            if self._process.poll() is not None:
                stderr = self._process.stderr.read().decode() if self._process.stderr else ""
                raise RuntimeError(f"QuestDB exited during startup: {stderr[:500]}")

        raise RuntimeError(
            f"QuestDB failed to start within 30s (HTTP port {self._http_port})"
        )

    async def stop(self) -> None:
        """Gracefully stop QuestDB."""
        if self._process and self._process.poll() is None:
            logger.info("Stopping QuestDB (pid=%d)", self._process.pid)
            self._process.terminate()
            try:
                self._process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logger.warning("QuestDB did not stop gracefully, killing")
                self._process.kill()
                self._process.wait(timeout=5)
            logger.info("QuestDB stopped")
        self._process = None

    async def health(self) -> bool:
        """Check QuestDB health via HTTP endpoint."""
        url = f"http://{self._host}:{self._http_port}/"
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                resp = await client.get(url)
                return resp.status_code == 200
        except Exception:
            return False

    def _ensure_binary(self) -> Path:
        """Ensure QuestDB binary is available, downloading if needed."""
        bin_dir = self._data_dir / "bin"
        jar_candidates = list(bin_dir.glob("questdb*.jar")) if bin_dir.exists() else []
        if jar_candidates:
            return bin_dir

        logger.info("QuestDB binary not found, downloading v%s...", QUESTDB_VERSION)
        archive_name = _detect_archive_name()
        url = f"{QUESTDB_BASE_URL}/{QUESTDB_VERSION}/{archive_name}"

        bin_dir.mkdir(parents=True, exist_ok=True)
        archive_path = bin_dir / archive_name

        # Download (httpx handles macOS SSL certs properly)
        with httpx.stream("GET", url, follow_redirects=True, timeout=120) as resp:
            resp.raise_for_status()
            with open(archive_path, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=65536):
                    f.write(chunk)
        logger.info("Downloaded %s", archive_name)

        # Extract
        if archive_name.endswith(".tar.gz"):
            with tarfile.open(archive_path, "r:gz") as tf:
                tf.extractall(path=bin_dir)
        elif archive_name.endswith(".zip"):
            with zipfile.ZipFile(archive_path, "r") as zf:
                zf.extractall(path=bin_dir)

        archive_path.unlink()

        # Find extracted dir and flatten jar into bin_dir
        for child in bin_dir.iterdir():
            if child.is_dir() and child.name.startswith("questdb"):
                for jar in child.glob("questdb*.jar"):
                    shutil.move(str(jar), str(bin_dir / jar.name))
                # Keep lib dir if it exists
                lib_src = child / "lib"
                lib_dst = bin_dir / "lib"
                if lib_src.exists() and not lib_dst.exists():
                    shutil.move(str(lib_src), str(lib_dst))
                break

        logger.info("QuestDB v%s installed to %s", QUESTDB_VERSION, bin_dir)
        return bin_dir

    def _find_java(self) -> str:
        """Find a compatible Java (17-21) for QuestDB.

        QuestDB 8.x uses sun.misc.Unsafe which was removed in Java 22+.
        Priority: QUESTDB_JAVA_HOME → /usr/libexec/java_home -v 21 → -v 17.
        JAVA_HOME is only used if it points to Java 17-21.
        """
        import subprocess as _sp

        def _java_major(java_bin: str) -> int | None:
            """Get the major version of a java binary."""
            try:
                r = _sp.run([java_bin, "-version"], capture_output=True, text=True, timeout=5)
                # Version string is on stderr, e.g. 'openjdk version "21.0.7"'
                for line in (r.stderr + r.stdout).splitlines():
                    if "version" in line:
                        import re
                        m = re.search(r'"(\d+)', line)
                        if m:
                            return int(m.group(1))
            except Exception:
                pass
            return None

        def _is_compatible(java_bin: str) -> bool:
            major = _java_major(java_bin)
            return major is not None and 17 <= major <= 21

        # 1. Explicit QUESTDB_JAVA_HOME (trusted — no version check)
        qjh = os.environ.get("QUESTDB_JAVA_HOME")
        if qjh:
            candidate = os.path.join(qjh, "bin", "java")
            if os.path.isfile(candidate):
                logger.info("Using Java from QUESTDB_JAVA_HOME: %s", candidate)
                return candidate

        # 2. macOS java_home utility — try 21, then 17
        for version in ("21", "17"):
            try:
                result = _sp.run(
                    ["/usr/libexec/java_home", "-v", version],
                    capture_output=True, text=True, timeout=5,
                )
                if result.returncode == 0:
                    java_home = result.stdout.strip()
                    candidate = os.path.join(java_home, "bin", "java")
                    if os.path.isfile(candidate) and _is_compatible(candidate):
                        logger.info("Using Java %s: %s", version, candidate)
                        return candidate
            except (FileNotFoundError, _sp.TimeoutExpired):
                pass

        # 3. JAVA_HOME — only if compatible
        jh = os.environ.get("JAVA_HOME")
        if jh:
            candidate = os.path.join(jh, "bin", "java")
            if os.path.isfile(candidate) and _is_compatible(candidate):
                logger.info("Using Java from JAVA_HOME: %s", candidate)
                return candidate

        # 4. PATH — only if compatible
        java_path = shutil.which("java")
        if java_path and _is_compatible(java_path):
            return java_path

        raise RuntimeError(
            "Java 17-21 not found. QuestDB 8.x is incompatible with Java 22+. "
            "Install Java 21 or set QUESTDB_JAVA_HOME."
        )

    def _find_jar(self, bin_dir: Path) -> Path:
        """Find the QuestDB JAR in the bin directory."""
        jars = list(bin_dir.glob("questdb*.jar"))
        if not jars:
            raise FileNotFoundError(
                f"No QuestDB JAR found in {bin_dir}. "
                "Delete the directory and restart to re-download."
            )
        return jars[0]
