import argparse
import csv
from contextlib import contextmanager
import importlib.util
import json
import logging
import math
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import threading
import traceback
import tkinter as tk
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path
from tkinter import colorchooser, filedialog, messagebox, simpledialog, ttk

# Allow script-style execution (`python src/media_server_manager.py`) by making the
# repository root importable so the `src.` package imports below resolve.
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from pydub import AudioSegment
from src.workflows.library_cleaner.runner import SUPPORTED_EXTENSIONS as _AUDIO_EXTENSIONS
from src.workflows.tag_editor.runner import TagEditorWorkflow, TagUpdate

# Canonical audio-extension set, shared with every workflow (whose
# SUPPORTED_EXTENSIONS mirror it) so a file is handled the same way on every
# surface: GUI playback/tag editing and the cleaner/dedup/merge/tag workflows.
AUDIO_EXTENSIONS = frozenset(_AUDIO_EXTENSIONS)


def _module_available(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(module_name) is not None
    except (ImportError, ValueError):
        # find_spec raises ModuleNotFoundError (an ImportError) when a parent
        # package is missing, e.g. probing "watchdog.events" without watchdog.
        return False


WATCHDOG_AVAILABLE = (
    _module_available("watchdog.events") and _module_available("watchdog.observers")
)
if WATCHDOG_AVAILABLE:
    from watchdog.events import FileSystemEvent, FileSystemEventHandler
    from watchdog.observers import Observer
else:
    FileSystemEvent = object
    FileSystemEventHandler = object
    Observer = object

try:
    import simpleaudio
except ImportError:
    simpleaudio = None


DB_DEFAULT_PATH = os.path.join(os.path.expanduser("~"), ".media_server_organizer.db")

DEFAULT_THEME = {
    "window_background": "SystemButtonFace",
    "sidebar_background": "SystemWindow",
    "toolbar_background": "SystemButtonFace",
    "treeview_background": "SystemWindow",
    "metadata_background": "SystemButtonFace",
    "accent_color": "SystemHighlight",
    "text_color": "SystemWindowText",
}


def configure_logging(level: str | int = "INFO") -> Path:
    log_level = getattr(logging, str(level).upper(), logging.INFO)
    logs_dir = Path(__file__).resolve().parent.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "media_server_manager.log"

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

    root_logger.setLevel(log_level)
    logging.getLogger(__name__).info(
        "Logging initialized at %s level. Writing to %s",
        logging.getLevelName(log_level),
        log_file,
    )
    return log_file


logger = logging.getLogger(__name__)

AUDIO_BACKENDS = ("simpleaudio", "sounddevice")


def _backend_dependency_error(backend: str) -> str | None:
    if backend == "simpleaudio":
        try:
            import simpleaudio  # noqa: F401
        except ImportError:
            return "simpleaudio is not installed. Install it to use this backend."
        return None
    if backend == "sounddevice":
        missing: list[str] = []
        try:
            import numpy  # noqa: F401
        except ImportError:
            missing.append("numpy")
        try:
            import sounddevice  # noqa: F401
        except ImportError:
            missing.append("sounddevice")
        if missing:
            joined = ", ".join(missing)
            return f"The sounddevice backend requires: {joined}."
        return None
    return f"Unsupported audio backend: {backend}."


def resolve_audio_backend(requested_backend: str, emit_message: Callable[[str], None] | None = None) -> str:
    selected = requested_backend if requested_backend in AUDIO_BACKENDS else "simpleaudio"
    if selected != requested_backend and emit_message:
        emit_message(
            f"Unknown audio backend '{requested_backend}'. Falling back to '{selected}'."
        )

    fallback_order = [selected, *(backend for backend in AUDIO_BACKENDS if backend != selected)]
    selected_error: str | None = None
    for backend in fallback_order:
        error = _backend_dependency_error(backend)
        if error is None:
            if backend != selected and emit_message:
                emit_message(
                    f"Audio backend '{selected}' is unavailable ({selected_error}). "
                    f"Falling back to '{backend}'."
                )
            return backend
        if backend == selected:
            selected_error = error

    if emit_message:
        emit_message(
            "No supported audio backend is currently available. "
            "Install 'simpleaudio' or install both 'sounddevice' and 'numpy'."
        )
    return selected


def available_workflow_names() -> list[str]:
    workflows_dir = Path(__file__).resolve().parent / "workflows"
    if not workflows_dir.is_dir():
        return []
    return sorted(
        path.name
        for path in workflows_dir.iterdir()
        if path.is_dir()
        and not path.name.startswith((".", "_"))
        and (path / "runner.py").is_file()
    )


def load_workflow_runner(workflow_name: str) -> object | None:
    workflows_dir = Path(__file__).resolve().parent / "workflows"
    runner_path = workflows_dir / workflow_name / "runner.py"
    if not runner_path.exists():
        return None
    module_name = f"workflows.{workflow_name}.runner"
    spec = importlib.util.spec_from_file_location(module_name, runner_path)
    if not spec or not spec.loader:
        return None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        logger.error(
            "Failed to execute workflow module: %s",
            {
                "workflow_name": workflow_name,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            },
        )
        return None
    create_workflow = getattr(module, "create_workflow", None)
    if callable(create_workflow):
        try:
            workflow = create_workflow()
        except Exception as exc:
            logger.error(
                "Failed to create workflow runner: %s",
                {
                    "workflow_name": workflow_name,
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                },
            )
            return None

        required_methods = ("option_definitions", "build_plan", "apply")
        missing_methods = [
            method_name
            for method_name in required_methods
            if not callable(getattr(workflow, method_name, None))
        ]
        if missing_methods:
            logger.error(
                "Workflow runner interface validation failed: %s",
                {
                    "workflow_name": workflow_name,
                    "missing_methods": missing_methods,
                },
            )
            return None
        return workflow
    return None


@dataclass(frozen=True)
class Library:
    library_id: int
    name: str
    library_type: str
    path: str
    host: str | None
    username: str | None


@dataclass(frozen=True)
class AudioSignature:
    id: int
    library_id: int | None
    path: str
    signature: str
    bitrate: int | None
    sample_rate: int | None
    format: str | None
    kept: bool
    updated_at: str | None


@dataclass(frozen=True)
class WatchRunHistoryEntry:
    timestamp: str
    status: str
    details: str
    rollback: str


class EventCoalescingQueue:
    def __init__(self, debounce_seconds: float, extensions: set[str] | None = None) -> None:
        self.debounce_seconds = max(0.0, debounce_seconds)
        self.extensions = {ext.lower() for ext in (extensions or set()) if ext}
        self._pending: dict[str, float] = {}
        self._lock = threading.Lock()

    def update_extensions(self, extensions: set[str]) -> None:
        with self._lock:
            self.extensions = {ext.lower() for ext in extensions if ext}

    def enqueue(self, paths: list[str], now: float | None = None) -> None:
        current = time.monotonic() if now is None else now
        with self._lock:
            for path in paths:
                normalized = str(Path(path).resolve())
                extension = Path(normalized).suffix.lower()
                if self.extensions and extension not in self.extensions:
                    continue
                self._pending[normalized] = current + self.debounce_seconds

    def pop_ready(self, now: float | None = None) -> list[str]:
        current = time.monotonic() if now is None else now
        ready: list[str] = []
        with self._lock:
            for path, due_at in list(self._pending.items()):
                if due_at <= current:
                    ready.append(path)
                    self._pending.pop(path, None)
        return sorted(ready)


class PollingLibraryWatcher:
    def __init__(
        self,
        get_library_roots: Callable[[], list[str]],
        on_paths_detected: Callable[[list[str]], None],
        interval_seconds: float = 0.05,
        max_scan_interval_seconds: float = 30.0,
        on_status: Callable[[str], None] | None = None,
    ) -> None:
        self.get_library_roots = get_library_roots
        self.on_paths_detected = on_paths_detected
        self.interval_seconds = max(0.05, interval_seconds)
        self.max_scan_interval_seconds = max(0.05, max_scan_interval_seconds)
        self.on_status = on_status
        self._running = threading.Event()
        self._thread: threading.Thread | None = None
        self._known_paths: set[str] = set()
        self._known_dirs: set[str] = set()
        self._dir_mtimes: dict[str, float] = {}
        self._dir_children: dict[str, set[str]] = {}
        self._dir_files: dict[str, set[str]] = {}
        self._observer: Observer | None = None
        self._event_handler: _WatchdogEventHandler | None = None
        self._watch_mode = "polling"

    def configure(self, max_scan_interval_seconds: float) -> None:
        self.max_scan_interval_seconds = max(0.05, max_scan_interval_seconds)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._running.set()
        self._known_paths = set()
        self._known_dirs = set()
        self._dir_mtimes.clear()
        self._dir_children.clear()
        self._dir_files.clear()
        self._emit_status("Starting library watch…")
        self._setup_watchdog()
        self._scan_paths(force_full=True)
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 3.0) -> None:
        self._running.clear()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=timeout)
        self._teardown_watchdog()

    def _run_loop(self) -> None:
        while self._running.is_set():
            scan_started = time.monotonic()
            try:
                created_or_renamed = self._scan_paths(force_full=False)
                if created_or_renamed:
                    self.on_paths_detected(created_or_renamed)
            except Exception:
                # Never let an unexpected error silently kill the poll thread and
                # with it new-file detection; log and keep polling.
                logger.exception("Library watch poll cycle failed; continuing")
            elapsed = time.monotonic() - scan_started
            effective_interval = max(self.interval_seconds, self.max_scan_interval_seconds)
            wait_seconds = max(0.0, effective_interval - elapsed)
            self._running.wait(wait_seconds)

    def _setup_watchdog(self) -> None:
        self._watch_mode = "polling"
        if not WATCHDOG_AVAILABLE:
            self._emit_status("Watch mode active (polling fallback: install watchdog for event-based updates).")
            return
        roots = self._resolve_roots()
        if not roots:
            return
        observer = Observer()
        handler = _WatchdogEventHandler(self.on_paths_detected)
        watches = 0
        for root in roots:
            try:
                observer.schedule(handler, root, recursive=True)
                watches += 1
            except OSError:
                continue
        if watches == 0:
            self._emit_status("Watch mode active (polling fallback: unable to subscribe to library roots).")
            return
        observer.start()
        self._observer = observer
        self._event_handler = handler
        self._watch_mode = "event+poll"
        self._emit_status(f"Watch mode active (event+poll across {watches} roots).")

    def _teardown_watchdog(self) -> None:
        if self._observer is None:
            return
        self._observer.stop()
        self._observer.join(timeout=2.0)
        self._observer = None
        self._event_handler = None

    def _resolve_roots(self) -> list[str]:
        roots: list[str] = []
        for root in self.get_library_roots():
            root_path = Path(root)
            if root_path.exists() and root_path.is_dir():
                roots.append(str(root_path.resolve()))
        return sorted(set(roots))

    def _scan_paths(self, force_full: bool) -> list[str]:
        roots = self._resolve_roots()
        active_roots = set(roots)
        stale_dirs = [
            path
            for path in self._known_dirs
            if not any(path.startswith(f"{root}{os.sep}") or path == root for root in active_roots)
        ]
        for stale_dir in stale_dirs:
            self._drop_directory_state(stale_dir)

        created_or_renamed: set[str] = set()
        listed_directories = 0
        for root in roots:
            listed_directories += self._scan_directory_tree(root, created_or_renamed, force_full)

        self._emit_status(
            f"Watch mode {self._watch_mode}: {len(self._known_paths):,} files tracked across {len(self._known_dirs):,} directories "
            f"({listed_directories:,} directories listed this cycle)."
        )
        return sorted(created_or_renamed)

    def _scan_directory_tree(self, directory: str, detected_paths: set[str], force_full: bool) -> int:
        listed_directories = 0
        stack: list[str] = [directory]
        while stack:
            current = stack.pop()
            try:
                stat_result = os.stat(current)
            except OSError:
                self._drop_directory_state(current)
                continue

            previous_mtime = self._dir_mtimes.get(current)
            should_list = force_full or previous_mtime is None or stat_result.st_mtime != previous_mtime
            self._dir_mtimes[current] = stat_result.st_mtime
            self._known_dirs.add(current)

            if should_list:
                listed_directories += 1
                child_dirs: set[str] = set()
                child_files: set[str] = set()
                try:
                    with os.scandir(current) as entries:
                        for entry in entries:
                            resolved = str(Path(entry.path).resolve())
                            if entry.is_dir(follow_symlinks=False):
                                child_dirs.add(resolved)
                            elif entry.is_file(follow_symlinks=False):
                                child_files.add(resolved)
                except OSError:
                    continue

                previous_child_dirs = self._dir_children.get(current, set())
                previous_child_files = self._dir_files.get(current, set())
                for removed_dir in previous_child_dirs - child_dirs:
                    self._drop_directory_state(removed_dir)
                for removed_file in previous_child_files - child_files:
                    self._known_paths.discard(removed_file)

                detected_paths.update(child_files - previous_child_files)
                self._dir_children[current] = child_dirs
                self._dir_files[current] = child_files
                self._known_paths.update(child_files)

            stack.extend(sorted(self._dir_children.get(current, set())))

        return listed_directories

    def _drop_directory_state(self, directory: str) -> None:
        for child in list(self._dir_children.get(directory, set())):
            self._drop_directory_state(child)
        for file_path in self._dir_files.get(directory, set()):
            self._known_paths.discard(file_path)
        self._known_dirs.discard(directory)
        self._dir_mtimes.pop(directory, None)
        self._dir_children.pop(directory, None)
        self._dir_files.pop(directory, None)

    def _emit_status(self, status: str) -> None:
        if self.on_status:
            self.on_status(status)


class _WatchdogEventHandler(FileSystemEventHandler):
    def __init__(self, on_paths_detected: Callable[[list[str]], None]) -> None:
        self.on_paths_detected = on_paths_detected

    def on_any_event(self, event: FileSystemEvent) -> None:
        if getattr(event, "is_directory", False):
            return
        paths: list[str] = []
        src_path = getattr(event, "src_path", None)
        dest_path = getattr(event, "dest_path", None)
        if src_path:
            paths.append(str(Path(src_path).resolve()))
        if dest_path:
            paths.append(str(Path(dest_path).resolve()))
        if paths:
            self.on_paths_detected(paths)


class LibraryDB:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path)
        self._init_schema()
        logger.debug("LibraryDB initialized with database at %s", self.db_path)

    def _init_schema(self) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS libraries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                library_type TEXT NOT NULL,
                path TEXT NOT NULL,
                host TEXT,
                username TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS library_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                library_id INTEGER NOT NULL,
                path TEXT NOT NULL,
                name TEXT NOT NULL,
                entry_type TEXT NOT NULL,
                FOREIGN KEY (library_id) REFERENCES libraries(id)
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_library_items_lookup
            ON library_items(library_id, name, path)
            """
        )
        self._ensure_audio_signatures_schema(cursor)
        self.connection.commit()

    def _ensure_audio_signatures_schema(self, cursor: sqlite3.Cursor) -> None:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS audio_signatures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                library_id INTEGER,
                path TEXT UNIQUE,
                signature TEXT NOT NULL,
                bitrate INTEGER,
                sample_rate INTEGER,
                format TEXT,
                kept INTEGER DEFAULT 1,
                updated_at TEXT,
                FOREIGN KEY(library_id) REFERENCES libraries(id)
            )
            """
        )
        existing_columns = {
            row[1]: row for row in cursor.execute("PRAGMA table_info(audio_signatures)")
        }
        if "kept" not in existing_columns:
            cursor.execute("ALTER TABLE audio_signatures ADD COLUMN kept INTEGER DEFAULT 1")
        if "updated_at" not in existing_columns:
            cursor.execute("ALTER TABLE audio_signatures ADD COLUMN updated_at TEXT")
        if "bitrate" not in existing_columns:
            cursor.execute("ALTER TABLE audio_signatures ADD COLUMN bitrate INTEGER")
        if "sample_rate" not in existing_columns:
            cursor.execute("ALTER TABLE audio_signatures ADD COLUMN sample_rate INTEGER")
        if "format" not in existing_columns:
            cursor.execute("ALTER TABLE audio_signatures ADD COLUMN format TEXT")
        if "signature" not in existing_columns:
            cursor.execute("ALTER TABLE audio_signatures ADD COLUMN signature TEXT")
        if "library_id" not in existing_columns:
            cursor.execute("ALTER TABLE audio_signatures ADD COLUMN library_id INTEGER")
        if "path" not in existing_columns:
            cursor.execute("ALTER TABLE audio_signatures ADD COLUMN path TEXT")
        try:
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_audio_signatures_path
                ON audio_signatures(path)
                """
            )
        except sqlite3.OperationalError:
            logger.warning("Unable to enforce unique index on audio_signatures.path; proceeding without it")
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_audio_signatures_signature
            ON audio_signatures(signature)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_audio_signatures_library_signature
            ON audio_signatures(library_id, signature)
            """
        )

    def _row_to_audio_signature(self, row: tuple) -> AudioSignature:
        return AudioSignature(
            id=row[0],
            library_id=row[1],
            path=row[2],
            signature=row[3],
            bitrate=row[4],
            sample_rate=row[5],
            format=row[6],
            kept=bool(row[7]),
            updated_at=row[8],
        )

    def begin_transaction(self) -> None:
        self.connection.execute("BEGIN")

    def commit_transaction(self) -> None:
        self.connection.commit()

    def rollback_transaction(self) -> None:
        self.connection.rollback()

    @contextmanager
    def transaction(self):
        self.begin_transaction()
        try:
            yield
        except Exception:
            self.rollback_transaction()
            raise
        else:
            self.commit_transaction()

    def upsert_audio_signature(
        self,
        path: str,
        signature: str,
        library_id: int | None = None,
        bitrate: int | None = None,
        sample_rate: int | None = None,
        format_name: str | None = None,
        kept: bool = True,
        auto_commit: bool = True,
    ) -> AudioSignature:
        updated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO audio_signatures (library_id, path, signature, bitrate, sample_rate, format, kept, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                library_id = excluded.library_id,
                signature = excluded.signature,
                bitrate = excluded.bitrate,
                sample_rate = excluded.sample_rate,
                format = excluded.format,
                kept = excluded.kept,
                updated_at = excluded.updated_at
            """,
            (
                library_id,
                path,
                signature,
                bitrate,
                sample_rate,
                format_name,
                1 if kept else 0,
                updated_at,
            ),
        )
        if auto_commit:
            self.connection.commit()
        signature_row = cursor.execute(
            """
            SELECT id, library_id, path, signature, bitrate, sample_rate, format, kept, updated_at
            FROM audio_signatures
            WHERE path = ?
            """,
            (path,),
        ).fetchone()
        if not signature_row:
            raise RuntimeError("Failed to upsert audio signature.")
        return self._row_to_audio_signature(signature_row)

    def fetch_audio_signature_by_path(self, path: str) -> AudioSignature | None:
        cursor = self.connection.cursor()
        row = cursor.execute(
            """
            SELECT id, library_id, path, signature, bitrate, sample_rate, format, kept, updated_at
            FROM audio_signatures
            WHERE path = ?
            """,
            (path,),
        ).fetchone()
        if not row:
            return None
        return self._row_to_audio_signature(row)

    def fetch_duplicates_by_signature(
        self, signature: str, library_id: int | None = None
    ) -> list[AudioSignature]:
        cursor = self.connection.cursor()
        params: list[object] = [signature]
        query = """
            SELECT id, library_id, path, signature, bitrate, sample_rate, format, kept, updated_at
            FROM audio_signatures
            WHERE signature = ?
        """
        if library_id is not None:
            query += " AND library_id = ?"
            params.append(library_id)
        query += " ORDER BY kept DESC, bitrate DESC, sample_rate DESC"
        return [self._row_to_audio_signature(row) for row in cursor.execute(query, params).fetchall()]

    def find_library_by_path(self, path: str) -> Library | None:
        target_path = os.path.abspath(path)
        for library in self.fetch_libraries():
            if os.path.abspath(library.path) == target_path:
                return library
        return None

    def clear_library_items(self, library_id: int) -> None:
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM library_items WHERE library_id = ?", (library_id,))
        self.connection.commit()

    def index_library_items(self, library: Library, max_records: int = 50000) -> None:
        if library.library_type != "local" or not os.path.isdir(library.path):
            logger.debug(
                "Skipping indexing for library %s (type=%s, path exists=%s)",
                library.name,
                library.library_type,
                os.path.isdir(library.path),
            )
            return
        logger.info("Indexing library items for '%s' at %s", library.name, library.path)
        self.clear_library_items(library.library_id)
        cursor = self.connection.cursor()
        rows: list[tuple[int, str, str, str]] = []
        for root, dirs, files in os.walk(library.path):
            for folder_name in dirs:
                if len(rows) >= max_records:
                    break
                rows.append(
                    (library.library_id, os.path.join(root, folder_name), folder_name, "Folder")
                )
            if len(rows) >= max_records:
                break
            for file_name in files:
                if len(rows) >= max_records:
                    break
                rows.append(
                    (library.library_id, os.path.join(root, file_name), file_name, "File")
                )
            if len(rows) >= max_records:
                break
        cursor.executemany(
            """
            INSERT INTO library_items (library_id, path, name, entry_type)
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )
        records_inserted = len(rows)
        self.connection.commit()
        logger.info(
            "Indexed %s records for library '%s'%s",
            records_inserted,
            library.name,
            " (truncated)" if records_inserted >= max_records else "",
        )

    def search_items(self, term: str, limit: int = 200) -> list[tuple[int, str, str, str]]:
        cursor = self.connection.cursor()
        escaped_term = (
            term.lower().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        )
        like_term = f"%{escaped_term}%"
        cursor.execute(
            """
            SELECT library_id, path, name, entry_type
            FROM library_items
            WHERE lower(name) LIKE ? ESCAPE '\\' OR lower(path) LIKE ? ESCAPE '\\'
            ORDER BY name
            LIMIT ?
            """,
            (like_term, like_term, limit),
        )
        logger.debug("Search query for term '%s' with limit %s", term, limit)
        return [(row[0], row[1], row[2], row[3]) for row in cursor.fetchall()]

    def fetch_library_items(self, library_id: int) -> list[tuple[str, str, str]]:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT path, name, entry_type
            FROM library_items
            WHERE library_id = ?
            ORDER BY path
            """,
            (library_id,),
        )
        return [(row[0], row[1], row[2]) for row in cursor.fetchall()]

    def fetch_libraries(self) -> list[Library]:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT id, name, library_type, path, host, username
            FROM libraries
            ORDER BY id
            """
        )
        return [
            Library(
                library_id=row[0],
                name=row[1],
                library_type=row[2],
                path=row[3],
                host=row[4],
                username=row[5],
            )
            for row in cursor.fetchall()
        ]

    def add_library(
        self, name: str, library_type: str, path: str, host: str | None, username: str | None
    ) -> Library:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO libraries (name, library_type, path, host, username)
            VALUES (?, ?, ?, ?, ?)
            """,
            (name, library_type, path, host, username),
        )
        self.connection.commit()
        return Library(
            library_id=cursor.lastrowid,
            name=name,
            library_type=library_type,
            path=path,
            host=host,
            username=username,
        )

    def update_library(
        self,
        library_id: int,
        name: str,
        library_type: str,
        path: str,
        host: str | None,
        username: str | None,
    ) -> Library:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            UPDATE libraries
            SET name = ?, library_type = ?, path = ?, host = ?, username = ?
            WHERE id = ?
            """,
            (name, library_type, path, host, username, library_id),
        )
        self.connection.commit()
        return Library(
            library_id=library_id,
            name=name,
            library_type=library_type,
            path=path,
            host=host,
            username=username,
        )

    def delete_library(self, library_id: int) -> None:
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM library_items WHERE library_id = ?", (library_id,))
        cursor.execute(
            "UPDATE audio_signatures SET library_id = NULL WHERE library_id = ?",
            (library_id,),
        )
        cursor.execute("DELETE FROM libraries WHERE id = ?", (library_id,))
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


class PlaylistManager:
    def __init__(self, playlist_dir: Path) -> None:
        self.playlist_dir = playlist_dir
        self.playlist_dir.mkdir(parents=True, exist_ok=True)
        self.playlists: dict[str, list[str]] = {}
        self.load_playlists()

    def load_playlists(self) -> None:
        self.playlists.clear()
        for playlist_file in sorted(self.playlist_dir.glob("*.m3u")):
            name = playlist_file.stem
            try:
                lines = playlist_file.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue
            entries = [
                line.strip()
                for line in lines
                if line.strip() and not line.strip().startswith("#")
            ]
            self.playlists[name] = entries

    def _playlist_path(self, name: str) -> Path:
        safe_name = re.sub(r"[^\w\-\s\.]", "_", name).strip() or "playlist"
        return self.playlist_dir / f"{safe_name}.m3u"

    def create_playlist(self, name: str) -> str:
        name = name.strip()
        if not name:
            raise ValueError("Playlist name cannot be empty.")
        if name in self.playlists:
            return name
        self.playlists[name] = []
        self._save_playlist(name)
        return name

    def add_item(self, name: str, path: str) -> None:
        if name not in self.playlists:
            self.create_playlist(name)
        normalized_path = os.path.abspath(path)
        if normalized_path not in self.playlists[name]:
            self.playlists[name].append(normalized_path)
            self._save_playlist(name)

    def remove_item(self, name: str, path: str) -> None:
        if name not in self.playlists:
            return
        normalized_path = os.path.abspath(path)
        try:
            self.playlists[name].remove(normalized_path)
        except ValueError:
            return
        self._save_playlist(name)

    def delete_playlist(self, name: str) -> None:
        self.playlists.pop(name, None)
        playlist_path = self._playlist_path(name)
        if playlist_path.exists():
            try:
                playlist_path.unlink()
            except OSError:
                pass

    def _save_playlist(self, name: str) -> None:
        playlist_path = self._playlist_path(name)
        playlist_path.parent.mkdir(parents=True, exist_ok=True)
        entries = self.playlists.get(name, [])
        content_lines = ["#EXTM3U", *entries]
        playlist_path.write_text("\n".join(content_lines) + "\n", encoding="utf-8")

    def rename_playlist(self, old_name: str, new_name: str) -> str:
        if old_name not in self.playlists:
            raise ValueError("Playlist does not exist.")
        new_name = new_name.strip()
        if not new_name:
            raise ValueError("Playlist name cannot be empty.")
        if new_name == old_name:
            return old_name
        if new_name in self.playlists:
            raise ValueError("A playlist with that name already exists.")
        entries = self.playlists.pop(old_name)
        old_path = self._playlist_path(old_name)
        self.playlists[new_name] = entries
        if old_path.exists():
            try:
                old_path.unlink()
            except OSError:
                pass
        self._save_playlist(new_name)
        return new_name


class NewLibraryDialog(tk.Toplevel):
    def __init__(self, master: tk.Tk) -> None:
        super().__init__(master)
        self.title("Add Library")
        self.resizable(False, False)
        self.result: dict[str, str | None] | None = None

        self.name_var = tk.StringVar()
        self.type_var = tk.StringVar(value="local")
        self.path_var = tk.StringVar()
        self.host_var = tk.StringVar()
        self.user_var = tk.StringVar()

        frame = ttk.Frame(self, padding=12)
        frame.grid(sticky="nsew")
        self.columnconfigure(0, weight=1)

        ttk.Label(frame, text="Name").grid(row=0, column=0, sticky="w")
        ttk.Entry(frame, textvariable=self.name_var, width=40).grid(row=0, column=1, sticky="ew")

        ttk.Label(frame, text="Type").grid(row=1, column=0, sticky="w", pady=(8, 0))
        type_frame = ttk.Frame(frame)
        type_frame.grid(row=1, column=1, sticky="w", pady=(8, 0))
        ttk.Radiobutton(type_frame, text="Local", variable=self.type_var, value="local").pack(
            side="left"
        )
        ttk.Radiobutton(type_frame, text="Remote (SSH)", variable=self.type_var, value="remote").pack(
            side="left", padx=(12, 0)
        )

        ttk.Label(frame, text="Path").grid(row=2, column=0, sticky="w", pady=(8, 0))
        path_frame = ttk.Frame(frame)
        path_frame.grid(row=2, column=1, sticky="ew", pady=(8, 0))
        path_entry = ttk.Entry(path_frame, textvariable=self.path_var, width=32)
        path_entry.pack(side="left", fill="x", expand=True)
        self.browse_button = ttk.Button(path_frame, text="Browse", command=self._browse)
        self.browse_button.pack(side="left", padx=(6, 0))

        ttk.Label(frame, text="Host").grid(row=3, column=0, sticky="w", pady=(8, 0))
        self.host_entry = ttk.Entry(frame, textvariable=self.host_var)
        self.host_entry.grid(row=3, column=1, sticky="ew", pady=(8, 0))

        ttk.Label(frame, text="Username").grid(row=4, column=0, sticky="w", pady=(8, 0))
        self.user_entry = ttk.Entry(frame, textvariable=self.user_var)
        self.user_entry.grid(row=4, column=1, sticky="ew", pady=(8, 0))

        button_frame = ttk.Frame(frame)
        button_frame.grid(row=5, column=0, columnspan=2, pady=(12, 0), sticky="e")
        ttk.Button(button_frame, text="Cancel", command=self._cancel).pack(side="right")
        ttk.Button(button_frame, text="Add", command=self._submit).pack(side="right", padx=(0, 8))

        frame.columnconfigure(1, weight=1)
        self.bind("<Return>", lambda event: self._submit())
        self.bind("<Escape>", lambda event: self._cancel())
        self.type_var.trace_add("write", lambda *_: self._toggle_remote_fields())
        self._toggle_remote_fields()
        self.grab_set()
        self.wait_visibility()
        self.focus_set()

    def _browse(self) -> None:
        if self.type_var.get() == "local":
            folder = filedialog.askdirectory(parent=self)
            if folder:
                self.path_var.set(folder)

    def _toggle_remote_fields(self) -> None:
        is_remote = self.type_var.get() == "remote"
        state = "normal" if is_remote else "disabled"
        self.host_entry.configure(state=state)
        self.user_entry.configure(state=state)
        self.browse_button.configure(state="disabled" if is_remote else "normal")
        if not is_remote:
            self.host_var.set("")
            self.user_var.set("")

    def _submit(self) -> None:
        name = self.name_var.get().strip()
        library_type = self.type_var.get().strip()
        path = self.path_var.get().strip()
        host = self.host_var.get().strip() or None
        username = self.user_var.get().strip() or None

        if not name:
            messagebox.showerror("Missing name", "Please enter a library name.", parent=self)
            return
        if not path:
            messagebox.showerror("Missing path", "Please enter a library path.", parent=self)
            return
        if library_type == "remote" and not host:
            messagebox.showerror("Missing host", "Remote libraries require a host.", parent=self)
            return

        self.result = {
            "name": name,
            "library_type": library_type,
            "path": path,
            "host": host,
            "username": username,
        }
        self.destroy()

    def _cancel(self) -> None:
        self.destroy()


class LibraryManagementDialog(tk.Toplevel):
    def __init__(self, master: tk.Tk, db: LibraryDB) -> None:
        super().__init__(master)
        self.title("Library Management")
        self.resizable(False, False)
        self.db = db
        self.selected_library_id: int | None = None

        self.name_var = tk.StringVar()
        self.type_var = tk.StringVar(value="local")
        self.path_var = tk.StringVar()
        self.host_var = tk.StringVar()
        self.user_var = tk.StringVar()

        container = ttk.Frame(self, padding=12)
        container.grid(sticky="nsew")
        container.columnconfigure(1, weight=1)

        self.library_list = ttk.Treeview(
            container, columns=("name", "type", "path"), show="headings", height=8
        )
        self.library_list.heading("name", text="Name")
        self.library_list.heading("type", text="Type")
        self.library_list.heading("path", text="Path")
        self.library_list.column("name", width=140, anchor="w")
        self.library_list.column("type", width=80, anchor="w")
        self.library_list.column("path", width=230, anchor="w")
        self.library_list.grid(row=0, column=0, rowspan=6, sticky="nsw", padx=(0, 12))
        self.library_list.bind("<<TreeviewSelect>>", self._on_library_selected)

        ttk.Label(container, text="Name").grid(row=0, column=1, sticky="w")
        ttk.Entry(container, textvariable=self.name_var, width=36).grid(
            row=0, column=2, sticky="ew"
        )

        ttk.Label(container, text="Type").grid(row=1, column=1, sticky="w", pady=(6, 0))
        type_frame = ttk.Frame(container)
        type_frame.grid(row=1, column=2, sticky="w", pady=(6, 0))
        ttk.Radiobutton(type_frame, text="Local", variable=self.type_var, value="local").pack(
            side="left"
        )
        ttk.Radiobutton(type_frame, text="Remote", variable=self.type_var, value="remote").pack(
            side="left", padx=(12, 0)
        )

        ttk.Label(container, text="Path").grid(row=2, column=1, sticky="w", pady=(6, 0))
        path_frame = ttk.Frame(container)
        path_frame.grid(row=2, column=2, sticky="ew", pady=(6, 0))
        ttk.Entry(path_frame, textvariable=self.path_var, width=28).pack(
            side="left", fill="x", expand=True
        )
        self.browse_button = ttk.Button(path_frame, text="Browse", command=self._browse)
        self.browse_button.pack(side="left", padx=(6, 0))

        ttk.Label(container, text="Host").grid(row=3, column=1, sticky="w", pady=(6, 0))
        self.host_entry = ttk.Entry(container, textvariable=self.host_var, width=28)
        self.host_entry.grid(row=3, column=2, sticky="ew", pady=(6, 0))

        ttk.Label(container, text="Username").grid(row=4, column=1, sticky="w", pady=(6, 0))
        self.user_entry = ttk.Entry(container, textvariable=self.user_var, width=28)
        self.user_entry.grid(row=4, column=2, sticky="ew", pady=(6, 0))

        button_frame = ttk.Frame(container)
        button_frame.grid(row=5, column=1, columnspan=2, sticky="e", pady=(10, 0))
        ttk.Button(button_frame, text="Add Library", command=self._add_library).pack(
            side="left"
        )
        ttk.Button(button_frame, text="Remove", command=self._remove_library).pack(
            side="right", padx=(6, 0)
        )
        ttk.Button(button_frame, text="Save Changes", command=self._save_library).pack(
            side="right"
        )

        container.columnconfigure(2, weight=1)
        self.type_var.trace_add("write", lambda *_: self._toggle_remote_fields())
        self._toggle_remote_fields()
        self._load_libraries()
        self.grab_set()
        self.wait_visibility()
        self.focus_set()

    def _load_libraries(self) -> None:
        for item in self.library_list.get_children():
            self.library_list.delete(item)
        for library in self.db.fetch_libraries():
            self.library_list.insert(
                "",
                "end",
                iid=str(library.library_id),
                values=(library.name, library.library_type, library.path),
            )
        if self.library_list.get_children():
            self.library_list.selection_set(self.library_list.get_children()[0])
            self._on_library_selected()

    def _on_library_selected(self, _event: tk.Event | None = None) -> None:
        selection = self.library_list.selection()
        if not selection:
            return
        library_id = int(selection[0])
        library = next(
            (item for item in self.db.fetch_libraries() if item.library_id == library_id), None
        )
        if not library:
            return
        self.selected_library_id = library.library_id
        self.name_var.set(library.name)
        self.type_var.set(library.library_type)
        self.path_var.set(library.path)
        self.host_var.set(library.host or "")
        self.user_var.set(library.username or "")

    def _browse(self) -> None:
        if self.type_var.get() == "local":
            folder = filedialog.askdirectory(parent=self)
            if folder:
                self.path_var.set(folder)

    def _toggle_remote_fields(self) -> None:
        is_remote = self.type_var.get() == "remote"
        state = "normal" if is_remote else "disabled"
        self.host_entry.configure(state=state)
        self.user_entry.configure(state=state)
        self.browse_button.configure(state="disabled" if is_remote else "normal")
        if not is_remote:
            self.host_var.set("")
            self.user_var.set("")

    def _validate_form(self) -> bool:
        name = self.name_var.get().strip()
        path = self.path_var.get().strip()
        if not name:
            messagebox.showerror("Missing name", "Please enter a library name.", parent=self)
            return False
        if not path:
            messagebox.showerror("Missing path", "Please enter a library path.", parent=self)
            return False
        if self.type_var.get() == "remote" and not self.host_var.get().strip():
            messagebox.showerror("Missing host", "Remote libraries require a host.", parent=self)
            return False
        return True

    def _add_library(self) -> None:
        dialog = NewLibraryDialog(self)
        self.wait_window(dialog)
        if not dialog.result:
            return
        self.db.add_library(
            name=dialog.result["name"],
            library_type=dialog.result["library_type"],
            path=dialog.result["path"],
            host=dialog.result["host"],
            username=dialog.result["username"],
        )
        self._load_libraries()

    def _save_library(self) -> None:
        if self.selected_library_id is None:
            messagebox.showinfo("Library Management", "Select a library to edit.", parent=self)
            return
        if not self._validate_form():
            return
        self.db.update_library(
            library_id=self.selected_library_id,
            name=self.name_var.get().strip(),
            library_type=self.type_var.get().strip(),
            path=self.path_var.get().strip(),
            host=self.host_var.get().strip() or None,
            username=self.user_var.get().strip() or None,
        )
        self._load_libraries()
        self.library_list.selection_set(str(self.selected_library_id))
        self._on_library_selected()

    def _remove_library(self) -> None:
        if self.selected_library_id is None:
            messagebox.showinfo("Library Management", "Select a library to remove.", parent=self)
            return
        if not messagebox.askyesno(
            "Remove Library",
            "Are you sure you want to remove this library?",
            parent=self,
        ):
            return
        self.db.delete_library(self.selected_library_id)
        self.selected_library_id = None
        self._load_libraries()


class ThemeEditorDialog(tk.Toplevel):
    COMPONENTS = [
        ("window_background", "Window Background"),
        ("sidebar_background", "Sidebar Background"),
        ("toolbar_background", "Toolbar Background"),
        ("treeview_background", "Library Tree Background"),
        ("metadata_background", "Metadata Panel Background"),
        ("accent_color", "Accent Color"),
        ("text_color", "Primary Text"),
    ]

    def __init__(
        self,
        master: tk.Tk,
        themes: dict[str, dict[str, str]],
        apply_theme_callback: Callable[[dict[str, str]], None] | None = None,
        save_theme_callback: Callable[[str, dict[str, str]], Path] | None = None,
    ) -> None:
        super().__init__(master)
        self.title("Theme Editor")
        self.geometry("940x520")
        self.resizable(False, False)

        self.themes = themes
        self.theme_names = list(self.themes.keys())
        self.selected_theme_name = tk.StringVar(value=self.theme_names[0])
        self.selected_component: str | None = None
        self.apply_theme_callback = apply_theme_callback
        self.save_theme_callback = save_theme_callback

        self.current_color = tk.StringVar(value="#3b74ff")
        self.hex_entry = tk.StringVar()
        self.picker_mode = tk.StringVar(value="screen")

        container = ttk.Frame(self, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(1, weight=1)
        container.rowconfigure(0, weight=1)

        self._build_theme_list(container)
        self._build_editor_panel(container)
        self._load_theme(self.theme_names[0])

        self.grab_set()
        self.wait_visibility()
        self.focus_set()

    def _build_theme_list(self, parent: ttk.Frame) -> None:
        left_frame = ttk.Frame(parent)
        left_frame.grid(row=0, column=0, sticky="ns")

        ttk.Label(left_frame, text="Themes", font=("Segoe UI", 11, "bold")).pack(
            anchor="w"
        )
        list_frame = ttk.Frame(left_frame)
        list_frame.pack(fill="y", pady=(8, 0))

        self.theme_listbox = tk.Listbox(list_frame, height=16, width=22)
        scroll = ttk.Scrollbar(list_frame, orient="vertical", command=self.theme_listbox.yview)
        self.theme_listbox.configure(yscrollcommand=scroll.set)
        self.theme_listbox.pack(side="left", fill="y")
        scroll.pack(side="right", fill="y")

        for name in self.theme_names:
            self.theme_listbox.insert("end", name)
        self.theme_listbox.selection_set(0)
        self.theme_listbox.bind("<<ListboxSelect>>", self._on_theme_selected)

        button_frame = ttk.Frame(left_frame)
        button_frame.pack(fill="x", pady=(12, 0))
        ttk.Button(button_frame, text="New Theme", command=self._create_new_theme).pack(
            fill="x", pady=(0, 6)
        )
        ttk.Button(button_frame, text="Save Theme", command=self._save_theme).pack(
            fill="x", pady=(0, 6)
        )
        ttk.Button(button_frame, text="Apply Theme", command=self._apply_theme).pack(fill="x")

    def _build_editor_panel(self, parent: ttk.Frame) -> None:
        right_frame = ttk.Frame(parent)
        right_frame.grid(row=0, column=1, sticky="nsew", padx=(18, 0))
        right_frame.columnconfigure(0, weight=1)
        right_frame.rowconfigure(1, weight=1)

        ttk.Label(right_frame, text="Theme Colors", font=("Segoe UI", 11, "bold")).grid(
            row=0, column=0, sticky="w"
        )

        editor_frame = ttk.Frame(right_frame)
        editor_frame.grid(row=1, column=0, sticky="nsew", pady=(8, 0))
        editor_frame.columnconfigure(0, weight=1)
        editor_frame.rowconfigure(1, weight=1)

        self.component_tree = ttk.Treeview(
            editor_frame,
            columns=("component", "color"),
            show="headings",
            height=8,
        )
        self.component_tree.heading("component", text="Component")
        self.component_tree.heading("color", text="Color")
        self.component_tree.column("component", width=260)
        self.component_tree.column("color", width=120, anchor="center")
        self.component_tree.grid(row=0, column=0, sticky="nsew")
        self.component_tree.bind("<<TreeviewSelect>>", self._on_component_selected)

        picker_panel = ttk.Frame(editor_frame, padding=(0, 12, 0, 0))
        picker_panel.grid(row=1, column=0, sticky="nsew")
        picker_panel.columnconfigure(1, weight=1)

        mode_frame = ttk.Labelframe(picker_panel, text="Color Picker Options")
        mode_frame.grid(row=0, column=0, sticky="ew", columnspan=2)

        ttk.Radiobutton(
            mode_frame,
            text="Color Picker",
            variable=self.picker_mode,
            value="screen",
            command=self._show_picker_mode,
        ).grid(row=0, column=0, sticky="w", padx=8, pady=6)
        ttk.Radiobutton(
            mode_frame,
            text="Hex code",
            variable=self.picker_mode,
            value="hex",
            command=self._show_picker_mode,
        ).grid(row=0, column=1, sticky="w", padx=8, pady=6)

        self.mode_container = ttk.Frame(picker_panel)
        self.mode_container.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
        self.mode_container.columnconfigure(0, weight=1)

        self.mode_frames: dict[str, ttk.Frame] = {}
        self._build_screen_picker(self.mode_container)
        self._build_hex_picker(self.mode_container)
        self._show_picker_mode()

        preview_frame = ttk.Frame(picker_panel)
        preview_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(12, 0))

        ttk.Label(preview_frame, text="Selected Color:").pack(side="left")
        self.color_swatch = tk.Canvas(preview_frame, width=32, height=18, highlightthickness=1)
        self.color_swatch.pack(side="left", padx=(8, 6))
        self.color_label = ttk.Label(preview_frame, textvariable=self.current_color)
        self.color_label.pack(side="left")

        ttk.Button(
            preview_frame, text="Apply to Component", command=self._apply_color_to_component
        ).pack(side="right")

    def _build_screen_picker(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent)
        frame.columnconfigure(0, weight=1)
        ttk.Label(
            frame,
            text="Use the system color picker to sample a color from anywhere on screen.",
            wraplength=520,
        ).grid(row=0, column=0, sticky="w")
        ttk.Button(frame, text="Pick Color", command=self._pick_screen_color).grid(
            row=1, column=0, sticky="w", pady=(8, 0)
        )
        self.mode_frames["screen"] = frame

    def _build_hex_picker(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent)
        frame.columnconfigure(1, weight=1)
        ttk.Label(frame, text="Enter a hex color code:").grid(row=0, column=0, sticky="w")
        entry = ttk.Entry(frame, textvariable=self.hex_entry, width=14)
        entry.grid(row=0, column=1, sticky="w", padx=(8, 0))
        ttk.Button(frame, text="Apply Hex", command=self._apply_hex_color).grid(
            row=0, column=2, sticky="w", padx=(8, 0)
        )
        self.mode_frames["hex"] = frame

    def _show_picker_mode(self) -> None:
        for frame in self.mode_frames.values():
            frame.grid_forget()
        frame = self.mode_frames.get(self.picker_mode.get())
        if frame:
            frame.grid(row=0, column=0, sticky="ew")

    def _on_theme_selected(self, event: tk.Event) -> None:
        selection = self.theme_listbox.curselection()
        if not selection:
            return
        theme_name = self.theme_listbox.get(selection[0])
        self._load_theme(theme_name)

    def _load_theme(self, theme_name: str) -> None:
        self.selected_theme_name.set(theme_name)
        self.component_tree.delete(*self.component_tree.get_children())
        theme = self.themes.get(theme_name, {})
        for key, label in self.COMPONENTS:
            color = theme.get(key, "#ffffff")
            self.component_tree.insert("", "end", iid=key, values=(label, color))
        self.component_tree.selection_set(self.COMPONENTS[0][0])
        self._set_current_color(theme.get(self.COMPONENTS[0][0], "#ffffff"))

    def _on_component_selected(self, event: tk.Event) -> None:
        selection = self.component_tree.selection()
        if not selection:
            return
        component_id = selection[0]
        self.selected_component = component_id
        color = self.component_tree.set(component_id, "color")
        if color:
            self._set_current_color(color)

    def _set_current_color(self, color: str) -> None:
        self.current_color.set(color)
        self.color_swatch.configure(background=color)
        self.hex_entry.set(color)

    def _pick_screen_color(self) -> None:
        result = colorchooser.askcolor(color=self.current_color.get(), parent=self)
        if result and result[1]:
            self._set_current_color(result[1])

    def _apply_hex_color(self) -> None:
        value = self.hex_entry.get().strip()
        if not value.startswith("#"):
            value = f"#{value}"
        if len(value) != 7 or any(char not in "0123456789abcdefABCDEF" for char in value[1:]):
            messagebox.showerror("Invalid Color", "Enter a valid hex color (e.g. #33AADD).")
            return
        self._set_current_color(value.lower())

    def _apply_color_to_component(self) -> None:
        selection = self.component_tree.selection()
        if not selection:
            messagebox.showinfo("Theme Editor", "Select a component to update.")
            return
        component_id = selection[0]
        theme_name = self.selected_theme_name.get()
        color = self.current_color.get()
        self.component_tree.set(component_id, "color", color)
        self.themes.setdefault(theme_name, {})[component_id] = color

    def _create_new_theme(self) -> None:
        name = simpledialog.askstring("New Theme", "Theme name:", parent=self)
        if not name:
            return
        if name in self.themes:
            messagebox.showerror("New Theme", "That theme name already exists.")
            return
        current_theme = self.themes.get(self.selected_theme_name.get(), {})
        self.themes[name] = dict(current_theme)
        self.theme_names.append(name)
        self.theme_listbox.insert("end", name)
        self.theme_listbox.selection_clear(0, "end")
        self.theme_listbox.selection_set("end")
        self._load_theme(name)

    def _save_theme(self) -> None:
        theme_name = self.selected_theme_name.get()
        if not theme_name:
            return
        theme = self.themes.get(theme_name, {})
        if self.save_theme_callback:
            try:
                saved_path = self.save_theme_callback(theme_name, theme)
            except OSError as exc:
                messagebox.showerror(
                    "Theme Editor", f"Unable to save theme: {exc}", parent=self
                )
                return
            messagebox.showinfo(
                "Theme Editor", f"Theme '{theme_name}' saved to:\n{saved_path}", parent=self
            )
            return
        messagebox.showinfo("Theme Editor", f"Theme '{theme_name}' saved.", parent=self)

    def _apply_theme(self) -> None:
        theme_name = self.selected_theme_name.get()
        if not theme_name:
            return
        theme = self.themes.get(theme_name, {})
        if self.apply_theme_callback:
            self.apply_theme_callback(theme)


class WorkflowsDialog(tk.Toplevel):
    def __init__(self, master: tk.Tk) -> None:
        super().__init__(master)
        self.title("Workflows")
        self.geometry("860x520")
        self.resizable(False, False)

        self.workflow_names = self._load_workflows()
        self.workflow_runners = {
            name: load_workflow_runner(name) for name in self.workflow_names
        }
        self.description_var = tk.StringVar(value="Select a workflow to see details.")

        container = ttk.Frame(self, padding=12)
        container.grid(sticky="nsew")
        container.columnconfigure(1, weight=1)
        container.rowconfigure(1, weight=1)

        ttk.Label(container, text="Workflows").grid(row=0, column=0, sticky="w")
        ttk.Label(container, text="Workflow description").grid(row=0, column=1, sticky="w")

        list_frame = ttk.Frame(container)
        list_frame.grid(row=1, column=0, rowspan=2, sticky="nsw", padx=(0, 16))
        self.workflow_list = tk.Listbox(list_frame, height=18, width=26, exportselection=False)
        list_scroll = ttk.Scrollbar(list_frame, orient="vertical", command=self.workflow_list.yview)
        self.workflow_list.configure(yscrollcommand=list_scroll.set)
        self.workflow_list.grid(row=0, column=0, sticky="ns")
        list_scroll.grid(row=0, column=1, sticky="ns")
        self.workflow_list.bind("<<ListboxSelect>>", self._on_workflow_selected)

        description_frame = ttk.Frame(container)
        description_frame.grid(row=1, column=1, sticky="ew")
        description_frame.columnconfigure(0, weight=1)
        description_label = ttk.Label(
            description_frame,
            textvariable=self.description_var,
            wraplength=520,
            justify="left",
        )
        description_label.grid(row=0, column=0, sticky="w")

        self.run_button = ttk.Button(description_frame, text="Run Workflow", command=self._run_workflow)
        self.run_button.grid(row=0, column=1, sticky="e", padx=(12, 0))

        preview_frame = ttk.Labelframe(container, text="Workflow preview", padding=12)
        preview_frame.grid(row=2, column=1, sticky="nsew", pady=(12, 0))
        preview_frame.columnconfigure(0, weight=1)
        preview_frame.rowconfigure(0, weight=1)
        ttk.Label(preview_frame, text="Visual diagram of the workflow").grid(
            row=0, column=0, sticky="nsew"
        )

        button_frame = ttk.Frame(container)
        button_frame.grid(row=3, column=1, sticky="e", pady=(12, 0))
        ttk.Button(button_frame, text="Close", command=self.destroy).pack(side="right")

        self._populate_workflows()
        self._update_run_state()
        self.grab_set()
        self.wait_visibility()
        self.focus_set()

    def _load_workflows(self) -> list[str]:
        return available_workflow_names()

    def _populate_workflows(self) -> None:
        self.workflow_list.delete(0, tk.END)
        if not self.workflow_names:
            self.workflow_list.insert(tk.END, "No workflows available yet.")
            self.workflow_list.configure(state="disabled")
            return
        for name in self.workflow_names:
            self.workflow_list.insert(tk.END, name)

    def _on_workflow_selected(self, _event: tk.Event) -> None:
        if not self.workflow_names:
            return
        selection = self.workflow_list.curselection()
        if not selection:
            self.description_var.set("Select a workflow to see details.")
            self._update_run_state()
            return
        selected_name = self.workflow_list.get(selection[0])
        runner = self.workflow_runners.get(selected_name)
        description = getattr(runner, "description", None) if runner else None
        if description:
            self.description_var.set(description)
        else:
            self.description_var.set(f"{selected_name} workflow details will appear here.")
        self._update_run_state()

    def _update_run_state(self) -> None:
        has_selection = bool(self.workflow_list.curselection()) and bool(self.workflow_names)
        self.run_button.configure(state="normal" if has_selection else "disabled")

    def _run_workflow(self) -> None:
        if not self.workflow_names:
            return
        selection = self.workflow_list.curselection()
        if not selection:
            return
        selected_name = self.workflow_list.get(selection[0])
        runner = self.workflow_runners.get(selected_name)
        if not runner:
            messagebox.showerror(
                "Workflow Error",
                "Unable to load the selected workflow runner.",
                parent=self,
            )
            return
        WorkflowProcessWizard(self, selected_name, runner).open_options()


class WorkflowProcessWizard:
    def __init__(self, master: tk.Toplevel, workflow_name: str, runner: object) -> None:
        self.master = master
        self.workflow_name = workflow_name
        self.runner = runner
        self._options_modal: WorkflowProcessModal | None = None
        self._preview_modal: WorkflowProcessModal | None = None
        self._review_modal: WorkflowProcessModal | None = None
        self._option_definitions: list[tuple[str, str]] = []
        self._options: dict[str, str] = {}
        self._plan: object | None = None
        self._rollback_script: Path | None = None
        self._rollback_powershell_script: Path | None = None

    def open_options(self) -> None:
        if self._options_modal:
            self._options_modal.destroy()
        option_definitions = getattr(self.runner, "option_definitions")()
        self._option_definitions = [(option.key, option.label) for option in option_definitions]
        list_items = [(option.label, option.value) for option in option_definitions]
        self._options_modal = WorkflowProcessModal(
            master=self.master,
            title="Workflow Options",
            header=f"{self.workflow_name} workflow options",
            list_title="Options",
            list_items=list_items,
            editable=True,
            buttons=[
                ("Back", self._close_options),
                ("Next", self._open_preview),
            ],
        )

    def _close_options(self) -> None:
        if self._options_modal:
            self._options_modal.destroy()
            self._options_modal = None

    def _open_preview(self) -> None:
        if self._options_modal:
            self._options = self._options_modal.get_option_values(self._option_definitions)
            self._options_modal.destroy()
            self._options_modal = None
        try:
            self._plan = getattr(self.runner, "build_plan")(self._options)
            preview_items = getattr(self.runner, "preview_items")(self._plan)
            duplicate_groups = getattr(self._plan, "duplicates", None)
            planned_actions = getattr(self._plan, "actions", None)
            if isinstance(duplicate_groups, list) and isinstance(planned_actions, list):
                details: list[tuple[str, str]] = [
                    ("Duplicate groups", str(len(duplicate_groups))),
                    ("Planned actions", str(len(planned_actions))),
                ]
                preview_count = min(3, len(duplicate_groups))
                for index, group in enumerate(duplicate_groups[:preview_count], start=1):
                    best = getattr(group, "best", None)
                    best_path = getattr(best, "path", None)
                    details.append((f"Group {index} best", str(best_path) if best_path else "Unknown"))
                preview_items = details + preview_items
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror(
                "Workflow Error",
                f"Unable to build preview: {exc}",
                parent=self.master,
            )
            self.open_options()
            return
        if self._preview_modal:
            self._preview_modal.destroy()
        self._preview_modal = WorkflowProcessModal(
            master=self.master,
            title="Preview Changes",
            header=f"{self.workflow_name} preview changes",
            list_title="Planned changes",
            list_items=preview_items,
            editable=False,
            buttons=[
                ("Back", self._back_to_options),
                ("Next", self._open_review),
            ],
        )

    def _back_to_options(self) -> None:
        if self._preview_modal:
            self._preview_modal.destroy()
            self._preview_modal = None
        self.open_options()

    def _open_review(self) -> None:
        if self._preview_modal:
            self._preview_modal.destroy()
            self._preview_modal = None
        if self._review_modal:
            self._review_modal.destroy()
        try:
            if self._plan is None:
                raise ValueError("No preview plan available.")
            result = getattr(self.runner, "apply")(self._options, self._plan)
            self._rollback_script = getattr(result, "rollback_script", None)
            self._rollback_powershell_script = getattr(result, "rollback_powershell_script", None)
            review_items = getattr(result, "summary_items", [])
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror(
                "Workflow Error",
                f"Unable to apply changes: {exc}",
                parent=self.master,
            )
            self.open_options()
            return
        self._review_modal = WorkflowProcessModal(
            master=self.master,
            title="Review Changes",
            header=f"{self.workflow_name} results",
            list_title="Completed changes",
            list_items=review_items,
            editable=False,
            buttons=[
                ("Rollback", self._rollback),
                ("Finish", self._finish_review),
            ],
        )

    def _rollback(self) -> None:
        if self._review_modal:
            rollback_script = self._select_rollback_script()
            if not rollback_script:
                messagebox.showinfo(
                    "Rollback",
                    "No rollback script was generated for this run.",
                    parent=self._review_modal,
                )
                return
            result = getattr(self.runner, "rollback")(rollback_script)
            summary_items = getattr(result, "summary_items", [("Rollback", "Completed")])
            self._review_modal.update_list(summary_items)

    def _select_rollback_script(self) -> Path | None:
        if os.name == "nt" and self._rollback_powershell_script:
            return self._rollback_powershell_script
        return self._rollback_script

    def _finish_review(self) -> None:
        if self._review_modal:
            self._review_modal.destroy()
            self._review_modal = None


class WorkflowProcessModal(tk.Toplevel):
    def __init__(
        self,
        master: tk.Toplevel,
        title: str,
        header: str,
        list_title: str,
        list_items: list[tuple[str, str]],
        editable: bool,
        buttons: list[tuple[str, Callable[[], None]]],
    ) -> None:
        super().__init__(master)
        self.title(title)
        self.geometry("760x420")
        self.resizable(False, False)
        self.transient(master)

        self._edit_entry: ttk.Entry | None = None
        self._edit_item: str | None = None
        self._edit_column: str | None = None

        container = ttk.Frame(self, padding=12)
        container.grid(sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)

        ttk.Label(container, text=header).grid(row=0, column=0, sticky="w")

        list_frame = ttk.Labelframe(container, text=list_title, padding=12)
        list_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)

        self.list_view = ttk.Treeview(
            list_frame,
            columns=("name", "value"),
            show="headings",
            selectmode="browse",
            height=10,
        )
        self.list_view.heading("name", text="Item")
        self.list_view.heading("value", text="Value")
        self.list_view.column("name", width=240, anchor="w")
        self.list_view.column("value", width=420, anchor="w")
        self.list_view.grid(row=0, column=0, sticky="nsew")

        list_scroll = ttk.Scrollbar(list_frame, orient="vertical", command=self.list_view.yview)
        list_scroll.grid(row=0, column=1, sticky="ns")
        self.list_view.configure(yscrollcommand=list_scroll.set)

        for name, value in list_items:
            self.list_view.insert("", "end", values=(name, value))

        if editable:
            self.list_view.bind("<Double-1>", self._begin_edit)
            self.list_view.bind("<Return>", self._begin_edit)

        button_frame = ttk.Frame(container)
        button_frame.grid(row=2, column=0, sticky="e", pady=(12, 0))
        for label, command in buttons:
            ttk.Button(button_frame, text=label, command=command).pack(side="right", padx=(6, 0))

        self.grab_set()
        self.wait_visibility()
        self.focus_set()

    def _begin_edit(self, event: tk.Event) -> None:
        region = self.list_view.identify("region", event.x, event.y)
        if region != "cell":
            return
        column = self.list_view.identify_column(event.x)
        if column != "#2":
            return
        row_id = self.list_view.identify_row(event.y)
        if not row_id:
            return
        self._start_edit(row_id, column)

    def _start_edit(self, row_id: str, column: str) -> None:
        self._end_edit(commit=False)
        bbox = self.list_view.bbox(row_id, column)
        if not bbox:
            return
        x, y, width, height = bbox
        value = self.list_view.set(row_id, "value")
        entry = ttk.Entry(self.list_view)
        entry.insert(0, value)
        entry.select_range(0, tk.END)
        entry.place(x=x, y=y, width=width, height=height)
        entry.focus_set()
        entry.bind("<Return>", lambda _event: self._end_edit(commit=True))
        entry.bind("<Escape>", lambda _event: self._end_edit(commit=False))
        entry.bind("<FocusOut>", lambda _event: self._end_edit(commit=True))
        self._edit_entry = entry
        self._edit_item = row_id
        self._edit_column = column

    def _end_edit(self, commit: bool) -> None:
        if not self._edit_entry or not self._edit_item:
            return
        entry = self._edit_entry
        if commit:
            new_value = entry.get().strip()
            self.list_view.set(self._edit_item, "value", new_value)
        entry.destroy()
        self._edit_entry = None
        self._edit_item = None
        self._edit_column = None

    def get_option_values(self, definitions: list[tuple[str, str]]) -> dict[str, str]:
        label_to_key = {label: key for key, label in definitions}
        values: dict[str, str] = {}
        for item in self.list_view.get_children():
            name, value = self.list_view.item(item, "values")
            key = label_to_key.get(name, name)
            values[key] = value
        return values

    def update_list(self, list_items: list[tuple[str, str]]) -> None:
        for item in self.list_view.get_children():
            self.list_view.delete(item)
        for name, value in list_items:
            self.list_view.insert("", "end", values=(name, value))


class SoundDevicePlayObject:
    def __init__(
        self,
        context: object | None,
        finished_event: threading.Event | None = None,
        stop_callback: Callable[[], None] | None = None,
    ) -> None:
        self.context = context
        self.finished_event = finished_event
        self.stop_callback = stop_callback
        self._stopped = False

    def is_playing(self) -> bool:
        if self._stopped:
            return False
        if self.finished_event is not None:
            return not self.finished_event.is_set()
        ctx = self.context
        if ctx is None:
            return False
        status = getattr(ctx, "status", None)
        if callable(status):
            try:
                status_value = status()
                if hasattr(status_value, "active"):
                    return bool(status_value.active)
            except Exception:
                return False
        if hasattr(status, "active"):
            return bool(status.active)
        active_attr = getattr(ctx, "active", None)
        if isinstance(active_attr, bool):
            return active_attr
        is_active = getattr(ctx, "is_active", None)
        if callable(is_active):
            try:
                return bool(is_active())
            except Exception:
                return False
        finished = getattr(ctx, "finished", None)
        if hasattr(finished, "is_set"):
            try:
                return not finished.is_set()
            except Exception:
                return False
        return not self._stopped

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self.stop_callback:
            try:
                self.stop_callback()
            except Exception:
                return
        ctx = self.context
        stop_method = getattr(ctx, "stop", None)
        if callable(stop_method):
            try:
                stop_method()
            except Exception:
                return
        close_method = getattr(ctx, "close", None)
        if callable(close_method):
            try:
                close_method()
            except Exception:
                return
        if self.finished_event is not None:
            self.finished_event.set()


class MediaServerApp:
    def __init__(self, root: tk.Tk, db: LibraryDB, audio_backend: str = "simpleaudio") -> None:
        self.root = root
        self.db = db
        self.root.title("Media Server Organizer")
        self.root.geometry("1200x720")
        self.style = ttk.Style(self.root)
        self.themes = self._load_themes()
        self.current_theme = dict(self.themes.get("Default", DEFAULT_THEME))

        self.playlist_dir = Path(__file__).resolve().parent.parent / "playlists"
        self.playlist_manager = PlaylistManager(self.playlist_dir)
        self.playlists_visible = True
        self.current_playlist_name = tk.StringVar(value="")
        self.playlist_items: ttk.Treeview | None = None
        self.playlist_listbox: tk.Listbox | None = None
        self.playlist_toggle_button: ttk.Button | None = None

        self.library_tabs: dict[int, ttk.Frame] = {}
        self.library_views: dict[int, ttk.Treeview] = {}
        self.library_paths: dict[int, str] = {}
        self.current_library: Library | None = None
        self.audio_segment: AudioSegment | None = None
        self.audio_segment_path: str | None = None
        self.audio_play_obj: object | SoundDevicePlayObject | None = None
        self.audio_backend = resolve_audio_backend(audio_backend, emit_message=logger.warning)
        self.audio_path: str | None = None
        self.audio_paused_position_ms = 0
        self.audio_playback_start_time: float | None = None
        self.audio_progress_job: str | None = None
        self.audio_is_paused = False
        self.audio_title_var = tk.StringVar(value="No audio loaded")
        self.audio_time_var = tk.StringVar(value="00:00 / 00:00")
        self.audio_volume = tk.DoubleVar(value=100.0)
        self.search_var = tk.StringVar()
        self.search_status_var = tk.StringVar(value="Enter a term to search all libraries.")
        self.watch_enabled_var = tk.BooleanVar(value=False)
        self.watch_debounce_var = tk.StringVar(value="2.0")
        self.watch_poll_max_hz_var = tk.StringVar(value="1.0")
        self.watch_extensions_var = tk.StringVar(value=",".join(sorted(AUDIO_EXTENSIONS)))
        self.watch_status_var = tk.StringVar(value="Watch mode disabled.")
        self.indexed_libraries: set[int] = set()
        self.watch_history: list[WatchRunHistoryEntry] = []
        self.watch_history_list: ttk.Treeview | None = None
        self.watch_task_queue = EventCoalescingQueue(
            debounce_seconds=self._parse_debounce_seconds(),
            extensions=self._parse_watch_extensions(),
        )
        self.watch_processor_job: str | None = None
        # The polling watcher runs on a background thread and must not touch the
        # main-thread SQLite connection (sqlite3 forbids cross-thread use). It
        # reads library roots through this cache, which is refreshed only on the
        # main thread (watch start + each _process_watch_queue tick).
        self._watch_roots_lock = threading.Lock()
        self._watch_roots_cache: list[str] = []
        self.library_watcher = PollingLibraryWatcher(
            get_library_roots=self._watch_library_roots,
            on_paths_detected=self._enqueue_watch_paths,
            on_status=self._set_watch_status,
        )
        self.library_context_menu = tk.Menu(self.root, tearoff=0)
        self.library_context_menu.add_command(
            label="Play Video", command=self._play_selected_library_video
        )
        self.library_context_menu.add_command(
            label="Play Audio", command=self._play_selected_library_audio
        )
        self.library_context_menu.add_command(label="Edit Tags…", command=self._bulk_edit_selected_tags)
        self.library_context_menu.add_separator()
        self.library_context_menu.add_command(label="New Playlist...", command=self._prompt_new_playlist)
        self.library_playlist_submenu = tk.Menu(self.library_context_menu, tearoff=0)
        self.library_context_menu.add_cascade(
            label="Add to Playlist", menu=self.library_playlist_submenu
        )
        self.search_context_menu = tk.Menu(self.root, tearoff=0)
        self.search_context_menu.add_command(label="Edit Tags…", command=self._bulk_edit_selected_tags)

        self._build_menu()
        self._build_layout()
        self._apply_theme(self.current_theme)
        self._load_libraries()
        self._update_playlist_menus()
        self.root.protocol("WM_DELETE_WINDOW", self._on_app_close)
        logger.info("MediaServerApp initialized with %s libraries", len(self.db.fetch_libraries()))

    def _build_menu(self) -> None:
        menu = tk.Menu(self.root)
        file_menu = tk.Menu(menu, tearoff=0)
        file_menu.add_command(label="New Library...", command=self._open_new_library_dialog)
        file_menu.add_command(label="Open Library Location", command=self._open_current_library_location)
        file_menu.add_command(label="Reload Library View", command=self._refresh_current_library)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self._on_app_close)
        menu.add_cascade(label="File", menu=file_menu)

        view_menu = tk.Menu(menu, tearoff=0)
        view_menu.add_command(label="Workflows...", command=self._open_workflows_dialog)
        view_menu.add_command(label="Run History", command=self._open_watch_history_panel)
        view_menu.add_separator()
        view_menu.add_command(label="Refresh Library", command=self._refresh_current_library)
        view_menu.add_command(label="Refresh Folder Tree", command=self._refresh_folder_tree)
        view_menu.add_separator()
        view_menu.add_command(label="Expand All Folders", command=lambda: self._set_folder_tree_expanded(True))
        view_menu.add_command(label="Collapse All Folders", command=lambda: self._set_folder_tree_expanded(False))
        menu.add_cascade(label="View", menu=view_menu)

        options_menu = tk.Menu(menu, tearoff=0)
        options_menu.add_command(
            label="Library Management", command=self._open_library_management_dialog
        )
        options_menu.add_command(label="Theme", command=self._open_theme_dialog)
        options_menu.add_command(label="Export", command=self._export_library_index)
        menu.add_cascade(label="Options", menu=options_menu)

        help_menu = tk.Menu(menu, tearoff=0)
        help_menu.add_command(label="About", command=self._show_about)
        menu.add_cascade(label="Help", menu=help_menu)
        self.root.config(menu=menu)

    def _load_themes(self) -> dict[str, dict[str, str]]:
        themes: dict[str, dict[str, str]] = {}
        themes_dir = Path(__file__).resolve().parent / "themes"
        if themes_dir.is_dir():
            for theme_path in sorted(themes_dir.glob("*.json")):
                try:
                    data = json.loads(theme_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                if not isinstance(data, dict):
                    continue
                theme_name = theme_path.stem.replace("_", " ").title()
                themes[theme_name] = {key: str(value) for key, value in data.items()}
        if "Default" not in themes:
            themes["Default"] = dict(DEFAULT_THEME)
        if "Default" in themes:
            themes = {"Default": themes.pop("Default"), **themes}
        return themes

    def _build_layout(self) -> None:
        main_pane = ttk.PanedWindow(self.root, orient="horizontal")
        main_pane.pack(fill="both", expand=True)

        self.left_frame = ttk.Frame(main_pane, width=280, style="Sidebar.TFrame")
        self.left_frame.columnconfigure(0, weight=1)
        self.left_frame.rowconfigure(0, weight=1)
        self.left_frame.rowconfigure(1, weight=0)

        self.folder_tree = ttk.Treeview(self.left_frame, show="tree", style="Sidebar.Treeview")
        folder_scroll = ttk.Scrollbar(
            self.left_frame, orient="vertical", command=self.folder_tree.yview
        )
        self.folder_tree.configure(yscrollcommand=folder_scroll.set)
        self.folder_tree.grid(row=0, column=0, sticky="nsew")
        folder_scroll.grid(row=0, column=1, sticky="ns")
        self.folder_tree.bind("<<TreeviewOpen>>", self._expand_folder_node)
        self.folder_tree.bind("<<TreeviewSelect>>", self._on_folder_tree_selected)
        self.folder_tree.bind("<Double-1>", self._on_folder_tree_double_click)
        self.folder_tree.bind("<Button-3>", self._show_folder_tree_menu)
        self.folder_tree_menu = tk.Menu(self.folder_tree, tearoff=0)
        self.folder_tree_menu.add_command(
            label="Play Video", command=self._play_selected_folder_video
        )
        self.folder_tree_menu.add_command(
            label="Play Audio", command=self._play_selected_folder_audio
        )
        self.folder_tree_menu.add_separator()
        self.folder_tree_menu.add_command(label="New Playlist...", command=self._prompt_new_playlist)
        self.folder_playlist_submenu = tk.Menu(self.folder_tree_menu, tearoff=0)
        self.folder_tree_menu.add_cascade(
            label="Add to Playlist", menu=self.folder_playlist_submenu
        )

        self.metadata_frame = ttk.Labelframe(
            self.left_frame, text="File Metadata", padding=8, style="Metadata.TLabelframe"
        )
        self.metadata_frame.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
        self.metadata_frame.columnconfigure(0, weight=1)
        self.metadata_frame.rowconfigure(0, weight=1)

        self.metadata_canvas = tk.Canvas(self.metadata_frame, highlightthickness=0)
        self.metadata_scroll = ttk.Scrollbar(
            self.metadata_frame, orient="vertical", command=self.metadata_canvas.yview
        )
        self.metadata_canvas.configure(yscrollcommand=self.metadata_scroll.set)
        self.metadata_canvas.grid(row=0, column=0, sticky="nsew")
        self.metadata_scroll.grid(row=0, column=1, sticky="ns")

        self.metadata_content = ttk.Frame(self.metadata_canvas, style="Metadata.TFrame")
        self.metadata_content.columnconfigure(1, weight=1)
        self.metadata_window = self.metadata_canvas.create_window(
            (0, 0), window=self.metadata_content, anchor="nw"
        )
        self.metadata_content.bind(
            "<Configure>", lambda event: self.metadata_canvas.configure(scrollregion=self.metadata_canvas.bbox("all"))
        )
        self.metadata_canvas.bind(
            "<Configure>", lambda event: self.metadata_canvas.itemconfig(self.metadata_window, width=event.width)
        )

        self.content_pane = ttk.PanedWindow(main_pane, orient="horizontal")

        self.right_frame = ttk.Frame(self.content_pane)
        self.right_frame.columnconfigure(0, weight=1)
        self.right_frame.rowconfigure(1, weight=2)
        self.right_frame.rowconfigure(2, weight=1)
        self.right_frame.rowconfigure(3, weight=0)

        self._build_toolbar()

        self.notebook = ttk.Notebook(self.right_frame)
        self.notebook.grid(row=1, column=0, sticky="nsew")
        self.notebook.bind("<<NotebookTabChanged>>", self._handle_tab_changed)

        self.new_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.new_tab, text="+")

        self._build_search_results()
        self._build_audio_player()

        self.playlist_frame = ttk.Frame(self.content_pane, width=300, padding=(8, 6))
        self.playlist_frame.columnconfigure(0, weight=1)
        self.playlist_frame.rowconfigure(1, weight=1)
        self._build_playlist_pane()

        main_pane.add(self.left_frame, weight=1)
        main_pane.add(self.content_pane, weight=4)
        self.content_pane.add(self.right_frame, weight=3)
        self.content_pane.add(self.playlist_frame, weight=1)

    def _build_toolbar(self) -> None:
        toolbar = ttk.Frame(self.right_frame, padding=(8, 6))
        toolbar.grid(row=0, column=0, sticky="ew")
        toolbar.columnconfigure(1, weight=1)

        ttk.Label(toolbar, text="Search").grid(row=0, column=0, sticky="w", padx=(0, 6))
        search_entry = ttk.Entry(toolbar, textvariable=self.search_var)
        search_entry.grid(row=0, column=1, sticky="ew")
        search_entry.bind("<Return>", lambda _event: self._perform_search())
        ttk.Button(toolbar, text="Go", command=self._perform_search).grid(
            row=0, column=2, padx=(6, 0)
        )
        ttk.Button(toolbar, text="Playlists Pane", command=self._toggle_playlist_pane).grid(
            row=0, column=3, padx=(6, 0)
        )

        self.search_status = ttk.Label(toolbar, textvariable=self.search_status_var, anchor="w")
        self.search_status.grid(row=1, column=0, columnspan=4, sticky="ew", pady=(6, 0))

        watch_row = ttk.Frame(toolbar)
        watch_row.grid(row=2, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        watch_row.columnconfigure(9, weight=1)
        ttk.Checkbutton(
            watch_row,
            text="Watch library",
            variable=self.watch_enabled_var,
            command=self._toggle_watch_mode,
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(watch_row, text="Debounce (s)").grid(row=0, column=1, sticky="w", padx=(10, 4))
        ttk.Entry(watch_row, textvariable=self.watch_debounce_var, width=6).grid(row=0, column=2, sticky="w")
        ttk.Label(watch_row, text="Extensions").grid(row=0, column=3, sticky="w", padx=(10, 4))
        ttk.Entry(watch_row, textvariable=self.watch_extensions_var, width=24).grid(row=0, column=4, sticky="ew")
        ttk.Label(watch_row, text="Max scans/s").grid(row=0, column=5, sticky="w", padx=(10, 4))
        ttk.Entry(watch_row, textvariable=self.watch_poll_max_hz_var, width=6).grid(row=0, column=6, sticky="w")
        ttk.Button(watch_row, text="Apply", command=self._apply_watch_settings).grid(
            row=0, column=7, sticky="w", padx=(6, 0)
        )
        ttk.Button(watch_row, text="Run History", command=self._open_watch_history_panel).grid(
            row=0, column=8, sticky="w", padx=(6, 0)
        )
        ttk.Label(watch_row, textvariable=self.watch_status_var).grid(
            row=0, column=9, sticky="e", padx=(8, 0)
        )

    def _build_search_results(self) -> None:
        frame = ttk.Labelframe(self.right_frame, text="Search Results", padding=6)
        frame.grid(row=2, column=0, sticky="nsew")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        self.search_results = ttk.Treeview(
            frame,
            columns=("library", "type", "path"),
            show="headings",
            height=6,
            selectmode="extended",
        )
        self.search_results.heading("library", text="Library")
        self.search_results.heading("type", text="Type")
        self.search_results.heading("path", text="Path")
        self.search_results.column("library", width=140, anchor="w")
        self.search_results.column("type", width=80, anchor="w")
        self.search_results.column("path", width=420, anchor="w")
        self.search_results.grid(row=0, column=0, sticky="nsew")

        scroll = ttk.Scrollbar(frame, orient="vertical", command=self.search_results.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        self.search_results.configure(yscrollcommand=scroll.set)
        self.search_results.bind("<Double-1>", self._open_search_result)
        self.search_results.bind("<Button-3>", self._show_search_item_menu)

    def _build_audio_player(self) -> None:
        self.audio_player_frame = ttk.Frame(self.right_frame, padding=(8, 6))
        self.audio_player_frame.grid(row=3, column=0, sticky="ew")
        self.audio_player_frame.columnconfigure(1, weight=1)

        ttk.Label(self.audio_player_frame, text="Now Playing:", style="Metadata.TLabel").grid(
            row=0, column=0, sticky="w", padx=(0, 8)
        )
        ttk.Label(self.audio_player_frame, textvariable=self.audio_title_var, style="Metadata.TLabel").grid(
            row=0, column=1, sticky="w"
        )

        self.audio_progress = ttk.Progressbar(
            self.audio_player_frame, mode="determinate", length=260, maximum=100
        )
        self.audio_progress.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(6, 2))
        self.audio_time_label = ttk.Label(
            self.audio_player_frame, textvariable=self.audio_time_var, style="Metadata.TLabel"
        )
        self.audio_time_label.grid(row=1, column=2, sticky="e", padx=(8, 0))

        controls = ttk.Frame(self.audio_player_frame)
        controls.grid(row=2, column=0, columnspan=3, sticky="w", pady=(6, 0))
        ttk.Button(controls, text="Play", command=self._resume_or_restart_audio).pack(side="left")
        ttk.Button(controls, text="Pause", command=self._pause_audio).pack(side="left", padx=(6, 0))
        ttk.Button(controls, text="Stop", command=self._stop_audio).pack(side="left", padx=(6, 0))

        volume_frame = ttk.Frame(self.audio_player_frame)
        volume_frame.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(6, 0))
        ttk.Label(volume_frame, text="Volume", style="Metadata.TLabel").pack(side="left")
        ttk.Scale(
            volume_frame,
            from_=0,
            to=100,
            orient="horizontal",
            variable=self.audio_volume,
            length=160,
        ).pack(side="left", padx=(6, 0), fill="x", expand=True)
        ttk.Label(volume_frame, text="%", style="Metadata.TLabel").pack(side="left", padx=(4, 0))
        self._toggle_audio_player(False)

    def _build_playlist_pane(self) -> None:
        header = ttk.Frame(self.playlist_frame)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="Playlists", style="Metadata.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Button(header, text="New", command=self._prompt_new_playlist).grid(
            row=0, column=1, padx=(6, 0)
        )
        self.playlist_toggle_button = ttk.Button(header, text="Hide", command=self._toggle_playlist_pane)
        self.playlist_toggle_button.grid(
            row=0, column=2, padx=(6, 0)
        )

        list_container = ttk.Labelframe(self.playlist_frame, text="Available Playlists", padding=6)
        list_container.grid(row=1, column=0, sticky="nsew")
        list_container.columnconfigure(0, weight=1)
        list_container.rowconfigure(0, weight=1)
        self.playlist_listbox = tk.Listbox(list_container, exportselection=False, height=6)
        playlist_scroll = ttk.Scrollbar(
            list_container, orient="vertical", command=self.playlist_listbox.yview
        )
        self.playlist_listbox.configure(yscrollcommand=playlist_scroll.set)
        self.playlist_listbox.grid(row=0, column=0, sticky="nsew")
        playlist_scroll.grid(row=0, column=1, sticky="ns")
        self.playlist_listbox.bind("<<ListboxSelect>>", self._on_playlist_selected)
        self.playlist_listbox.bind("<Button-3>", self._show_playlist_menu)
        self.playlist_menu = tk.Menu(self.playlist_listbox, tearoff=0)
        self.playlist_menu.add_command(label="Rename...", command=self._rename_selected_playlist)
        self.playlist_menu.add_command(label="Delete", command=self._delete_selected_playlist)

        items_container = ttk.Labelframe(self.playlist_frame, text="Playlist Items", padding=6)
        items_container.grid(row=2, column=0, sticky="nsew", pady=(8, 0))
        items_container.columnconfigure(0, weight=1)
        items_container.rowconfigure(0, weight=1)
        self.playlist_items = ttk.Treeview(
            items_container,
            columns=("name", "path"),
            show="headings",
            height=8,
        )
        self.playlist_items.heading("name", text="Name")
        self.playlist_items.heading("path", text="Path")
        self.playlist_items.column("name", width=120, anchor="w")
        self.playlist_items.column("path", width=260, anchor="w")
        self.playlist_items.grid(row=0, column=0, sticky="nsew")
        item_scroll = ttk.Scrollbar(items_container, orient="vertical", command=self.playlist_items.yview)
        item_scroll.grid(row=0, column=1, sticky="ns")
        self.playlist_items.configure(yscrollcommand=item_scroll.set)
        self.playlist_items.bind("<Double-1>", self._open_playlist_item)

        item_controls = ttk.Frame(items_container)
        item_controls.grid(row=1, column=0, columnspan=2, sticky="e", pady=(6, 0))
        ttk.Button(item_controls, text="Remove Selected", command=self._remove_selected_playlist_item).pack(
            side="right"
        )

        self.playlist_frame.rowconfigure(2, weight=1)
        self._refresh_playlist_list()

    def _toggle_playlist_pane(self) -> None:
        if self.playlists_visible:
            self.content_pane.forget(self.playlist_frame)
            self.playlists_visible = False
            if self.playlist_toggle_button:
                self.playlist_toggle_button.configure(text="Show")
        else:
            self.content_pane.add(self.playlist_frame, weight=1)
            self.playlists_visible = True
            if self.playlist_toggle_button:
                self.playlist_toggle_button.configure(text="Hide")

    def _prompt_new_playlist(self) -> None:
        name = simpledialog.askstring("New Playlist", "Playlist name:", parent=self.root)
        if not name:
            return
        try:
            created = self.playlist_manager.create_playlist(name)
        except ValueError as exc:
            messagebox.showerror("New Playlist", str(exc))
            return
        self.current_playlist_name.set(created)
        self._refresh_playlist_list()
        self._refresh_playlist_items()
        self._update_playlist_menus()

    def _refresh_playlist_list(self) -> None:
        if not self.playlist_listbox:
            return
        self.playlist_listbox.delete(0, tk.END)
        current = self.current_playlist_name.get()
        for name in sorted(self.playlist_manager.playlists):
            self.playlist_listbox.insert(tk.END, name)
        if current and current in self.playlist_manager.playlists:
            index = sorted(self.playlist_manager.playlists).index(current)
            self.playlist_listbox.selection_set(index)
        elif self.playlist_manager.playlists:
            self.playlist_listbox.selection_set(0)
            self.current_playlist_name.set(sorted(self.playlist_manager.playlists)[0])
        else:
            self.current_playlist_name.set("")
        self._refresh_playlist_items()

    def _on_playlist_selected(self, _event: tk.Event) -> None:
        if not self.playlist_listbox:
            return
        selection = self.playlist_listbox.curselection()
        if not selection:
            self.current_playlist_name.set("")
            self._refresh_playlist_items()
            return
        index = selection[0]
        names = sorted(self.playlist_manager.playlists)
        if 0 <= index < len(names):
            self.current_playlist_name.set(names[index])
        self._refresh_playlist_items()

    def _show_playlist_menu(self, event: tk.Event) -> None:
        if not self.playlist_listbox:
            return
        index = self.playlist_listbox.nearest(event.y)
        if index < 0 or index >= self.playlist_listbox.size():
            return
        self.playlist_listbox.selection_clear(0, tk.END)
        self.playlist_listbox.selection_set(index)
        self._on_playlist_selected(event)
        try:
            self.playlist_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.playlist_menu.grab_release()

    def _rename_selected_playlist(self) -> None:
        name = self.current_playlist_name.get()
        if not name:
            return
        new_name = simpledialog.askstring(
            "Rename Playlist", "New playlist name:", initialvalue=name, parent=self.root
        )
        if not new_name or new_name == name:
            return
        try:
            renamed = self.playlist_manager.rename_playlist(name, new_name)
        except ValueError as exc:
            messagebox.showerror("Rename Playlist", str(exc))
            return
        self.current_playlist_name.set(renamed)
        self._refresh_playlist_list()
        self._update_playlist_menus()

    def _delete_selected_playlist(self) -> None:
        name = self.current_playlist_name.get()
        if not name:
            return
        if not messagebox.askyesno(
            "Delete Playlist",
            f"Delete playlist '{name}'? The media files themselves are not removed.",
            parent=self.root,
        ):
            return
        self.playlist_manager.delete_playlist(name)
        self.current_playlist_name.set("")
        self._refresh_playlist_list()
        self._update_playlist_menus()

    def _refresh_playlist_items(self) -> None:
        if not self.playlist_items:
            return
        for item in self.playlist_items.get_children():
            self.playlist_items.delete(item)
        playlist_name = self.current_playlist_name.get()
        entries = self.playlist_manager.playlists.get(playlist_name, [])
        for path in entries:
            self.playlist_items.insert("", "end", values=(Path(path).name, path))

    def _open_playlist_item(self, _event: tk.Event) -> None:
        selection = self.playlist_items.selection() if self.playlist_items else ()
        if not selection:
            return
        values = self.playlist_items.item(selection[0], "values")
        if len(values) < 2:
            return
        path = values[1]
        self._handle_media_activation(path)

    def _remove_selected_playlist_item(self) -> None:
        if not self.playlist_items:
            return
        selection = self.playlist_items.selection()
        if not selection:
            return
        playlist_name = self.current_playlist_name.get()
        if not playlist_name:
            return
        removed_any = False
        for item_id in selection:
            values = self.playlist_items.item(item_id, "values")
            if len(values) < 2:
                continue
            path = values[1]
            self.playlist_manager.remove_item(playlist_name, path)
            removed_any = True
        if removed_any:
            self._refresh_playlist_items()
            self._update_playlist_menus()

    def _add_path_to_playlist(self, playlist_name: str, path: str) -> None:
        if not os.path.isfile(path):
            messagebox.showerror("Add to Playlist", "Selected file is unavailable.")
            return
        extension = os.path.splitext(path)[1].lower()
        if not self._is_media_file(extension):
            messagebox.showinfo("Add to Playlist", "Only audio or video files can be added.")
            return
        try:
            self.playlist_manager.add_item(playlist_name, path)
        except ValueError as exc:
            messagebox.showerror("Add to Playlist", str(exc))
            return
        if self.current_playlist_name.get() == playlist_name:
            self._refresh_playlist_items()
        self._update_playlist_menus()

    def _update_playlist_menus(self) -> None:
        self._rebuild_add_to_playlist_menu(
            self.folder_playlist_submenu, self._get_selected_folder_path()
        )
        entry = self._get_selected_library_item()
        path = entry[1] if entry else None
        self._rebuild_add_to_playlist_menu(self.library_playlist_submenu, path)

    def _load_libraries(self) -> None:
        for library in self.db.fetch_libraries():
            self._create_library_tab(library)
        if self.library_tabs:
            first_library = next(iter(self.library_tabs))
            self.notebook.select(self.library_tabs[first_library])
            self._set_current_library(self.db.fetch_libraries()[0])
        self._refresh_search_index(force=False)

    def _open_new_library_dialog(self) -> None:
        dialog = NewLibraryDialog(self.root)
        self.root.wait_window(dialog)
        if dialog.result:
            library = self.db.add_library(
                name=dialog.result["name"],
                library_type=dialog.result["library_type"],
                path=dialog.result["path"],
                host=dialog.result["host"],
                username=dialog.result["username"],
            )
            self._create_library_tab(library)
            self.notebook.select(self.library_tabs[library.library_id])
            self._set_current_library(library)

    def _open_library_management_dialog(self) -> None:
        dialog = LibraryManagementDialog(self.root, self.db)
        self.root.wait_window(dialog)
        self._sync_libraries()

    def _open_workflows_dialog(self) -> None:
        dialog = WorkflowsDialog(self.root)
        self.root.wait_window(dialog)

    def _parse_debounce_seconds(self) -> float:
        try:
            return max(0.0, float(self.watch_debounce_var.get().strip()))
        except (AttributeError, ValueError):
            return 2.0

    def _parse_watch_poll_max_hz(self) -> float:
        try:
            return min(20.0, max(0.1, float(self.watch_poll_max_hz_var.get().strip())))
        except (AttributeError, ValueError):
            return 1.0

    def _parse_watch_extensions(self) -> set[str]:
        raw = self.watch_extensions_var.get().strip() if hasattr(self, "watch_extensions_var") else ""
        values = {item.strip().lower() for item in re.split(r"[,\s]+", raw) if item.strip()}
        return {value if value.startswith(".") else f".{value}" for value in values}

    def _watch_library_roots(self) -> list[str]:
        # Called from the watcher thread; return the main-thread-populated cache
        # instead of querying SQLite (which raises across threads).
        lock = getattr(self, "_watch_roots_lock", None)
        if lock is None:
            return list(getattr(self, "_watch_roots_cache", []))
        with lock:
            return list(self._watch_roots_cache)

    def _refresh_watch_roots_cache(self) -> None:
        # Must run on the main thread (owns the SQLite connection).
        db = getattr(self, "db", None)
        lock = getattr(self, "_watch_roots_lock", None)
        if db is None or lock is None:
            return
        roots = [library.path for library in db.fetch_libraries() if library.path]
        with lock:
            self._watch_roots_cache = roots

    def _apply_watch_settings(self) -> None:
        debounce_seconds = self._parse_debounce_seconds()
        extensions = self._parse_watch_extensions()
        poll_max_hz = self._parse_watch_poll_max_hz()
        self.watch_task_queue.debounce_seconds = debounce_seconds
        self.watch_task_queue.update_extensions(extensions)
        self.library_watcher.configure(max_scan_interval_seconds=1.0 / poll_max_hz)
        self._set_watch_status(
            f"Watch settings updated (debounce: {debounce_seconds:.1f}s, extensions: {len(extensions)}, max scans/s: {poll_max_hz:.1f})."
        )

    def _toggle_watch_mode(self) -> None:
        if self.watch_enabled_var.get():
            self._apply_watch_settings()
            # Populate the roots cache on the main thread before the watcher's
            # first (main-thread) scan and its background polling read it.
            self._refresh_watch_roots_cache()
            self.library_watcher.start()
            self._ensure_watch_processor()
            self._set_watch_status("Watch mode running.")
        else:
            self._stop_watch_mode()

    def _stop_watch_mode(self) -> None:
        self.library_watcher.stop()
        if self.watch_processor_job:
            self.root.after_cancel(self.watch_processor_job)
            self.watch_processor_job = None
        self.watch_enabled_var.set(False)
        self._set_watch_status("Watch mode paused.")

    def _set_watch_status(self, status: str) -> None:
        if threading.current_thread() is threading.main_thread():
            self.watch_status_var.set(status)
            return
        self.root.after(0, lambda: self.watch_status_var.set(status))

    def _on_app_close(self) -> None:
        self._stop_watch_mode()
        self._stop_audio(suppress_errors=True)
        self.root.destroy()

    def _enqueue_watch_paths(self, paths: list[str]) -> None:
        self.watch_task_queue.enqueue(paths)

    def _ensure_watch_processor(self) -> None:
        if self.watch_processor_job is None:
            self.watch_processor_job = self.root.after(500, self._process_watch_queue)

    def _process_watch_queue(self) -> None:
        self.watch_processor_job = None
        # Keep the watcher thread's roots cache current (added/removed libraries)
        # from the main thread, which owns the SQLite connection.
        self._refresh_watch_roots_cache()
        for path in self.watch_task_queue.pop_ready():
            self._run_watch_workflows_for_path(path)
        if self.watch_enabled_var.get():
            self.watch_processor_job = self.root.after(500, self._process_watch_queue)

    def _library_root_for_path(self, path: str) -> str | None:
        try:
            target = Path(path).resolve()
        except OSError:
            target = Path(path)
        best: str | None = None
        for root in self._watch_library_roots():
            if not root:
                continue
            try:
                root_resolved = Path(root).resolve()
            except OSError:
                continue
            if target == root_resolved or root_resolved in target.parents:
                root_str = str(root_resolved)
                # Prefer the deepest (most specific) matching library root.
                if best is None or len(root_str) > len(best):
                    best = root_str
        return best

    def _run_watch_workflows_for_path(self, path: str) -> None:
        parent = str(Path(path).parent)
        # Organize newly detected files within the library they belong to. The
        # cleaner's destination_root default is ~/Music, so without this a file
        # dropped into any watched library would be silently relocated out of it.
        destination_root = self._library_root_for_path(path) or parent
        for workflow_name in ("library_cleaner", "library_dedup"):
            runner = load_workflow_runner(workflow_name)
            if not runner:
                self._append_watch_history("error", f"{workflow_name}: runner unavailable for {path}", "-")
                continue
            options: dict[str, str] = {
                "library_path": parent,
                "destination_root": destination_root,
            }
            for option in getattr(runner, "option_definitions")():
                options.setdefault(option.key, option.value)
            try:
                plan = getattr(runner, "build_plan")(options)
                result = getattr(runner, "apply")(options, plan)
                rollback = str(getattr(result, "rollback_script", "-") or "-")
                self._append_watch_history("success", f"{workflow_name}: processed {path}", rollback)
            except Exception as exc:
                self._append_watch_history("error", f"{workflow_name}: {exc}", "-")

    def _append_watch_history(self, status: str, details: str, rollback: str) -> None:
        self.watch_history.append(
            WatchRunHistoryEntry(
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                status=status,
                details=details,
                rollback=rollback,
            )
        )
        self.watch_history = self.watch_history[-200:]
        self._refresh_watch_history_panel()

    def _open_watch_history_panel(self) -> None:
        panel = tk.Toplevel(self.root)
        panel.title("Watch Run History")
        panel.geometry("860x320")
        frame = ttk.Frame(panel, padding=10)
        frame.pack(fill="both", expand=True)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)
        self.watch_history_list = ttk.Treeview(
            frame,
            columns=("timestamp", "status", "details", "rollback"),
            show="headings",
            height=10,
        )
        for name in ("timestamp", "status", "details", "rollback"):
            self.watch_history_list.heading(name, text=name.title())
        self.watch_history_list.column("timestamp", width=160, anchor="w")
        self.watch_history_list.column("status", width=80, anchor="w")
        self.watch_history_list.column("details", width=360, anchor="w")
        self.watch_history_list.column("rollback", width=230, anchor="w")
        self.watch_history_list.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(frame, orient="vertical", command=self.watch_history_list.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        self.watch_history_list.configure(yscrollcommand=scroll.set)
        self._refresh_watch_history_panel()

    def _refresh_watch_history_panel(self) -> None:
        if not self.watch_history_list:
            return
        for item in self.watch_history_list.get_children():
            self.watch_history_list.delete(item)
        for entry in reversed(self.watch_history):
            self.watch_history_list.insert("", "end", values=(entry.timestamp, entry.status, entry.details, entry.rollback))

    def _create_library_tab(self, library: Library) -> None:
        frame = ttk.Frame(self.notebook)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        tree = ttk.Treeview(
            frame,
            columns=("type", "location"),
            show="headings",
            style="Library.Treeview",
            selectmode="extended",
        )
        tree.heading("type", text="Type")
        tree.heading("location", text="Location")
        tree.column("type", width=120, anchor="w")
        tree.column("location", width=400, anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")

        scroll = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scroll.set)
        scroll.grid(row=0, column=1, sticky="ns")

        tree.bind("<<TreeviewSelect>>", self._on_library_item_selected)
        tree.bind("<Double-1>", self._on_library_item_double_click)
        tree.bind("<Button-3>", self._show_library_item_menu)

        self.library_tabs[library.library_id] = frame
        self.library_views[library.library_id] = tree
        self.notebook.insert(self.new_tab, frame, text=library.name)
        self.library_paths.setdefault(library.library_id, library.path)
        self._populate_library_view(library, self.library_paths[library.library_id])

    def _sync_libraries(self) -> None:
        libraries = self.db.fetch_libraries()
        library_map = {library.library_id: library for library in libraries}

        for library_id in list(self.library_tabs.keys()):
            if library_id not in library_map:
                self.notebook.forget(self.library_tabs[library_id])
                self.library_tabs.pop(library_id, None)
                self.library_views.pop(library_id, None)
                self.library_paths.pop(library_id, None)

        for library in libraries:
            if library.library_id not in self.library_tabs:
                self._create_library_tab(library)
            else:
                self.notebook.tab(self.library_tabs[library.library_id], text=library.name)
                self.library_paths[library.library_id] = library.path

        if self.current_library and self.current_library.library_id in library_map:
            self._set_current_library(library_map[self.current_library.library_id])
        elif libraries:
            self._set_current_library(libraries[0])
            self.notebook.select(self.library_tabs[libraries[0].library_id])
        else:
            self.current_library = None
            self._clear_metadata()
            self._refresh_folder_tree()

    def _populate_library_view(self, library: Library, path: str) -> None:
        tree = self.library_views[library.library_id]
        for item in tree.get_children():
            tree.delete(item)

        if library.library_type == "remote":
            tree.insert("", "end", values=("Remote Library", f"{library.username or 'user'}@{library.host}"))
            tree.insert("", "end", values=("Path", library.path))
            return

        if not os.path.isdir(path):
            tree.insert("", "end", values=("Missing", path))
            logger.warning("Library path missing for '%s': %s", library.name, path)
            return

        try:
            entries = sorted(os.listdir(path))
        except OSError as exc:
            tree.insert("", "end", values=("Error", str(exc)))
            logger.exception("Unable to list directory for library '%s' at %s", library.name, path)
            return

        root_path = os.path.abspath(library.path)
        current_path = os.path.abspath(path)
        if current_path != root_path and current_path.startswith(f"{root_path}{os.sep}"):
            tree.insert("", "end", values=("Up", os.path.dirname(current_path)))

        for entry in entries:
            full_path = os.path.join(path, entry)
            entry_type = "Folder" if os.path.isdir(full_path) else "File"
            tree.insert("", "end", values=(entry_type, full_path))
        logger.debug("Populated library view for '%s' at %s with %s entries", library.name, path, len(entries))
        self._refresh_search_index(force=False)

    def _refresh_search_index(self, force: bool) -> None:
        for library in self.db.fetch_libraries():
            if not force and library.library_id in self.indexed_libraries:
                continue
            try:
                self.db.index_library_items(library)
                self.indexed_libraries.add(library.library_id)
            except Exception as exc:
                self.search_status_var.set(f"Indexing failed for {library.name}: {exc}")
                logger.exception("Indexing failed for library '%s'", library.name)

    def _perform_search(self) -> None:
        term = self.search_var.get().strip()
        for item in self.search_results.get_children():
            self.search_results.delete(item)
        if not term:
            self.search_status_var.set("Enter a term to search all libraries.")
            return
        logger.debug("Starting search for term '%s'", term)
        self.search_status_var.set("Indexing libraries...")
        self.root.update_idletasks()
        self._refresh_search_index(force=False)
        self.search_status_var.set("Searching...")
        try:
            results = self.db.search_items(term)
        except Exception as exc:
            self.search_status_var.set(f"Search failed: {exc}")
            logger.exception("Search failed for term '%s'", term)
            return

        library_lookup = {lib.library_id: lib for lib in self.db.fetch_libraries()}
        for library_id, path, name, entry_type in results:
            library_name = library_lookup.get(
                library_id, Library(0, "Unknown", "local", "", None, None)
            ).name
            self.search_results.insert(
                "",
                "end",
                values=(library_name, entry_type, path),
                tags=(str(library_id),),
            )
        if results:
            self.search_status_var.set(f"Found {len(results)} matches for '{term}'.")
            logger.info("Search for '%s' returned %s results", term, len(results))
        else:
            self.search_status_var.set(f"No results for '{term}'.")
            logger.info("Search for '%s' returned no results", term)

    def _open_search_result(self, _event: tk.Event) -> None:
        selection = self.search_results.selection()
        if not selection:
            return
        item_id = selection[0]
        values = self.search_results.item(item_id, "values")
        tags = self.search_results.item(item_id, "tags")
        if len(values) < 3 or not tags:
            return
        library_id = int(tags[0])
        path = values[2]
        self._navigate_to_search_result(library_id, path)

    def _navigate_to_search_result(self, library_id: int, path: str) -> None:
        library = next((lib for lib in self.db.fetch_libraries() if lib.library_id == library_id), None)
        if not library:
            messagebox.showerror("Search", "Library for this result no longer exists.")
            return
        if library.library_id not in self.library_tabs:
            self._create_library_tab(library)
        self.notebook.select(self.library_tabs[library.library_id])
        self._set_current_library(library)

        if library.library_type != "local":
            return

        target_dir = path if os.path.isdir(path) else os.path.dirname(path)
        if target_dir and os.path.isdir(target_dir):
            self.library_paths[library.library_id] = target_dir
            self._populate_library_view(library, target_dir)
            tree = self.library_views[library.library_id]
            for item in tree.get_children():
                item_values = tree.item(item, "values")
                if len(item_values) >= 2 and os.path.abspath(item_values[1]) == os.path.abspath(path):
                    tree.selection_set(item)
                    tree.see(item)
                    break

    def _handle_tab_changed(self, _event: tk.Event) -> None:
        selected = self.notebook.select()
        if selected == str(self.new_tab):
            self._open_new_library_dialog()
            if self.current_library:
                self.notebook.select(self.library_tabs[self.current_library.library_id])
            return

        for library_id, frame in self.library_tabs.items():
            if str(frame) == selected:
                library = next(
                    (item for item in self.db.fetch_libraries() if item.library_id == library_id),
                    None,
                )
                if library:
                    self._set_current_library(library)
                break

    def _set_current_library(self, library: Library) -> None:
        self.current_library = library
        self.library_paths.setdefault(library.library_id, library.path)
        self._populate_library_view(library, self.library_paths[library.library_id])
        self._refresh_folder_tree()
        self._clear_metadata()
        self._refresh_search_index(force=False)

    def _refresh_current_library(self) -> None:
        if not self.current_library:
            return
        current_path = self.library_paths.get(
            self.current_library.library_id, self.current_library.path
        )
        self._populate_library_view(self.current_library, current_path)
        self._refresh_folder_tree()
        self._refresh_search_index(force=True)

    def _refresh_folder_tree(self) -> None:
        for item in self.folder_tree.get_children():
            self.folder_tree.delete(item)

        if not self.current_library:
            return
        if self.current_library.library_type != "local":
            self.folder_tree.insert("", "end", text="Remote libraries unavailable for browsing")
            return

        root_path = self.current_library.path
        root_node = self.folder_tree.insert("", "end", text=root_path, open=True, values=(root_path,))
        self._populate_folder_children(root_node, root_path)

    def _populate_folder_children(self, parent_id: str, path: str) -> None:
        try:
            entries = sorted(os.listdir(path))
        except OSError:
            return
        for entry in entries:
            full_path = os.path.join(path, entry)
            if os.path.isdir(full_path):
                node = self.folder_tree.insert(parent_id, "end", text=entry, values=(full_path,))
                self.folder_tree.insert(node, "end", text="loading...")

    def _expand_folder_node(self, event: tk.Event) -> None:
        node_id = self.folder_tree.focus()
        children = self.folder_tree.get_children(node_id)
        if len(children) == 1 and self.folder_tree.item(children[0], "text") == "loading...":
            self.folder_tree.delete(children[0])
            path = self.folder_tree.item(node_id, "values")[0]
            self._populate_folder_children(node_id, path)

    def _on_folder_tree_selected(self, _event: tk.Event) -> None:
        selection = self.folder_tree.selection()
        if not selection:
            return
        node_id = selection[0]
        values = self.folder_tree.item(node_id, "values")
        if not values:
            return
        path = values[0]
        if os.path.isdir(path):
            entry_type = "Folder"
        elif os.path.isfile(path):
            entry_type = "File"
        else:
            entry_type = "Unknown"
        self._set_metadata_rows(self._gather_metadata(path, entry_type))

    def _on_folder_tree_double_click(self, _event: tk.Event) -> None:
        selection = self.folder_tree.selection()
        if not selection:
            return
        node_id = selection[0]
        values = self.folder_tree.item(node_id, "values")
        if not values:
            return
        path = values[0]
        if os.path.isdir(path):
            self._navigate_to_path(path)
        elif os.path.isfile(path):
            self._handle_media_activation(path)

    def _on_library_item_selected(self, _event: tk.Event) -> None:
        if not self.current_library:
            return
        tree = self.library_views[self.current_library.library_id]
        selection = tree.selection()
        if not selection:
            return
        if len(selection) == 1:
            item = tree.item(selection[0])
            entry_type, location = item["values"]
            self._set_metadata_rows(self._gather_metadata(location, entry_type))
            return
        file_count = len(self._selected_library_file_paths())
        self._set_metadata_rows(
            [
                ("Selection", f"{len(selection)} items"),
                ("Files", str(file_count)),
                ("Folders", str(len(selection) - file_count)),
            ]
        )

    def _on_library_item_double_click(self, _event: tk.Event) -> None:
        if not self.current_library or self.current_library.library_type != "local":
            return
        tree = self.library_views[self.current_library.library_id]
        selection = tree.selection()
        if not selection:
            return
        item = tree.item(selection[0])
        entry_type, location = item["values"]
        if entry_type in ("Folder", "Up") and os.path.isdir(location):
            self._navigate_to_path(location)
        elif entry_type == "File" and os.path.isfile(location):
            self._handle_media_activation(location)

    def _navigate_to_path(self, path: str) -> None:
        if not self.current_library or self.current_library.library_type != "local":
            return
        if not os.path.isdir(path):
            logger.warning("Attempted to navigate to non-directory path: %s", path)
            return
        self.library_paths[self.current_library.library_id] = path
        self._populate_library_view(self.current_library, path)
        self._clear_metadata()
        logger.debug("Navigated to path %s for library %s", path, self.current_library.name)

    def _set_folder_tree_expanded(self, expanded: bool) -> None:
        def recurse(node: str) -> None:
            self.folder_tree.item(node, open=expanded)
            for child in self.folder_tree.get_children(node):
                recurse(child)

        for root in self.folder_tree.get_children(""):
            recurse(root)

    def _show_folder_tree_menu(self, event: tk.Event) -> None:
        item = self.folder_tree.identify_row(event.y)
        if item:
            self.folder_tree.selection_set(item)
            self.folder_tree.focus(item)
        self._update_folder_menu_state()
        try:
            self.folder_tree_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.folder_tree_menu.grab_release()

    def _show_library_item_menu(self, event: tk.Event) -> None:
        if not self.current_library:
            return
        tree = self.library_views[self.current_library.library_id]
        item = tree.identify_row(event.y)
        if item:
            tree.selection_set(item)
            tree.focus(item)
        self._update_library_menu_state()
        try:
            self.library_context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.library_context_menu.grab_release()

    def _show_search_item_menu(self, event: tk.Event) -> None:
        item = self.search_results.identify_row(event.y)
        if item:
            selected = set(self.search_results.selection())
            if item not in selected:
                self.search_results.selection_set(item)
            self.search_results.focus(item)
        try:
            self.search_context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.search_context_menu.grab_release()

    def _update_folder_menu_state(self) -> None:
        path = self._get_selected_folder_path()
        self._set_menu_state(self.folder_tree_menu, path)
        self._rebuild_add_to_playlist_menu(self.folder_playlist_submenu, path)

    def _update_library_menu_state(self) -> None:
        entry = self._get_selected_library_item()
        path = entry[1] if entry else None
        self._set_menu_state(self.library_context_menu, path)
        self._rebuild_add_to_playlist_menu(self.library_playlist_submenu, path)

    def _set_menu_state(self, menu: tk.Menu, path: str | None) -> None:
        extension = os.path.splitext(path)[1].lower() if path else ""
        menu.entryconfigure(
            "Play Video", state="normal" if self._is_video_file(extension) else "disabled"
        )
        menu.entryconfigure(
            "Play Audio", state="normal" if self._is_audio_file(extension) else "disabled"
        )

    def _rebuild_add_to_playlist_menu(self, submenu: tk.Menu, path: str | None) -> None:
        submenu.delete(0, "end")
        submenu.add_command(label="New Playlist...", command=self._prompt_new_playlist)
        submenu.add_separator()
        if not path or not os.path.isfile(path):
            submenu.add_command(label="Select a media file to add", state="disabled")
            return
        extension = os.path.splitext(path)[1].lower()
        if not self._is_media_file(extension):
            submenu.add_command(label="Only audio or video files can be added", state="disabled")
            return
        if not self.playlist_manager.playlists:
            submenu.add_command(label="No playlists yet", state="disabled")
            return
        for name in sorted(self.playlist_manager.playlists):
            submenu.add_command(label=name, command=lambda n=name, p=path: self._add_path_to_playlist(n, p))

    def _get_selected_folder_path(self) -> str | None:
        selection = self.folder_tree.selection()
        if not selection:
            return None
        values = self.folder_tree.item(selection[0], "values")
        if not values:
            return None
        return values[0]

    def _get_selected_library_item(self) -> tuple[str, str] | None:
        if not self.current_library:
            return None
        tree = self.library_views[self.current_library.library_id]
        selection = tree.selection()
        if not selection:
            return None
        entry_type, location = tree.item(selection[0], "values")
        return entry_type, location

    def _selected_library_file_paths(self) -> list[str]:
        if not self.current_library:
            return []
        tree = self.library_views[self.current_library.library_id]
        paths: list[str] = []
        for item_id in tree.selection():
            values = tree.item(item_id, "values")
            if len(values) < 2:
                continue
            entry_type, location = values
            if entry_type == "File" and os.path.isfile(location):
                paths.append(location)
        return paths

    def _selected_search_file_paths(self) -> list[str]:
        paths: list[str] = []
        for item_id in self.search_results.selection():
            values = self.search_results.item(item_id, "values")
            if len(values) < 3:
                continue
            path = values[2]
            if os.path.isfile(path):
                paths.append(path)
        return paths

    def _bulk_edit_selected_tags(self) -> None:
        unique_paths = {
            *self._selected_library_file_paths(),
            *self._selected_search_file_paths(),
        }
        files = [Path(path) for path in sorted(unique_paths) if self._is_audio_file(Path(path).suffix.lower())]
        if not files:
            messagebox.showinfo("Edit Tags", "Select one or more audio files in library or search views.")
            return

        artist = simpledialog.askstring("Edit Tags", "Artist (blank keeps existing):", parent=self.root)
        if artist is None:
            return
        album = simpledialog.askstring("Edit Tags", "Album (blank keeps existing):", parent=self.root)
        if album is None:
            return
        title = simpledialog.askstring("Edit Tags", "Title (blank keeps existing):", parent=self.root)
        if title is None:
            return
        track = simpledialog.askstring("Edit Tags", "Track (blank keeps existing):", parent=self.root)
        if track is None:
            return

        updates = TagUpdate(
            artist=(artist or "").strip() or None,
            album=(album or "").strip() or None,
            title=(title or "").strip() or None,
            track=(track or "").strip() or None,
        )
        rename_files = messagebox.askyesno(
            "Edit Tags",
            "Also rename files from updated tags?",
            parent=self.root,
        )

        workflow = TagEditorWorkflow()
        try:
            plan = workflow.build_plan(files, updates, rename_files)
        except Exception as exc:
            messagebox.showerror("Edit Tags", f"Unable to build preview: {exc}")
            return
        if not plan.edits:
            messagebox.showinfo("Edit Tags", "No changes needed for selected files.")
            return

        preview = "\n".join(workflow.preview_items(plan))
        if not messagebox.askyesno("Edit Tags Preview", f"Apply these changes?\n\n{preview}", parent=self.root):
            return

        try:
            result = workflow.apply(plan, files[0].parent)
        except Exception as exc:
            logger.exception("Bulk tag edit failed")
            messagebox.showerror("Edit Tags", f"Update failed: {exc}")
            return

        self._refresh_current_library()
        summary = "\n".join(f"{label}: {value}" for label, value in result.summary_items)
        messagebox.showinfo("Edit Tags", summary)

    def _play_selected_folder_video(self) -> None:
        path = self._get_selected_folder_path()
        if path:
            self._launch_video_file(path)

    def _play_selected_folder_audio(self) -> None:
        path = self._get_selected_folder_path()
        if path:
            self._play_audio_file(path)

    def _play_selected_library_video(self) -> None:
        entry = self._get_selected_library_item()
        if entry:
            _, path = entry
            self._launch_video_file(path)

    def _play_selected_library_audio(self) -> None:
        entry = self._get_selected_library_item()
        if entry:
            _, path = entry
            self._play_audio_file(path)

    def _handle_media_activation(self, path: str) -> None:
        extension = os.path.splitext(path)[1].lower()
        if self._is_video_file(extension):
            self._launch_video_file(path)
        elif self._is_audio_file(extension):
            self._play_audio_file(path)

    def _open_current_library_location(self) -> None:
        if not self.current_library or self.current_library.library_type != "local":
            messagebox.showinfo(
                "Open Library Location", "Select a local library to open its folder."
            )
            return
        path = self.library_paths.get(self.current_library.library_id, self.current_library.path)
        if not os.path.isdir(path):
            messagebox.showerror("Open Library Location", "Library path is unavailable.")
            return
        logger.info("Opening library location: %s", path)
        if sys.platform.startswith("win"):
            os.startfile(path)
        elif sys.platform == "darwin":
            subprocess.run(["open", path], check=False)
        else:
            subprocess.run(["xdg-open", path], check=False)

    def _launch_video_file(self, path: str) -> None:
        if not os.path.isfile(path):
            messagebox.showerror("Play Video", "Selected video file is unavailable.")
            return
        extension = os.path.splitext(path)[1].lower()
        if not self._is_video_file(extension):
            messagebox.showinfo("Play Video", "The selected item is not a video file.")
            return
        try:
            logger.info("Launching video file: %s", path)
            if sys.platform.startswith("win"):
                os.startfile(path)  # type: ignore[attr-defined]
                return
            command = ["open", path] if sys.platform == "darwin" else ["xdg-open", path]
            result = subprocess.run(command, check=False, capture_output=True, text=True)
            if result.returncode != 0:
                error_output = result.stderr.strip() or result.stdout.strip()
                raise RuntimeError(error_output or "Unable to open video with default player.")
        except Exception as exc:
            logger.exception("Could not launch video player for %s", path)
            messagebox.showerror("Play Video", f"Could not launch video player: {exc}")

    def _play_audio_file(self, path: str) -> None:
        if not os.path.isfile(path):
            messagebox.showerror("Play Audio", "Selected audio file is unavailable.")
            return
        extension = os.path.splitext(path)[1].lower()
        if not self._is_audio_file(extension):
            messagebox.showinfo("Play Audio", "The selected item is not an audio file.")
            return
        self._stop_audio()
        self.audio_path = path
        self.audio_title_var.set(Path(path).name)
        self.audio_paused_position_ms = 0
        self.audio_is_paused = False
        logger.info("Starting audio playback: %s", path)
        self._start_audio_playback()

    def _start_audio_playback(self) -> None:
        if not self.audio_path:
            return
        self._cancel_audio_progress_job()
        if self.audio_play_obj:
            self.audio_play_obj.stop()
        if self.audio_segment_path != self.audio_path:
            self.audio_segment = None
        if not self.audio_segment:
            try:
                self.audio_segment = AudioSegment.from_file(self.audio_path)
                self.audio_segment_path = self.audio_path
            except Exception as exc:
                logger.exception("Unable to load audio: %s", self.audio_path)
                messagebox.showerror("Play Audio", f"Unable to load audio: {exc}")
                self.audio_segment = None
                self.audio_segment_path = None
                self._stop_audio()
                return

        start_ms = max(0, min(int(self.audio_paused_position_ms), len(self.audio_segment)))
        segment = self._apply_volume(self.audio_segment[start_ms:])
        if len(segment) == 0:
            messagebox.showerror("Play Audio", "Audio file contains no playable data.")
            return

        try:
            self.audio_play_obj = self._play_segment(segment)
        except Exception as exc:
            logger.exception("Unable to start playback for %s", self.audio_path)
            messagebox.showerror("Play Audio", f"Unable to start playback: {exc}")
            self.audio_play_obj = None
            self._stop_audio()
            return

        self.audio_playback_start_time = time.time()
        self.audio_is_paused = False
        self._update_audio_time_display()
        self._schedule_audio_progress()
        self._toggle_audio_player(True)

    def _play_segment(self, segment: AudioSegment) -> object:
        backend = getattr(self, "audio_backend", "simpleaudio")
        if backend == "sounddevice":
            return self._play_with_sounddevice(segment)
        return self._play_with_simpleaudio(segment)

    def _play_with_simpleaudio(self, segment: AudioSegment) -> object:
        if simpleaudio is None:
            raise RuntimeError(
                "simpleaudio is not installed. Install it or choose the sounddevice backend."
            )
        return simpleaudio.play_buffer(
            segment.raw_data,
            num_channels=segment.channels,
            bytes_per_sample=segment.sample_width,
            sample_rate=segment.frame_rate,
        )

    def _play_with_sounddevice(self, segment: AudioSegment) -> SoundDevicePlayObject:
        try:
            import numpy as np
        except ImportError as exc:
            raise RuntimeError(
                "The sounddevice backend requires numpy. Install it or choose the simpleaudio backend."
            ) from exc
        try:
            import sounddevice as sd
        except ImportError as exc:
            raise RuntimeError(
                "sounddevice is not installed. Install it or choose the simpleaudio backend."
            ) from exc

        if hasattr(segment, "get_array_of_samples"):
            sample_data = segment.get_array_of_samples()  # type: ignore[attr-defined]
        else:
            dtype = self._numpy_dtype(segment.sample_width)
            sample_data = np.frombuffer(segment.raw_data, dtype=dtype)
        data = np.array(sample_data)
        if segment.channels > 1:
            data = data.reshape((-1, segment.channels))
        dtype = self._numpy_dtype(segment.sample_width)
        data = data.astype(dtype, copy=False)
        finished_event = threading.Event()
        wait_fn = getattr(sd, "wait", None)
        stop_fn = getattr(sd, "stop", None)

        def _wait_for_completion() -> None:
            try:
                if callable(wait_fn):
                    wait_fn()
            finally:
                finished_event.set()

        playback = sd.play(data, samplerate=segment.frame_rate, blocking=False)
        if callable(wait_fn):
            threading.Thread(target=_wait_for_completion, daemon=True).start()
        return SoundDevicePlayObject(
            playback, finished_event=finished_event, stop_callback=stop_fn if callable(stop_fn) else None
        )

    @staticmethod
    def _numpy_dtype(sample_width: int) -> str:
        match sample_width:
            case 1:
                return "int8"
            case 2:
                return "int16"
            case 3:
                return "int32"
            case 4:
                return "int32"
            case _:
                raise RuntimeError(f"Unsupported sample width: {sample_width}")

    def _handle_playback_exception(self, exc: Exception) -> None:
        logger.exception("Playback error")
        messagebox.showerror("Play Audio", f"Playback error: {exc}")
        self._stop_audio(suppress_errors=True)

    def _play_object_is_playing(self) -> bool:
        if not self.audio_play_obj:
            return False
        try:
            return self.audio_play_obj.is_playing()
        except Exception as exc:
            self._handle_playback_exception(exc)
            return False

    def _resume_or_restart_audio(self) -> None:
        if not self.audio_path:
            messagebox.showinfo("Play Audio", "Select an audio file to play.")
            return
        if self.audio_is_paused:
            self._start_audio_playback()
        else:
            self.audio_paused_position_ms = 0
            self._start_audio_playback()

    def _pause_audio(self) -> None:
        if not self.audio_play_obj:
            return
        if self.audio_playback_start_time is None:
            return
        try:
            playing = self.audio_play_obj.is_playing()
        except Exception as exc:
            self._handle_playback_exception(exc)
            return
        if playing:
            elapsed = int((time.time() - self.audio_playback_start_time) * 1000)
            self.audio_paused_position_ms = min(
                self.audio_paused_position_ms + elapsed, self._current_audio_duration()
            )
            try:
                self.audio_play_obj.stop()
            except Exception as exc:
                self._handle_playback_exception(exc)
                return
            self.audio_play_obj = None
            self.audio_playback_start_time = None
            self.audio_is_paused = True
            self._update_audio_time_display()
            logger.debug("Paused audio at %sms", self.audio_paused_position_ms)

    def _stop_audio(self, suppress_errors: bool = False) -> None:
        if self.audio_play_obj:
            try:
                self.audio_play_obj.stop()
            except Exception as exc:
                if not suppress_errors:
                    messagebox.showerror("Play Audio", f"Playback error: {exc}")
            self.audio_play_obj = None
        self.audio_playback_start_time = None
        self.audio_paused_position_ms = 0
        self.audio_is_paused = False
        self._cancel_audio_progress_job()
        self._update_audio_time_display()
        self.audio_segment = None
        self.audio_segment_path = None
        self.audio_path = None
        self.audio_title_var.set("No audio loaded")
        self.audio_time_var.set("00:00 / 00:00")
        self.audio_progress.configure(value=0, maximum=1)
        self._toggle_audio_player(False)
        logger.info("Audio playback stopped")

    def _schedule_audio_progress(self) -> None:
        self._cancel_audio_progress_job()
        self.audio_progress_job = self.root.after(250, self._update_audio_progress)

    def _cancel_audio_progress_job(self) -> None:
        if self.audio_progress_job is not None:
            self.root.after_cancel(self.audio_progress_job)
        self.audio_progress_job = None

    def _update_audio_progress(self) -> None:
        self._update_audio_time_display()
        playing = self._play_object_is_playing()
        if playing:
            self._schedule_audio_progress()
            return
        if not self.audio_is_paused:
            self._stop_audio()
        self.audio_progress_job = None

    def _update_audio_time_display(self) -> None:
        duration_ms = self._current_audio_duration()
        elapsed_ms = self._current_audio_position_ms(duration_ms)
        maximum = duration_ms if duration_ms > 0 else 1
        self.audio_progress.configure(maximum=maximum, value=min(elapsed_ms, maximum))
        self.audio_time_var.set(
            f"{self._format_milliseconds(elapsed_ms)} / {self._format_milliseconds(duration_ms)}"
        )

    def _current_audio_position_ms(self, duration_ms: int) -> int:
        elapsed_ms = self.audio_paused_position_ms
        if self.audio_play_obj and self.audio_playback_start_time is not None:
            elapsed_ms += int((time.time() - self.audio_playback_start_time) * 1000)
        return min(elapsed_ms, duration_ms)

    def _current_audio_duration(self) -> int:
        if self.audio_segment:
            return len(self.audio_segment)
        return 0

    def _format_milliseconds(self, value: int) -> str:
        seconds = max(0, int(value // 1000))
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours:d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:02d}:{seconds:02d}"

    def _apply_volume(self, segment: AudioSegment) -> AudioSegment:
        volume = max(0.0, min(self.audio_volume.get(), 100.0))
        if volume <= 0:
            return segment - 120
        gain_db = 20 * math.log10(volume / 100.0)
        return segment + gain_db

    def _toggle_audio_player(self, visible: bool) -> None:
        if visible:
            self.audio_player_frame.grid()
        else:
            self.audio_player_frame.grid_remove()

    def _open_theme_dialog(self) -> None:
        ThemeEditorDialog(
            self.root,
            self.themes,
            apply_theme_callback=self._apply_theme,
            save_theme_callback=self._save_theme_preset,
        )

    def _save_theme_preset(self, name: str, theme: dict[str, str]) -> Path:
        themes_dir = Path(__file__).resolve().parent / "themes"
        themes_dir.mkdir(parents=True, exist_ok=True)
        safe_stem = re.sub(r"[^\w\-]+", "_", name.strip().lower()).strip("_") or "theme"
        theme_path = themes_dir / f"{safe_stem}.json"
        theme_path.write_text(json.dumps(theme, indent=2) + "\n", encoding="utf-8")
        self.themes[name] = dict(theme)
        logger.info("Saved theme '%s' to %s", name, theme_path)
        return theme_path

    def _resolve_color(self, color: str, fallback: str) -> str:
        try:
            self.root.winfo_rgb(color)
        except tk.TclError:
            # Windows-only names like "SystemButtonFace" are not valid on
            # other platforms; fall back so themes stay cross-platform.
            return fallback
        return color

    def _apply_theme(self, theme: dict[str, str]) -> None:
        self.current_theme = dict(theme)
        default_bg = self.root.cget("bg")
        window_bg = self._resolve_color(
            theme.get("window_background", default_bg), default_bg
        )
        sidebar_bg = self._resolve_color(theme.get("sidebar_background", window_bg), window_bg)
        toolbar_bg = self._resolve_color(theme.get("toolbar_background", window_bg), window_bg)
        tree_bg = self._resolve_color(theme.get("treeview_background", "white"), "white")
        metadata_bg = self._resolve_color(theme.get("metadata_background", window_bg), window_bg)
        accent = self._resolve_color(theme.get("accent_color", "#3b74ff"), "#3b74ff")
        text_color = self._resolve_color(theme.get("text_color", "black"), "black")

        self.root.configure(background=window_bg)
        self.style.configure("TFrame", background=window_bg)
        self.style.configure("Sidebar.TFrame", background=sidebar_bg)
        self.style.configure("Metadata.TFrame", background=metadata_bg)
        self.style.configure("TLabel", background=window_bg, foreground=text_color)
        self.style.configure("Sidebar.TLabel", background=sidebar_bg, foreground=text_color)
        self.style.configure("Metadata.TLabel", background=metadata_bg, foreground=text_color)
        self.style.configure("TLabelframe", background=window_bg, foreground=text_color)
        self.style.configure(
            "TLabelframe.Label", background=window_bg, foreground=text_color
        )
        self.style.configure(
            "Metadata.TLabelframe", background=metadata_bg, foreground=text_color
        )
        self.style.configure(
            "Metadata.TLabelframe.Label", background=metadata_bg, foreground=text_color
        )
        self.style.configure(
            "Treeview",
            background=tree_bg,
            fieldbackground=tree_bg,
            foreground=text_color,
        )
        self.style.configure(
            "Treeview.Heading",
            background=toolbar_bg,
            foreground=text_color,
        )
        self.style.configure(
            "Sidebar.Treeview",
            background=tree_bg,
            fieldbackground=tree_bg,
            foreground=text_color,
        )
        self.style.configure(
            "Library.Treeview",
            background=tree_bg,
            fieldbackground=tree_bg,
            foreground=text_color,
        )
        self.style.configure(
            "Accent.TButton",
            background=accent,
            foreground=text_color,
        )

        if hasattr(self, "metadata_canvas"):
            self.metadata_canvas.configure(background=metadata_bg)

    def _show_placeholder(self, title: str) -> None:
        messagebox.showinfo(title, f"{title} options will be available in a future update.")

    def _export_library_index(self) -> None:
        if not self.current_library:
            messagebox.showinfo("Export", "Select a library to export.")
            return
        library = self.current_library
        self._refresh_search_index(force=False)
        items = self.db.fetch_library_items(library.library_id)
        if not items:
            messagebox.showinfo("Export", f"No indexed items found for '{library.name}'.")
            return
        file_path = filedialog.asksaveasfilename(
            parent=self.root,
            title="Export Library Index",
            defaultextension=".csv",
            initialfile=f"{library.name}_index",
            filetypes=[("CSV files", "*.csv"), ("JSON files", "*.json")],
        )
        if not file_path:
            return
        try:
            if file_path.lower().endswith(".json"):
                payload = {
                    "library": library.name,
                    "library_path": library.path,
                    "exported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "items": [
                        {"path": path, "name": name, "type": entry_type}
                        for path, name, entry_type in items
                    ],
                }
                Path(file_path).write_text(
                    json.dumps(payload, indent=2), encoding="utf-8"
                )
            else:
                with open(file_path, "w", newline="", encoding="utf-8") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(["path", "name", "type"])
                    writer.writerows(items)
        except OSError as exc:
            logger.exception("Export failed for library '%s'", library.name)
            messagebox.showerror("Export", f"Unable to write export file: {exc}")
            return
        logger.info("Exported %s items for library '%s' to %s", len(items), library.name, file_path)
        messagebox.showinfo(
            "Export", f"Exported {len(items)} items from '{library.name}' to:\n{file_path}"
        )

    def _show_about(self) -> None:
        messagebox.showinfo(
            "About Media Server Organizer",
            "Media Server Organizer\nManage and explore your media libraries.",
        )

    def _set_metadata_rows(self, rows: list[tuple[str, str]]) -> None:
        for child in self.metadata_content.winfo_children():
            child.destroy()
        if not rows:
            rows = [("Metadata", "—")]
        for index, (label, value) in enumerate(rows):
            ttk.Label(self.metadata_content, text=f"{label}:", style="Metadata.TLabel").grid(
                row=index, column=0, sticky="nw", padx=(0, 8), pady=2
            )
            value_label = ttk.Label(
                self.metadata_content,
                text=value or "—",
                wraplength=240,
                justify="left",
                style="Metadata.TLabel",
            )
            value_label.grid(row=index, column=1, sticky="w", pady=2)

    def _clear_metadata(self) -> None:
        self._set_metadata_rows([("Metadata", "Select a file or folder to view details.")])

    def _gather_metadata(self, path: str, entry_type: str) -> list[tuple[str, str]]:
        rows: list[tuple[str, str]] = [
            ("Title", os.path.basename(path) or path),
            ("Path", path),
            ("Type", entry_type),
        ]

        if not os.path.exists(path):
            rows.append(("Status", "Unavailable"))
            return rows

        if os.path.isdir(path):
            try:
                entries = os.listdir(path)
            except OSError as exc:
                rows.append(("Error", str(exc)))
            else:
                rows.append(("Item Count", str(len(entries))))
            return rows

        try:
            stats = os.stat(path)
        except OSError as exc:
            rows.append(("Error", str(exc)))
            return rows

        rows.extend(
            [
                ("Size", f"{self._format_size(stats.st_size)} ({stats.st_size:,} bytes)"),
                ("Modified", self._format_timestamp(stats.st_mtime)),
                ("Created", self._format_timestamp(stats.st_ctime)),
            ]
        )

        extension = os.path.splitext(path)[1].lower()
        if extension:
            rows.append(("Extension", extension))

        if self._is_media_file(extension):
            rows.extend(self._probe_media(path))

        return rows

    @staticmethod
    def _format_timestamp(timestamp: float) -> str:
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        units = ["B", "KB", "MB", "GB", "TB"]
        size = float(size_bytes)
        for unit in units:
            if size < 1024 or unit == units[-1]:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"

    @staticmethod
    def _audio_extensions() -> set[str]:
        return set(AUDIO_EXTENSIONS)

    @staticmethod
    def _video_extensions() -> set[str]:
        return {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".webm", ".m4v"}

    @classmethod
    def _is_audio_file(cls, extension: str) -> bool:
        return extension in cls._audio_extensions()

    @classmethod
    def _is_video_file(cls, extension: str) -> bool:
        return extension in cls._video_extensions()

    @classmethod
    def _is_media_file(cls, extension: str) -> bool:
        return cls._is_audio_file(extension) or cls._is_video_file(extension)

    def _probe_media(self, path: str) -> list[tuple[str, str]]:
        if not shutil.which("ffprobe"):
            return [("Media Info", "ffprobe not available on this system.")]

        try:
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-print_format",
                    "json",
                    "-show_format",
                    "-show_streams",
                    path,
                ],
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except (subprocess.SubprocessError, OSError) as exc:
            return [("Media Info", f"Unable to read media metadata: {exc}")]

        if not result.stdout:
            return [("Media Info", "Metadata failed to parse.")]

        try:
            data = json.loads(result.stdout)
        except (json.JSONDecodeError, TypeError):
            return [("Media Info", "Metadata failed to parse.")]

        rows: list[tuple[str, str]] = []
        format_info = data.get("format", {})
        if format_info:
            if duration := format_info.get("duration"):
                rows.append(("Duration", self._format_duration(duration)))
            if bitrate := format_info.get("bit_rate"):
                rows.append(("Overall Bitrate", f"{self._format_bitrate(bitrate)}"))
            if container := format_info.get("format_name"):
                rows.append(("Container", container))

        streams = data.get("streams", [])
        video_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
        audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)

        if video_stream:
            rows.extend(self._video_stream_rows(video_stream))
        if audio_stream:
            rows.extend(self._audio_stream_rows(audio_stream))

        tags = {}
        tags.update(format_info.get("tags", {}) if isinstance(format_info.get("tags"), dict) else {})
        if audio_stream and isinstance(audio_stream.get("tags"), dict):
            tags.update(audio_stream["tags"])
        if video_stream and isinstance(video_stream.get("tags"), dict):
            tags.update(video_stream["tags"])

        for key, value in self._format_media_tags(tags).items():
            rows.append((key, value))

        return rows

    @staticmethod
    def _format_duration(duration: str) -> str:
        try:
            total_seconds = float(duration)
        except (TypeError, ValueError):
            return duration
        minutes, seconds = divmod(int(total_seconds), 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours:d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:d}:{seconds:02d}"

    @staticmethod
    def _format_bitrate(bit_rate: str) -> str:
        try:
            return f"{int(bit_rate) / 1000:.0f} kbps"
        except (TypeError, ValueError):
            return bit_rate

    @staticmethod
    def _video_stream_rows(stream: dict[str, object]) -> list[tuple[str, str]]:
        rows: list[tuple[str, str]] = [("Media Type", "Video")]
        if codec := stream.get("codec_name"):
            rows.append(("Video Codec", str(codec)))
        width = stream.get("width")
        height = stream.get("height")
        if width and height:
            rows.append(("Resolution", f"{width}x{height}"))
        fps = MediaServerApp._parse_frame_rate(stream.get("avg_frame_rate"))
        if not fps:
            fps = MediaServerApp._parse_frame_rate(stream.get("r_frame_rate"))
        if fps:
            rows.append(("FPS", fps))
        if bit_rate := stream.get("bit_rate"):
            rows.append(("Video Bitrate", MediaServerApp._format_bitrate(str(bit_rate))))
        return rows

    @staticmethod
    def _audio_stream_rows(stream: dict[str, object]) -> list[tuple[str, str]]:
        rows: list[tuple[str, str]] = [("Media Type", "Audio")]
        if codec := stream.get("codec_name"):
            rows.append(("Audio Codec", str(codec)))
        if bit_rate := stream.get("bit_rate"):
            rows.append(("Audio Bitrate", MediaServerApp._format_bitrate(str(bit_rate))))
        if sample_rate := stream.get("sample_rate"):
            rows.append(("Sample Rate", f"{int(sample_rate):,} Hz"))
        if channels := stream.get("channels"):
            rows.append(("Channels", str(channels)))
        return rows

    @staticmethod
    def _parse_frame_rate(rate: object) -> str | None:
        if not rate or not isinstance(rate, str) or rate == "0/0":
            return None
        try:
            value = float(Fraction(rate))
        except (ValueError, ZeroDivisionError):
            return None
        return f"{value:.2f}"

    @staticmethod
    def _format_media_tags(tags: dict[str, str]) -> dict[str, str]:
        preferred = {
            "artist": "Artist",
            "album": "Album",
            "title": "Track Title",
            "genre": "Genre",
            "track": "Track",
            "date": "Year",
        }
        formatted: dict[str, str] = {}
        seen = set()
        for key, label in preferred.items():
            value = tags.get(key) or tags.get(key.upper())
            if value:
                formatted[label] = str(value)
                seen.add(key)
                seen.add(key.upper())
        for key, value in sorted(tags.items()):
            if key in seen:
                continue
            formatted[f"Tag ({key})"] = str(value)
        return formatted

def headless_list_libraries(db_path: str) -> None:
    db = LibraryDB(db_path)
    try:
        libraries = db.fetch_libraries()
        if not libraries:
            print("No libraries configured.")
            return
        for library in libraries:
            location = library.path
            if library.library_type == "remote":
                location = f"{library.username or 'user'}@{library.host}:{library.path}"
            print(f"[{library.library_id}] {library.name} ({library.library_type}): {location}")
    finally:
        db.close()


def headless_run_workflow(workflow_name: str, options_file: str | None, action: str) -> int:
    runner = load_workflow_runner(workflow_name)
    if runner is None:
        names = ", ".join(available_workflow_names()) or "none found"
        print(f"Unable to load workflow '{workflow_name}'. Available workflows: {names}")
        return 1

    options = {option.key: option.value for option in getattr(runner, "option_definitions")()}
    if options_file:
        try:
            overrides = json.loads(Path(options_file).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            print(f"Unable to read workflow options file: {exc}")
            return 1
        if not isinstance(overrides, dict):
            print("Workflow options file must contain a JSON object of option overrides.")
            return 1
        options.update({str(key): str(value) for key, value in overrides.items()})

    try:
        plan = getattr(runner, "build_plan")(options)
        preview_items = getattr(runner, "preview_items")(plan)
    except Exception as exc:  # noqa: BLE001
        print(f"Unable to build workflow plan: {exc}")
        return 1

    print(f"Workflow: {workflow_name}")
    for label, value in preview_items:
        print(f"  {label}: {value}")
    if action == "plan":
        print("Plan only; re-run with --workflow-action apply to make changes.")
        return 0

    try:
        result = getattr(runner, "apply")(options, plan)
    except Exception as exc:  # noqa: BLE001
        print(f"Workflow apply failed: {exc}")
        return 1
    for label, value in getattr(result, "summary_items", []):
        print(f"  {label}: {value}")
    rollback_script = getattr(result, "rollback_script", None)
    if rollback_script:
        print(f"Rollback script: {rollback_script}")
    return 0


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DB_DEFAULT_PATH, help="SQLite database location.")
    parser.add_argument("--nogui", action="store_true", help="Run in CLI-only mode.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).",
    )
    parser.add_argument(
        "--audio-backend",
        choices=["simpleaudio", "sounddevice"],
        default="simpleaudio",
        help="Choose the playback backend for audio files.",
    )
    parser.add_argument(
        "--list-libraries",
        action="store_true",
        help="List configured libraries and exit (implies --nogui).",
    )
    parser.add_argument(
        "--run-workflow",
        metavar="NAME",
        help="Run a workflow headlessly and exit (implies --nogui).",
    )
    parser.add_argument(
        "--workflow-options",
        metavar="FILE",
        help="JSON file with option overrides for --run-workflow.",
    )
    parser.add_argument(
        "--workflow-action",
        choices=["plan", "apply"],
        default="plan",
        help="Whether --run-workflow previews the plan or applies it (default: plan).",
    )
    args = parser.parse_args()

    startup_messages: list[str] = []
    resolved_audio_backend = resolve_audio_backend(
        args.audio_backend,
        emit_message=startup_messages.append,
    )

    log_file = configure_logging(args.log_level)
    logger.info("Using database at %s", args.db)
    logger.info("Log file: %s", log_file)
    for message in startup_messages:
        logger.warning(message)
    logger.info("Audio backend: %s", resolved_audio_backend)

    if args.list_libraries or args.run_workflow:
        logger.info("Running headless command; GUI disabled")
        exit_code = 0
        if args.list_libraries:
            headless_list_libraries(args.db)
        if args.run_workflow:
            exit_code = headless_run_workflow(
                args.run_workflow, args.workflow_options, args.workflow_action
            )
        if exit_code:
            raise SystemExit(exit_code)
        return

    if args.nogui:
        for message in startup_messages:
            print(f"Audio backend warning: {message}")
        print(
            "GUI disabled. Use --list-libraries or --run-workflow NAME to work headlessly; "
            "provide --db to change database location."
        )
        logger.info("Running in CLI-only mode; GUI disabled")
        return

    root = tk.Tk()
    db = LibraryDB(args.db)
    try:
        if startup_messages:
            messagebox.showwarning(
                "Audio Backend Fallback",
                "\n".join(startup_messages),
                parent=root,
            )
        MediaServerApp(root, db, audio_backend=resolved_audio_backend)
        root.mainloop()
    finally:
        db.close()


if __name__ == "__main__":
    run()
