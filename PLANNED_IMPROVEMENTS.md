# Planned Improvements

## Improvements (existing behavior/quality/robustness/perf/UX/docs/tests)

1. **Fix startup crash when `watchdog` is not installed** — `importlib.util.find_spec("watchdog.events")` raises `ModuleNotFoundError` when the parent package is missing, which currently breaks app startup *and* test collection.
2. **Make the documented `python src/media_server_manager.py` launch actually work** — the module imports `src.workflows...`, which fails when run script-style; bootstrap the repo root onto `sys.path`.
3. **Replace deprecated `datetime.utcnow()`** in `LibraryDB.upsert_audio_signature` with a timezone-aware UTC timestamp (deprecated since Python 3.12).
4. **Clean up orphaned rows when a library is deleted** — `delete_library` leaves stale `library_items` rows and dangling `audio_signatures.library_id` references behind.
5. **Escape SQL LIKE wildcards in search** — searching for `%` or `_` currently matches everything; escape them so searches are literal.
6. **Batch library indexing inserts** — `index_library_items` does one `execute` per row; use `executemany` for markedly faster indexing of large libraries.
7. **Deterministic dedup plans** — `library_dedup.scan_library` returns files in `os.walk` order while the other runners sort; sort it so duplicate groups and previews are stable run-to-run.
8. **Fix tag-editor rollback** — the generated script runs `mv X X` (which fails under `set -eu`) for in-place edits and never restores original tags; make rollback restore the original file content from the `.mso-backup` copy.
9. **Remove dead `_edit_metadata_value` method** — it references the nonexistent `self.metadata_labels` and would raise `AttributeError` if ever wired up.
10. **Add unit tests for the untested `library_cleaner` and `library_merge` runners** — cover filename parsing, template rendering, plan building, apply, and rollback-script generation.

## New Features

1. **Working Export menu** — export the current library's indexed contents to CSV or JSON via a save dialog (backed by a new `LibraryDB.fetch_library_items`), replacing the "future update" placeholder.
2. **Headless workflow CLI** — `--list-libraries` and `--run-workflow NAME [--workflow-options FILE] [--workflow-action plan|apply]` so the merge/cleaner/dedup workflows can run without the GUI (makes `--nogui` genuinely useful, e.g. for cron).
3. **Theme persistence** — "Save Theme" in the Theme Editor actually writes `src/themes/<name>.json` so custom themes survive restarts (today it only shows a fake "saved" dialog).
4. **Playlist rename/delete UI** — right-click context menu on the playlists list wiring up the already-implemented `PlaylistManager.rename_playlist`/`delete_playlist`.
5. **Parent-folder navigation in library tabs** — an "Up" row appears while browsing below the library root so users can navigate back up without the sidebar (currently there is no way back inside a tab).

## Additional fixes discovered during implementation

- **Self-collision rename bug in cleaner/merge planning** — `resolve_collision` ran before the `destination == source` skip check, so already-organized files collided with themselves and were planned for a spurious `" (1)"` rename (merging a library into itself would rename every file). Surfaced by the new runner tests.
- **Linux startup crash** — the bundled Default theme uses Windows-only color names (`SystemButtonFace`, ...); `_apply_theme` raised `TclError` during `__init__` on non-Windows platforms. Colors are now validated with portable fallbacks.
- **Two pre-existing audio test failures** — `tests/test_media_server_audio.py` patches `msm.simpleaudio`, but the module only imported it under `TYPE_CHECKING`; `simpleaudio` is now a module-level optional import.
- **`__pycache__` listed as a workflow** — workflow discovery now requires a `runner.py` and skips `_`/`.` prefixed directories.
- **Added `.gitignore`** — the app writes `logs/` and `playlists/` into the checkout at runtime and nothing ignored `__pycache__`.
