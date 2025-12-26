# Media Server Organizer

Media Server Organizer is a desktop GUI for organizing the files and folders of a media server (music, videos, books, comics). It keeps common chores in one place while staying predictable and conservative about what it changes.

![Main application window](mso_window.png)

## Current version
- **Library management UI** with a local SQLite database for storing libraries.
- **Local + remote libraries** (remote entries stored via SSH host/user details).
- **Tabbed library view** with browsing for folders/files inside the selected library.
- **Folder tree sidebar** for local libraries, with expand/collapse and navigation on double-click.
- **Metadata panel** that surfaces basic file details and media info (via `ffprobe` when available).
- **Theme editor** to customize UI colors and save theme presets.
- **Built-in playback** with audio controls (play/pause/stop, progress, volume) and video launchers from the library or folder tree.
- **Workflow dialog** with runnable workflows (library merge and library cleaner) that collect options, preview planned moves, and write rollback scripts and logs after applying changes.
- **CLI options** for database location and headless mode (`--db`, `--nogui`).

## Goals
- Organize files into existing, user-defined folder layouts without forcing a new structure.
- Make changes explicit with previews, logs, and reversible steps where possible.
- Keep workflows simple: select sources, review proposals, apply.
- Support mixed libraries (music, video, books, comics) without trying to be an all-in-one media manager.

## Getting started
```bash
python src/media_server_manager.py
```

### CLI options
```bash
python src/media_server_manager.py --db /path/to/media.db
python src/media_server_manager.py --nogui
```
