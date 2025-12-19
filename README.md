# media_server_organizer

a gui + tools for organizing the files and folders for a media server (music, videos, books, comics)

## overview
media_server_organizer keeps common media server chores in one place while staying predictable and conservative about what it changes. the focus is on repeatable organizing workflows, visibility into what will change before it happens, and a small set of utilities that match existing folder structures instead of inventing new ones.

## goals
- organize files into existing, user-defined folder layouts without forcing a new structure.
- make changes explicit with previews, logs, and reversible steps where possible.
- keep workflows simple: select sources, review proposals, apply.
- support mixed libraries (music, video, books, comics) without trying to be an all-in-one media manager.

## current scope
- a lightweight gui paired with a small set of command-line helpers.
- operations are file-focused (move, rename, cleanup), not metadata-heavy or library-aware.
- configuration is minimal and stored locally.

## proposed features

### major features
- **guided organize flow** that walks through select sources → preview plan → apply changes, with a summary at each step.
- **library profiles** to store per-library rules (base paths, folder patterns, allowed extensions) without changing underlying structure.
- **dry-run + undo history** that captures planned moves and makes it easy to revert recent operations.

### minor features
- **quick cleanup tools** such as empty folder removal, stray file detection, and duplicate extension audits.
- **batch rename helpers** for common naming patterns (season/episode, author/title, disc/track) that respect existing folder layouts.
- **import staging area** to review new drops before they touch the main library.

### improvements
- **safety enhancements** like conflict detection, path sanity checks, and confirmation prompts for destructive actions.
- **performance tuning** for large libraries (incremental scanning, cached directory snapshots, and resumable operations).
- **rules rework** to make patterns easier to read and test, with sample configs and a built-in validator.
