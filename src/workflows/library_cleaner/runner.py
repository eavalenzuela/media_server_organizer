from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


DEFAULT_EXTENSIONS = [".mp3", ".flac", ".m4a", ".ogg", ".wav", ".aac"]
DEFAULT_TEMPLATE = "{artist}/{album}/{track} - {title}"
UNKNOWN_ARTIST = "Unknown Artist"
UNKNOWN_ALBUM = "Unknown Album"


@dataclass(frozen=True)
class WorkflowOption:
    key: str
    label: str
    value: str


class WorkflowRunner:
    name = "library_cleaner"
    description = "Organize loose music files into artist/album folders with rollback support."

    def get_options(self) -> list[WorkflowOption]:
        home_music = Path.home() / "Music"
        return [
            WorkflowOption("library_path", "Library path", str(home_music)),
            WorkflowOption("destination_root", "Destination root", str(home_music)),
            WorkflowOption("template", "Path template", DEFAULT_TEMPLATE),
            WorkflowOption("extensions", "File extensions", ", ".join(DEFAULT_EXTENSIONS)),
            WorkflowOption("use_ffprobe", "Use ffprobe tags", "Yes"),
            WorkflowOption("keep_history", "Keep rollback history", "Yes"),
        ]

    def build_plan(self, options: dict[str, str]) -> dict[str, object]:
        parsed = self._parse_options(options)
        errors = self._validate_options(parsed)
        if errors:
            return {
                "errors": errors,
                "preview_items": [("Error", message) for message in errors],
                "summary_items": [("Errors", str(len(errors)))],
                "plan_items": [],
                "parsed": parsed,
            }
        files = self._scan_library(parsed["library_path"], parsed["extensions"])
        plan_items: list[dict[str, object]] = []
        used_destinations: set[Path] = set()
        collisions = 0
        skipped = 0
        for file_path in files:
            tags = self._read_tags(file_path, parsed["use_ffprobe"])
            destination = self._build_destination(file_path, tags, parsed)
            if destination.resolve() == file_path.resolve():
                skipped += 1
                continue
            resolved_destination, had_collision = self._resolve_collision(
                destination, used_destinations
            )
            if had_collision:
                collisions += 1
            used_destinations.add(resolved_destination)
            plan_items.append(
                {
                    "source": file_path,
                    "destination": resolved_destination,
                    "collision": had_collision,
                }
            )
        summary_items = [
            ("Files scanned", str(len(files))),
            ("Planned moves", str(len(plan_items))),
            ("Skipped (already organized)", str(skipped)),
            ("Collisions resolved", str(collisions)),
        ]
        preview_items = list(summary_items)
        for item in plan_items:
            source = item["source"]
            destination = item["destination"]
            note = " (collision resolved)" if item["collision"] else ""
            preview_items.append((source.name, f"→ {destination}{note}"))
        return {
            "errors": [],
            "preview_items": preview_items,
            "summary_items": summary_items,
            "plan_items": plan_items,
            "parsed": parsed,
        }

    def apply_plan(self, plan: dict[str, object]) -> dict[str, object]:
        parsed = plan["parsed"]
        plan_items = plan["plan_items"]
        if not plan_items:
            return {
                "summary_items": [("No changes", "No files needed organizing.")],
                "rollback_script": None,
            }
        history_dir = self._prepare_history_dir(
            parsed["library_path"], parsed["keep_history"]
        )
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = history_dir / f"library_cleaner_{timestamp}.log"
        rollback_path = history_dir / f"rollback_{timestamp}.sh"
        moved = 0
        errors = 0
        with log_path.open("w", encoding="utf-8") as log_file, rollback_path.open(
            "w", encoding="utf-8"
        ) as rollback_file:
            rollback_file.write("#!/bin/bash\nset -euo pipefail\n")
            for item in plan_items:
                source: Path = item["source"]
                destination: Path = item["destination"]
                destination.parent.mkdir(parents=True, exist_ok=True)
                try:
                    shutil.move(str(source), str(destination))
                    moved += 1
                    rollback_file.write(
                        f"mv -n {self._quote_path(destination)} {self._quote_path(source)}\n"
                    )
                    log_file.write(f"MOVED {source} -> {destination}\n")
                except OSError as exc:
                    errors += 1
                    log_file.write(f"ERROR {source} -> {destination} :: {exc}\n")
        os.chmod(rollback_path, 0o750)
        summary_items = [
            ("Files moved", str(moved)),
            ("Errors", str(errors)),
            ("Log file", str(log_path)),
            ("Rollback script", str(rollback_path)),
        ]
        return {
            "summary_items": summary_items,
            "rollback_script": str(rollback_path),
        }

    def rollback(self, rollback_script: str) -> dict[str, object]:
        if not rollback_script:
            return {"summary_items": [("Rollback", "No rollback script available.")]}
        rollback_path = Path(rollback_script)
        if not rollback_path.exists():
            return {
                "summary_items": [
                    ("Rollback", "Rollback script not found."),
                    ("Path", str(rollback_path)),
                ]
            }
        result = subprocess.run(
            ["bash", str(rollback_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        status = "Completed" if result.returncode == 0 else "Failed"
        summary_items = [
            ("Rollback", status),
            ("Exit code", str(result.returncode)),
        ]
        if result.stdout.strip():
            summary_items.append(("Output", result.stdout.strip()))
        if result.stderr.strip():
            summary_items.append(("Errors", result.stderr.strip()))
        return {"summary_items": summary_items}

    def _parse_options(self, options: dict[str, str]) -> dict[str, object]:
        library_path = Path(options.get("library_path", "")).expanduser()
        destination_root = options.get("destination_root") or str(library_path)
        destination_root_path = Path(destination_root).expanduser()
        template = options.get("template", DEFAULT_TEMPLATE).strip() or DEFAULT_TEMPLATE
        extensions = self._parse_extensions(options.get("extensions", ""))
        use_ffprobe = options.get("use_ffprobe", "Yes").strip().lower() in {"yes", "true", "1"}
        keep_history = options.get("keep_history", "Yes").strip().lower() in {"yes", "true", "1"}
        return {
            "library_path": library_path,
            "destination_root": destination_root_path,
            "template": template,
            "extensions": extensions,
            "use_ffprobe": use_ffprobe,
            "keep_history": keep_history,
        }

    def _validate_options(self, parsed: dict[str, object]) -> list[str]:
        errors: list[str] = []
        library_path: Path = parsed["library_path"]
        destination_root: Path = parsed["destination_root"]
        if not library_path.exists():
            errors.append(f"Library path does not exist: {library_path}")
        if not library_path.is_dir():
            errors.append(f"Library path is not a directory: {library_path}")
        if not destination_root.exists():
            errors.append(f"Destination root does not exist: {destination_root}")
        if not destination_root.is_dir():
            errors.append(f"Destination root is not a directory: {destination_root}")
        extensions: list[str] = parsed["extensions"]
        if not extensions:
            errors.append("No file extensions specified.")
        return errors

    def _scan_library(self, library_path: Path, extensions: list[str]) -> list[Path]:
        files: list[Path] = []
        for path in library_path.rglob("*"):
            if path.is_file() and path.suffix.lower() in extensions:
                files.append(path)
        return sorted(files)

    def _read_tags(self, file_path: Path, use_ffprobe: bool) -> dict[str, str]:
        tags: dict[str, str] = {}
        if use_ffprobe and shutil.which("ffprobe"):
            tags = self._read_tags_with_ffprobe(file_path)
        if not tags:
            tags = self._read_tags_from_filename(file_path)
        return tags

    def _read_tags_with_ffprobe(self, file_path: Path) -> dict[str, str]:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format_tags=artist,album,title,track",
                "-of",
                "json",
                str(file_path),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return {}
        data = json.loads(result.stdout)
        tags = data.get("format", {}).get("tags", {})
        return {
            "artist": str(tags.get("artist", "")).strip(),
            "album": str(tags.get("album", "")).strip(),
            "title": str(tags.get("title", "")).strip(),
            "track": str(tags.get("track", "")).strip(),
        }

    def _read_tags_from_filename(self, file_path: Path) -> dict[str, str]:
        stem = file_path.stem
        parts = [part.strip() for part in stem.split(" - ") if part.strip()]
        tags: dict[str, str] = {"artist": "", "album": "", "title": "", "track": ""}
        if len(parts) >= 4:
            tags["artist"] = parts[0]
            tags["album"] = parts[1]
            tags["track"] = parts[2]
            tags["title"] = " - ".join(parts[3:])
        elif len(parts) == 3:
            tags["artist"] = parts[0]
            tags["album"] = parts[1]
            tags["title"] = parts[2]
        elif len(parts) == 2:
            if parts[0].isdigit():
                tags["track"] = parts[0]
                tags["title"] = parts[1]
            else:
                tags["artist"] = parts[0]
                tags["title"] = parts[1]
        elif len(parts) == 1:
            tags["title"] = parts[0]
        return tags

    def _build_destination(
        self, file_path: Path, tags: dict[str, str], parsed: dict[str, object]
    ) -> Path:
        artist = tags.get("artist") or UNKNOWN_ARTIST
        album = tags.get("album") or UNKNOWN_ALBUM
        title = tags.get("title") or file_path.stem
        track = self._format_track(tags.get("track"))
        template = parsed["template"]
        rel_path = template.format(
            artist=artist,
            album=album,
            title=title,
            track=track,
            ext=file_path.suffix.lstrip("."),
        )
        rel_path = rel_path.replace("\\", "/")
        sanitized_parts = [self._sanitize_component(part) for part in Path(rel_path).parts]
        sanitized_rel = Path(*sanitized_parts)
        if "{ext}" in template:
            destination = parsed["destination_root"] / sanitized_rel
        else:
            destination = parsed["destination_root"] / f"{sanitized_rel}{file_path.suffix}"
        return destination

    def _format_track(self, track_value: str | None) -> str:
        if not track_value:
            return "00"
        track_value = track_value.split("/")[0].strip()
        if track_value.isdigit():
            return f"{int(track_value):02d}"
        return track_value

    def _resolve_collision(self, destination: Path, used_destinations: set[Path]) -> tuple[Path, bool]:
        if not destination.exists() and destination not in used_destinations:
            return destination, False
        stem = destination.stem
        suffix = destination.suffix
        parent = destination.parent
        counter = 1
        while True:
            candidate = parent / f"{stem} ({counter}){suffix}"
            if not candidate.exists() and candidate not in used_destinations:
                return candidate, True
            counter += 1

    def _prepare_history_dir(self, library_path: Path, keep_history: bool) -> Path:
        if keep_history:
            history_dir = library_path / ".library_cleaner_history"
            history_dir.mkdir(parents=True, exist_ok=True)
            return history_dir
        temp_dir = Path(tempfile.mkdtemp(prefix="library_cleaner_"))
        return temp_dir

    def _parse_extensions(self, extensions_raw: str) -> list[str]:
        if not extensions_raw.strip():
            return list(DEFAULT_EXTENSIONS)
        extensions = []
        for part in extensions_raw.split(","):
            cleaned = part.strip().lower()
            if not cleaned:
                continue
            if not cleaned.startswith("."):
                cleaned = f".{cleaned}"
            extensions.append(cleaned)
        return sorted(set(extensions))

    def _sanitize_component(self, value: str) -> str:
        cleaned = value.replace("/", "_").replace("\\", "_").strip()
        if not cleaned:
            return "_"
        return cleaned

    def _quote_path(self, path: Path) -> str:
        return subprocess.list2cmdline([str(path)])
