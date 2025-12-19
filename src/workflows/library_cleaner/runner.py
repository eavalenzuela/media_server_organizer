from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


MUSIC_EXTENSIONS = {
    ".mp3",
    ".flac",
    ".wav",
    ".m4a",
    ".aac",
    ".ogg",
    ".alac",
}

UNKNOWN_ARTIST = "Unknown Artist"
UNKNOWN_ALBUM = "Unknown Album"
UNKNOWN_TITLE = "Unknown Title"


@dataclass(frozen=True)
class TrackInfo:
    artist: str
    album: str
    title: str
    track: str


@dataclass(frozen=True)
class MovePlanItem:
    source: Path
    destination: Path
    collision: bool


@dataclass(frozen=True)
class MovePlan:
    library_path: Path
    template: str
    items: list[MovePlanItem]
    collisions: int
    skipped: int


@dataclass(frozen=True)
class ApplyResult:
    applied: int
    skipped: int
    rollback_script: Path | None
    log_path: Path | None
    run_directory: Path | None


def parse_extensions(raw_value: str) -> set[str]:
    if not raw_value:
        return set(MUSIC_EXTENSIONS)
    entries = {ext.strip().lower() for ext in raw_value.split(",") if ext.strip()}
    normalized = {ext if ext.startswith(".") else f".{ext}" for ext in entries}
    return normalized or set(MUSIC_EXTENSIONS)


def find_music_files(root: Path, extensions: Iterable[str]) -> list[Path]:
    results: list[Path] = []
    ext_set = {ext.lower() for ext in extensions}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() in ext_set:
            results.append(path)
    return sorted(results)


def ffprobe_available() -> bool:
    return shutil.which("ffprobe") is not None


def _ffprobe_tags(path: Path) -> dict[str, str]:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format_tags=artist,album,title,track",
            "-of",
            "default=noprint_wrappers=1:nokey=0",
            str(path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return {}
    tags: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().lower()
        tags[key] = value.strip()
    return tags


def _parse_filename_fallback(path: Path) -> TrackInfo:
    stem = path.stem
    track = ""
    title = stem
    if "-" in stem:
        parts = [part.strip() for part in stem.split("-", 1)]
        if parts and parts[0].isdigit():
            track = parts[0]
            title = parts[1] if len(parts) > 1 else stem
    artist = path.parent.parent.name if path.parent.parent else UNKNOWN_ARTIST
    album = path.parent.name if path.parent else UNKNOWN_ALBUM
    return TrackInfo(
        artist=artist or UNKNOWN_ARTIST,
        album=album or UNKNOWN_ALBUM,
        title=title or UNKNOWN_TITLE,
        track=track or "00",
    )


def read_track_info(path: Path) -> TrackInfo:
    if ffprobe_available():
        tags = _ffprobe_tags(path)
        artist = tags.get("artist", "").strip() or UNKNOWN_ARTIST
        album = tags.get("album", "").strip() or UNKNOWN_ALBUM
        title = tags.get("title", "").strip() or UNKNOWN_TITLE
        track = tags.get("track", "").strip()
        track = track.split("/", 1)[0] if track else "00"
        return TrackInfo(artist=artist, album=album, title=title, track=track or "00")
    return _parse_filename_fallback(path)


def _sanitize_component(value: str) -> str:
    cleaned = value.replace(os.sep, "-")
    cleaned = cleaned.replace("/", "-").replace("\\", "-").replace(":", "-")
    return cleaned.strip() or "Unknown"


def _format_track(track: str) -> str:
    if track.isdigit():
        return track.zfill(2)
    return track or "00"


def build_destination_path(
    library_path: Path, template: str, info: TrackInfo, extension: str
) -> Path:
    values = {
        "artist": _sanitize_component(info.artist or UNKNOWN_ARTIST),
        "album": _sanitize_component(info.album or UNKNOWN_ALBUM),
        "title": _sanitize_component(info.title or UNKNOWN_TITLE),
        "track": _sanitize_component(_format_track(info.track or "00")),
    }
    relative = template.format(**values)
    relative_path = Path(relative)
    return library_path / relative_path.with_suffix(extension)


def resolve_collision(destination: Path, planned: set[Path]) -> tuple[Path, bool]:
    if destination not in planned and not destination.exists():
        planned.add(destination)
        return destination, False
    stem = destination.stem
    suffix = destination.suffix
    parent = destination.parent
    index = 1
    while True:
        candidate = parent / f"{stem} ({index}){suffix}"
        if candidate not in planned and not candidate.exists():
            planned.add(candidate)
            return candidate, True
        index += 1


def build_plan(library_path: Path, template: str, extensions: set[str]) -> MovePlan:
    items: list[MovePlanItem] = []
    collisions = 0
    skipped = 0
    planned_destinations: set[Path] = set()
    for path in find_music_files(library_path, extensions):
        info = read_track_info(path)
        destination = build_destination_path(library_path, template, info, path.suffix)
        destination, had_collision = resolve_collision(destination, planned_destinations)
        if destination == path:
            skipped += 1
            continue
        if had_collision:
            collisions += 1
        items.append(MovePlanItem(source=path, destination=destination, collision=had_collision))
    return MovePlan(
        library_path=library_path,
        template=template,
        items=items,
        collisions=collisions,
        skipped=skipped,
    )


def write_rollback_script(plan: MovePlan, run_directory: Path) -> Path:
    rollback_path = run_directory / "rollback.sh"
    lines = ["#!/usr/bin/env bash", "set -euo pipefail", ""]
    for item in reversed(plan.items):
        source = shlex.quote(str(item.destination))
        destination = shlex.quote(str(item.source))
        lines.append(f"mkdir -p {shlex.quote(str(item.source.parent))}")
        lines.append(f"mv {source} {destination}")
    rollback_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    rollback_path.chmod(0o755)
    return rollback_path


def apply_plan(plan: MovePlan, run_directory: Path | None = None) -> ApplyResult:
    if not plan.items:
        return ApplyResult(applied=0, skipped=plan.skipped, rollback_script=None, log_path=None, run_directory=None)
    if run_directory is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_directory = plan.library_path / "workflow_runs" / "library_cleaner" / timestamp
    run_directory.mkdir(parents=True, exist_ok=True)
    rollback_script = write_rollback_script(plan, run_directory)
    log_path = run_directory / "apply.log"
    applied = 0
    with log_path.open("w", encoding="utf-8") as log_handle:
        for item in plan.items:
            item.destination.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(item.source), str(item.destination))
            except (OSError, shutil.Error) as exc:
                log_handle.write(f"ERROR: {item.source} -> {item.destination} :: {exc}\n")
                continue
            log_handle.write(f"MOVED: {item.source} -> {item.destination}\n")
            applied += 1
    run_summary = {
        "applied": applied,
        "skipped": plan.skipped,
        "collisions": plan.collisions,
        "rollback_script": str(rollback_script),
        "log_path": str(log_path),
    }
    (run_directory / "run.json").write_text(json.dumps(run_summary, indent=2), encoding="utf-8")
    return ApplyResult(
        applied=applied,
        skipped=plan.skipped,
        rollback_script=rollback_script,
        log_path=log_path,
        run_directory=run_directory,
    )


def execute_rollback(rollback_script: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["/usr/bin/env", "bash", str(rollback_script)],
        text=True,
        capture_output=True,
        check=False,
    )


def summarize_plan(plan: MovePlan, sample_limit: int = 3) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = [
        ("Library path", str(plan.library_path)),
        ("Template", plan.template),
        ("Files found", str(len(plan.items) + plan.skipped)),
        ("Planned moves", str(len(plan.items))),
        ("Collisions", str(plan.collisions)),
        ("Already organized", str(plan.skipped)),
    ]
    for item in plan.items[:sample_limit]:
        items.append(("Move", f"{item.source.name} -> {item.destination.relative_to(plan.library_path)}"))
    if len(plan.items) > sample_limit:
        items.append(("More", f"... and {len(plan.items) - sample_limit} more"))
    return items


def summarize_apply(result: ApplyResult) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = [
        ("Files moved", str(result.applied)),
        ("Skipped", str(result.skipped)),
    ]
    if result.log_path:
        items.append(("Log", str(result.log_path)))
    if result.rollback_script:
        items.append(("Rollback script", str(result.rollback_script)))
    return items


def default_options(library_path: str | None = None) -> list[tuple[str, str]]:
    return [
        ("Library path", library_path or ""),
        ("Template", "{artist}/{album}/{track} - {title}"),
        ("Extensions", ", ".join(sorted(MUSIC_EXTENSIONS))),
    ]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Library cleaner workflow runner.")
    parser.add_argument("library_path", help="Root library path to scan.")
    parser.add_argument("--template", default="{artist}/{album}/{track} - {title}")
    parser.add_argument(
        "--extensions",
        default=",".join(sorted(MUSIC_EXTENSIONS)),
        help="Comma-separated list of extensions (e.g. mp3,flac).",
    )
    parser.add_argument("--apply", action="store_true", help="Apply planned moves.")
    parser.add_argument("--rollback", help="Execute a rollback script.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.rollback:
        result = execute_rollback(Path(args.rollback))
        print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        return result.returncode
    library_path = Path(args.library_path).expanduser()
    extensions = parse_extensions(args.extensions)
    plan = build_plan(library_path, args.template, extensions)
    print(json.dumps([{"source": str(item.source), "destination": str(item.destination)} for item in plan.items]))
    if args.apply:
        apply_result = apply_plan(plan)
        print(json.dumps({"applied": apply_result.applied, "log": str(apply_result.log_path)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
