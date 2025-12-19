import argparse
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

SUPPORTED_EXTENSIONS = {".mp3", ".flac", ".m4a", ".aac", ".ogg", ".wav", ".alac"}

UNKNOWN_ARTIST = "Unknown Artist"
UNKNOWN_ALBUM = "Unknown Album"
UNKNOWN_TITLE = "Unknown Title"


@dataclass(frozen=True)
class WorkflowOption:
    key: str
    label: str
    value: str


@dataclass(frozen=True)
class TagInfo:
    artist: str
    album: str
    title: str
    track: str


@dataclass
class MoveAction:
    source: Path
    destination: Path
    collision: bool
    tag_info: TagInfo


@dataclass
class WorkflowPlan:
    library_root: Path
    destination_root: Path
    template: str
    moves: list[MoveAction]
    skipped: list[Path]


@dataclass
class WorkflowResult:
    summary_items: list[tuple[str, str]]
    rollback_script: Path | None


class LibraryCleanerWorkflow:
    name = "library_cleaner"
    description = (
        "Normalize loose music files into Artist/Album folders using audio tags or filename "
        "parsing with optional rollback support."
    )

    def option_definitions(self) -> list[WorkflowOption]:
        home_music = str(Path.home() / "Music")
        return [
            WorkflowOption("library_path", "Library path", home_music),
            WorkflowOption("destination_root", "Destination root", home_music),
            WorkflowOption("template", "Destination template", "{artist}/{album}/{track} - {title}"),
            WorkflowOption("extensions", "Extensions", ", ".join(sorted(SUPPORTED_EXTENSIONS))),
            WorkflowOption("use_ffprobe", "Use ffprobe (auto/true/false)", "auto"),
        ]

    def build_plan(self, options: dict[str, str]) -> WorkflowPlan:
        resolved = normalize_options(options)
        library_root = resolved["library_path"]
        destination_root = resolved["destination_root"]
        template = resolved["template"]
        extensions = resolved["extensions"]
        use_ffprobe = resolved["use_ffprobe"]

        moves: list[MoveAction] = []
        skipped: list[Path] = []
        planned_destinations: set[Path] = set()

        for source in scan_library(library_root, extensions):
            tag_info = extract_tags(source, use_ffprobe)
            destination = destination_root / render_template(template, tag_info)
            destination = destination.with_suffix(source.suffix)
            destination, collision = resolve_collision(destination, planned_destinations)

            if destination == source:
                skipped.append(source)
                continue

            planned_destinations.add(destination)
            moves.append(MoveAction(source=source, destination=destination, collision=collision, tag_info=tag_info))

        return WorkflowPlan(
            library_root=library_root,
            destination_root=destination_root,
            template=template,
            moves=moves,
            skipped=skipped,
        )

    def preview_items(self, plan: WorkflowPlan) -> list[tuple[str, str]]:
        items: list[tuple[str, str]] = [
            ("Files scanned", str(len(plan.moves) + len(plan.skipped))),
            ("Moves planned", str(len(plan.moves))),
            ("Already organized", str(len(plan.skipped))),
        ]
        collisions = sum(1 for move in plan.moves if move.collision)
        if collisions:
            items.append(("Collisions resolved", str(collisions)))

        preview_count = min(8, len(plan.moves))
        for move in plan.moves[:preview_count]:
            label = f"Move: {move.source.name}"
            value = str(move.destination.relative_to(plan.destination_root))
            if move.collision:
                value = f"{value} (renamed)"
            items.append((label, value))

        if len(plan.moves) > preview_count:
            items.append(("Additional moves", str(len(plan.moves) - preview_count)))
        return items

    def apply(self, options: dict[str, str], plan: WorkflowPlan) -> WorkflowResult:
        resolved = normalize_options(options)
        log_dir = ensure_log_dir(plan.destination_root)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = log_dir / f"library_cleaner_{timestamp}.json"
        rollback_path = log_dir / f"library_cleaner_{timestamp}_rollback.sh"

        results: list[dict[str, Any]] = []
        success_count = 0
        error_count = 0

        for move in plan.moves:
            try:
                move.destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(move.source), str(move.destination))
                success_count += 1
                results.append(
                    {
                        "source": str(move.source),
                        "destination": str(move.destination),
                        "status": "moved",
                        "collision": move.collision,
                    }
                )
            except (OSError, shutil.Error) as exc:
                error_count += 1
                results.append(
                    {
                        "source": str(move.source),
                        "destination": str(move.destination),
                        "status": "error",
                        "error": str(exc),
                        "collision": move.collision,
                    }
                )

        write_rollback_script(rollback_path, results)
        log_payload = {
            "workflow": self.name,
            "options": {key: str(value) for key, value in resolved.items()},
            "timestamp": timestamp,
            "results": results,
        }
        log_path.write_text(json.dumps(log_payload, indent=2), encoding="utf-8")

        summary_items = [
            ("Moves attempted", str(len(plan.moves))),
            ("Moves completed", str(success_count)),
            ("Errors", str(error_count)),
            ("Log file", str(log_path)),
        ]
        if plan.skipped:
            summary_items.append(("Already organized", str(len(plan.skipped))))

        return WorkflowResult(summary_items=summary_items, rollback_script=rollback_path)

    def rollback(self, rollback_script: Path) -> WorkflowResult:
        if not rollback_script.exists():
            return WorkflowResult(
                summary_items=[("Rollback", f"Script not found: {rollback_script}")],
                rollback_script=None,
            )
        try:
            subprocess.run(["/bin/bash", str(rollback_script)], check=True)
        except subprocess.CalledProcessError as exc:
            return WorkflowResult(
                summary_items=[("Rollback", f"Failed with exit code {exc.returncode}")],
                rollback_script=rollback_script,
            )
        return WorkflowResult(
            summary_items=[("Rollback", "Rollback completed successfully")],
            rollback_script=rollback_script,
        )


def create_workflow() -> LibraryCleanerWorkflow:
    return LibraryCleanerWorkflow()


def normalize_options(options: dict[str, str]) -> dict[str, Any]:
    library_value = options.get("library_path", "").strip()
    if not library_value:
        raise ValueError("Library path is required.")
    library_path = Path(os.path.expanduser(library_value)).resolve()
    destination_value = options.get("destination_root", "").strip() or str(library_path)
    destination_root = Path(os.path.expanduser(destination_value)).resolve()

    template = options.get("template", "{artist}/{album}/{track} - {title}").strip()
    if not template:
        raise ValueError("Destination template cannot be empty.")

    extensions_value = options.get("extensions", ", ".join(sorted(SUPPORTED_EXTENSIONS)))
    extensions = {normalize_extension(ext) for ext in re.split(r"[,\s]+", extensions_value) if ext}
    extensions = {ext for ext in extensions if ext}
    if not extensions:
        raise ValueError("At least one extension is required.")

    use_ffprobe_value = options.get("use_ffprobe", "auto").strip().lower()
    if use_ffprobe_value not in {"auto", "true", "false"}:
        raise ValueError("Use ffprobe must be auto, true, or false.")
    if use_ffprobe_value == "auto":
        use_ffprobe = shutil.which("ffprobe") is not None
    else:
        use_ffprobe = use_ffprobe_value == "true"

    return {
        "library_path": library_path,
        "destination_root": destination_root,
        "template": template,
        "extensions": extensions,
        "use_ffprobe": use_ffprobe,
    }


def normalize_extension(extension: str) -> str:
    if not extension:
        return ""
    extension = extension.lower()
    if not extension.startswith("."):
        extension = f".{extension}"
    return extension


def scan_library(library_root: Path, extensions: set[str]) -> list[Path]:
    if not library_root.exists():
        raise ValueError(f"Library path does not exist: {library_root}")
    if not library_root.is_dir():
        raise ValueError(f"Library path is not a directory: {library_root}")

    files: list[Path] = []
    for root, _dirs, filenames in os.walk(library_root):
        for filename in filenames:
            path = Path(root) / filename
            if path.suffix.lower() in extensions:
                files.append(path)
    return sorted(files)


def extract_tags(path: Path, use_ffprobe: bool) -> TagInfo:
    if use_ffprobe:
        tag_data = ffprobe_tags(path)
        if tag_data:
            track_number = parse_track_number(tag_data.get("track", ""))
            track_value = f"{track_number:02d}" if track_number is not None else "00"
            return TagInfo(
                artist=tag_data.get("artist") or UNKNOWN_ARTIST,
                album=tag_data.get("album") or UNKNOWN_ALBUM,
                title=tag_data.get("title") or UNKNOWN_TITLE,
                track=track_value,
            )
    return parse_filename(path)


def ffprobe_tags(path: Path) -> dict[str, str]:
    command = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_entries",
        "format_tags=artist,album,title,track",
        str(path),
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return {}
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}
    tags = payload.get("format", {}).get("tags", {})
    return {key.lower(): value for key, value in tags.items() if isinstance(value, str)}


def parse_filename(path: Path) -> TagInfo:
    stem = path.stem
    parts = [part.strip() for part in stem.split(" - ") if part.strip()]
    artist = UNKNOWN_ARTIST
    album = UNKNOWN_ALBUM
    title = UNKNOWN_TITLE
    track = "00"

    if len(parts) >= 4:
        artist, album, track, title = parts[0], parts[1], parts[2], " - ".join(parts[3:])
    elif len(parts) == 3:
        artist, track, title = parts
    elif len(parts) == 2:
        track, title = parts
    elif len(parts) == 1:
        title = parts[0]

    track_number = parse_track_number(track)
    if track_number is not None:
        track = f"{track_number:02d}"
    else:
        track = "00"

    return TagInfo(
        artist=normalize_component(artist, UNKNOWN_ARTIST),
        album=normalize_component(album, UNKNOWN_ALBUM),
        title=normalize_component(title, UNKNOWN_TITLE),
        track=track,
    )


def parse_track_number(value: str) -> int | None:
    match = re.search(r"\d+", value)
    if not match:
        return None
    try:
        return int(match.group(0))
    except ValueError:
        return None


def normalize_component(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|]", "-", value).strip()
    return cleaned or fallback


def render_template(template: str, tag_info: TagInfo) -> Path:
    path_value = template.format(
        artist=normalize_component(tag_info.artist, UNKNOWN_ARTIST),
        album=normalize_component(tag_info.album, UNKNOWN_ALBUM),
        title=normalize_component(tag_info.title, UNKNOWN_TITLE),
        track=tag_info.track or "00",
    )
    return Path(path_value)


def resolve_collision(destination: Path, planned_destinations: set[Path]) -> tuple[Path, bool]:
    if destination not in planned_destinations and not destination.exists():
        return destination, False

    parent = destination.parent
    stem = destination.stem
    suffix = destination.suffix
    counter = 1
    while True:
        candidate = parent / f"{stem} ({counter}){suffix}"
        if candidate not in planned_destinations and not candidate.exists():
            return candidate, True
        counter += 1


def ensure_log_dir(destination_root: Path) -> Path:
    log_dir = destination_root / ".library_cleaner"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def write_rollback_script(rollback_path: Path, results: list[dict[str, Any]]) -> None:
    lines = ["#!/usr/bin/env sh", "set -eu", ""]
    for entry in results:
        if entry.get("status") != "moved":
            continue
        source = entry["source"]
        destination = entry["destination"]
        lines.append(f"if [ -e {json.dumps(destination)} ]; then")
        lines.append(f"  mkdir -p {json.dumps(os.path.dirname(source))}")
        lines.append(f"  mv {json.dumps(destination)} {json.dumps(source)}")
        lines.append("fi")
    rollback_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    rollback_path.chmod(0o755)


def build_options_dict(option_pairs: list[tuple[str, str]]) -> dict[str, str]:
    return {key: value for key, value in option_pairs}


def main() -> None:
    parser = argparse.ArgumentParser(description="Library cleaner workflow runner")
    parser.add_argument("action", choices=["plan", "apply", "rollback"], help="Action to perform")
    parser.add_argument("--options", help="Path to JSON file with options")
    parser.add_argument("--rollback-script", help="Path to rollback script")
    args = parser.parse_args()

    workflow = create_workflow()

    if args.action == "rollback":
        if not args.rollback_script:
            raise SystemExit("--rollback-script is required for rollback action")
        result = workflow.rollback(Path(args.rollback_script))
        print(json.dumps(result.summary_items, indent=2))
        return

    if not args.options:
        raise SystemExit("--options is required for plan/apply action")

    options_data = json.loads(Path(args.options).read_text(encoding="utf-8"))
    plan = workflow.build_plan(options_data)

    if args.action == "plan":
        preview = workflow.preview_items(plan)
        print(json.dumps(preview, indent=2))
        return

    result = workflow.apply(options_data, plan)
    print(json.dumps(result.summary_items, indent=2))


if __name__ == "__main__":
    main()
