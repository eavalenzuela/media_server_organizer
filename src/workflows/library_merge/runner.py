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


@dataclass(frozen=True)
class WorkflowOption:
    key: str
    label: str
    value: str


@dataclass
class MoveAction:
    source: Path
    destination: Path
    collision: bool


@dataclass
class WorkflowPlan:
    source_root: Path
    destination_root: Path
    moves: list[MoveAction]
    skipped: list[Path]


@dataclass
class WorkflowResult:
    summary_items: list[tuple[str, str]]
    rollback_script: Path | None
    rollback_powershell_script: Path | None = None


class LibraryMergeWorkflow:
    name = "library_merge"
    description = "Merge one music library into another by moving files with collision handling."

    def option_definitions(self) -> list[WorkflowOption]:
        home_music = str(Path.home() / "Music")
        return [
            WorkflowOption("source_library_path", "Source library path", home_music),
            WorkflowOption("destination_library_path", "Destination library path", home_music),
            WorkflowOption("extensions", "Extensions", ", ".join(sorted(SUPPORTED_EXTENSIONS))),
        ]

    def build_plan(self, options: dict[str, str]) -> WorkflowPlan:
        resolved = normalize_options(options)
        source_root = resolved["source_library_path"]
        destination_root = resolved["destination_library_path"]
        extensions = resolved["extensions"]

        moves: list[MoveAction] = []
        skipped: list[Path] = []
        planned_destinations: set[Path] = set()

        for source in scan_library(source_root, extensions):
            destination = destination_root / source.relative_to(source_root)
            destination, collision = resolve_collision(destination, planned_destinations)

            if destination == source:
                skipped.append(source)
                continue

            planned_destinations.add(destination)
            moves.append(MoveAction(source=source, destination=destination, collision=collision))

        return WorkflowPlan(
            source_root=source_root,
            destination_root=destination_root,
            moves=moves,
            skipped=skipped,
        )

    def preview_items(self, plan: WorkflowPlan) -> list[tuple[str, str]]:
        items: list[tuple[str, str]] = [
            ("Files scanned", str(len(plan.moves) + len(plan.skipped))),
            ("Moves planned", str(len(plan.moves))),
            ("Already merged", str(len(plan.skipped))),
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
        log_path = log_dir / f"library_merge_{timestamp}.json"
        rollback_path = log_dir / f"library_merge_{timestamp}_rollback.sh"
        rollback_ps_path = log_dir / f"library_merge_{timestamp}_rollback.ps1"

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
        write_rollback_powershell_script(rollback_ps_path, results)
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
            summary_items.append(("Already merged", str(len(plan.skipped))))

        return WorkflowResult(
            summary_items=summary_items,
            rollback_script=rollback_path,
            rollback_powershell_script=rollback_ps_path,
        )

    def rollback(self, rollback_script: Path) -> WorkflowResult:
        if not rollback_script.exists():
            return WorkflowResult(
                summary_items=[("Rollback", f"Script not found: {rollback_script}")],
                rollback_script=None,
            )
        try:
            rollback_command = build_rollback_command(rollback_script)
            subprocess.run(rollback_command, check=True)
        except subprocess.CalledProcessError as exc:
            return WorkflowResult(
                summary_items=[("Rollback", f"Failed with exit code {exc.returncode}")],
                rollback_script=rollback_script,
            )
        return WorkflowResult(
            summary_items=[("Rollback", "Rollback completed successfully")],
            rollback_script=rollback_script,
        )


def create_workflow() -> LibraryMergeWorkflow:
    return LibraryMergeWorkflow()


def normalize_options(options: dict[str, str]) -> dict[str, Any]:
    source_value = options.get("source_library_path", "").strip()
    if not source_value:
        raise ValueError("Source library path is required.")
    source_root = Path(os.path.expanduser(source_value)).resolve()

    destination_value = options.get("destination_library_path", "").strip()
    if not destination_value:
        raise ValueError("Destination library path is required.")
    destination_root = Path(os.path.expanduser(destination_value)).resolve()

    extensions_value = options.get("extensions", ", ".join(sorted(SUPPORTED_EXTENSIONS)))
    extensions = {normalize_extension(ext) for ext in re.split(r"[,\s]+", extensions_value) if ext}
    extensions = {ext for ext in extensions if ext}
    if not extensions:
        raise ValueError("At least one extension is required.")

    return {
        "source_library_path": source_root,
        "destination_library_path": destination_root,
        "extensions": extensions,
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
    log_dir = destination_root / ".library_merge"
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


def write_rollback_powershell_script(
    rollback_path: Path, results: list[dict[str, Any]]
) -> None:
    lines = ["$ErrorActionPreference = 'Stop'", ""]
    for entry in results:
        if entry.get("status") != "moved":
            continue
        source = entry["source"]
        destination = entry["destination"]
        lines.append(f"if (Test-Path -LiteralPath {json.dumps(destination)}) {{")
        lines.append(f"  New-Item -ItemType Directory -Path {json.dumps(os.path.dirname(source))} -Force | Out-Null")
        lines.append(
            f"  Move-Item -LiteralPath {json.dumps(destination)} -Destination {json.dumps(source)} -Force"
        )
        lines.append("}")
    rollback_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_rollback_command(rollback_script: Path) -> list[str]:
    if os.name == "nt":
        return [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(rollback_script),
        ]
    return ["/bin/sh", str(rollback_script)]


def build_options_dict(option_pairs: list[tuple[str, str]]) -> dict[str, str]:
    return {key: value for key, value in option_pairs}


def main() -> None:
    parser = argparse.ArgumentParser(description="Library merge workflow runner")
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
