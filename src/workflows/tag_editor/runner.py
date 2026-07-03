from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from src.workflows.library_cleaner.runner import (
    build_rollback_command,
    write_rollback_powershell_script,
    write_rollback_script,
)


@dataclass(frozen=True)
class TagUpdate:
    artist: str | None = None
    album: str | None = None
    title: str | None = None
    track: str | None = None


@dataclass(frozen=True)
class TagEditItem:
    source: Path
    original_tags: dict[str, str]
    updated_tags: dict[str, str]
    renamed_to: Path | None
    collision: bool


@dataclass(frozen=True)
class TagEditPlan:
    files_scanned: int
    edits: list[TagEditItem]
    skipped: list[Path]


@dataclass(frozen=True)
class WorkflowResult:
    summary_items: list[tuple[str, str]]
    rollback_script: Path | None
    rollback_powershell_script: Path | None = None


class TagEditorWorkflow:
    name = "tag_editor"
    description = "Bulk edit audio tags with optional rename from tags and rollback scripts."

    def build_plan(self, files: list[Path], updates: TagUpdate, rename_files: bool) -> TagEditPlan:
        planned_destinations: set[Path] = set()
        edits: list[TagEditItem] = []
        skipped: list[Path] = []
        for path in files:
            original_tags = probe_tags(path)
            updated_tags = merge_tags(original_tags, updates)
            renamed_to: Path | None = None
            collision = False
            if rename_files:
                candidate = path.with_name(build_renamed_filename(updated_tags, path.suffix))
                candidate, collision = resolve_collision(candidate, planned_destinations)
                renamed_to = candidate
                planned_destinations.add(candidate)
            if updated_tags == original_tags and (renamed_to is None or renamed_to == path):
                skipped.append(path)
                continue
            edits.append(
                TagEditItem(
                    source=path,
                    original_tags=original_tags,
                    updated_tags=updated_tags,
                    renamed_to=renamed_to,
                    collision=collision,
                )
            )
        return TagEditPlan(files_scanned=len(files), edits=edits, skipped=skipped)

    def preview_items(self, plan: TagEditPlan) -> list[str]:
        rows = [
            f"Files scanned: {plan.files_scanned}",
            f"Files to update: {len(plan.edits)}",
            f"Skipped: {len(plan.skipped)}",
        ]
        for edit in plan.edits[:10]:
            before = summarize_tags(edit.original_tags)
            after = summarize_tags(edit.updated_tags)
            suffix = ""
            if edit.renamed_to and edit.renamed_to != edit.source:
                suffix = f" | rename: {edit.source.name} -> {edit.renamed_to.name}"
                if edit.collision:
                    suffix += " (collision suffix)"
            rows.append(f"- {edit.source.name}: {before} => {after}{suffix}")
        if len(plan.edits) > 10:
            rows.append(f"... and {len(plan.edits) - 10} more")
        return rows

    def apply(self, plan: TagEditPlan, log_root: Path) -> WorkflowResult:
        if not plan.edits:
            return WorkflowResult(summary_items=[("Updated", "0"), ("Skipped", str(len(plan.skipped)))], rollback_script=None)

        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            raise RuntimeError("ffmpeg and ffprobe are required for bulk tag editing.")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_dir = log_root / ".library_cleaner"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"tag_editor_{timestamp}.json"
        rollback_path = log_dir / f"tag_editor_{timestamp}_rollback.sh"
        rollback_ps_path = log_dir / f"tag_editor_{timestamp}_rollback.ps1"

        results: list[dict[str, Any]] = []
        success = 0
        for edit in plan.edits:
            backup = unique_backup_path(edit.source)
            temp_output = edit.source.with_name(f".{edit.source.stem}.mso-edit{edit.source.suffix}")
            try:
                shutil.copy2(edit.source, backup)
                write_tags(edit.source, temp_output, edit.updated_tags)
                os.replace(temp_output, edit.source)
                final_path = edit.source
                collision = False
                if edit.renamed_to and edit.renamed_to != edit.source:
                    edit.renamed_to.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(edit.source), str(edit.renamed_to))
                    final_path = edit.renamed_to
                    collision = edit.collision
                results.append(
                    {
                        "source": str(edit.source),
                        "destination": str(final_path),
                        "backup": str(backup),
                        "status": "updated",
                        "collision": collision,
                    }
                )
                success += 1
            except Exception as exc:  # noqa: BLE001
                results.append(
                    {
                        "source": str(edit.source),
                        "destination": str(edit.renamed_to or edit.source),
                        "status": "error",
                        "error": str(exc),
                    }
                )
            finally:
                if temp_output.exists():
                    temp_output.unlink(missing_ok=True)

        rollback_entries: list[dict[str, str]] = []
        for result in results:
            if result.get("status") != "updated":
                continue
            if result["destination"] != result["source"]:
                # Undo the rename first so the restore below targets the original path.
                rollback_entries.append(
                    {
                        "source": result["source"],
                        "destination": result["destination"],
                        "status": "moved",
                    }
                )
            # Restore the pristine pre-edit copy (original tags) over the original path.
            rollback_entries.append(
                {
                    "source": result["source"],
                    "destination": result["backup"],
                    "status": "moved",
                }
            )

        write_rollback_script(rollback_path, rollback_entries)
        write_rollback_powershell_script(rollback_ps_path, rollback_entries)
        log_payload = {
            "workflow": self.name,
            "timestamp": timestamp,
            "results": results,
        }
        log_path.write_text(json.dumps(log_payload, indent=2), encoding="utf-8")

        return WorkflowResult(
            summary_items=[
                ("Files attempted", str(len(plan.edits))),
                ("Files updated", str(success)),
                ("Skipped", str(len(plan.skipped))),
                ("Log file", str(log_path)),
            ],
            rollback_script=rollback_path,
            rollback_powershell_script=rollback_ps_path,
        )

    def rollback(self, rollback_script: Path) -> WorkflowResult:
        if not rollback_script.exists():
            return WorkflowResult(summary_items=[("Rollback", f"Script not found: {rollback_script}")], rollback_script=None)
        try:
            subprocess.run(build_rollback_command(rollback_script), check=True)
        except subprocess.CalledProcessError as exc:
            return WorkflowResult(
                summary_items=[("Rollback", f"Failed with exit code {exc.returncode}")],
                rollback_script=rollback_script,
            )
        return WorkflowResult(summary_items=[("Rollback", "Rollback completed successfully")], rollback_script=rollback_script)


def create_workflow() -> TagEditorWorkflow:
    return TagEditorWorkflow()


def merge_tags(existing: dict[str, str], updates: TagUpdate) -> dict[str, str]:
    merged = dict(existing)
    for key in ("artist", "album", "title", "track"):
        value = getattr(updates, key)
        if value is not None and value != "":
            merged[key] = value
    return merged


def summarize_tags(tags: dict[str, str]) -> str:
    return " / ".join(
        [
            tags.get("artist", "—"),
            tags.get("album", "—"),
            tags.get("title", "—"),
            tags.get("track", "—"),
        ]
    )


def sanitize_component(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|]", "-", str(value or "")).strip()
    return cleaned or fallback


def build_renamed_filename(tags: dict[str, str], suffix: str) -> str:
    track = sanitize_component(tags.get("track", ""), "00")
    title = sanitize_component(tags.get("title", ""), "Unknown Title")
    artist = sanitize_component(tags.get("artist", ""), "Unknown Artist")
    return f"{track} - {artist} - {title}{suffix}"


def resolve_collision(destination: Path, planned_destinations: set[Path]) -> tuple[Path, bool]:
    if destination not in planned_destinations and not destination.exists():
        return destination, False
    counter = 1
    while True:
        candidate = destination.with_name(f"{destination.stem} ({counter}){destination.suffix}")
        if candidate not in planned_destinations and not candidate.exists():
            return candidate, True
        counter += 1


def unique_backup_path(path: Path) -> Path:
    counter = 0
    while True:
        suffix = "" if counter == 0 else f".{counter}"
        candidate = path.with_name(f".{path.name}.mso-backup{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def probe_tags(path: Path) -> dict[str, str]:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_format",
            str(path),
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    payload = json.loads(result.stdout or "{}")
    raw_tags = payload.get("format", {}).get("tags", {}) if isinstance(payload, dict) else {}
    normalized: dict[str, str] = {}
    if isinstance(raw_tags, dict):
        for key in ("artist", "album", "title", "track"):
            normalized[key] = str(raw_tags.get(key) or raw_tags.get(key.upper()) or "")
    return normalized


def write_tags(source: Path, destination: Path, tags: dict[str, str]) -> None:
    command = ["ffmpeg", "-y", "-i", str(source), "-map", "0", "-c", "copy"]
    for key in ("artist", "album", "title", "track"):
        if tags.get(key):
            command.extend(["-metadata", f"{key}={tags[key]}"])
    command.append(str(destination))
    subprocess.run(command, check=True, capture_output=True, text=True, encoding="utf-8", errors="replace")
