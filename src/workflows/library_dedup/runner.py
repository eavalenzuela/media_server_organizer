import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from src.media_server_manager import DB_DEFAULT_PATH, LibraryDB
except ModuleNotFoundError:  # Script-style execution fallback.
    from media_server_manager import DB_DEFAULT_PATH, LibraryDB

SUPPORTED_EXTENSIONS = {".mp3", ".flac", ".m4a", ".aac", ".ogg", ".wav", ".alac"}


@dataclass(frozen=True)
class WorkflowOption:
    key: str
    label: str
    value: str


@dataclass(frozen=True)
class AudioCandidate:
    path: Path
    signature: str
    bitrate: int | None
    sample_rate: int | None
    format_name: str | None
    size_bytes: int


@dataclass
class DuplicateGroup:
    signature: str
    candidates: list[AudioCandidate]
    best: AudioCandidate


@dataclass
class DedupPlan:
    library_root: Path
    duplicates: list[DuplicateGroup]
    actions: list["DedupAction"]
    mode: str
    dry_run: bool
    quarantine_folder: Path | None
    skipped: int
    delete_backup_folder: Path | None


@dataclass
class DedupAction:
    signature: str
    best_path: Path
    candidate_path: Path
    action: str
    destination: Path | None = None


@dataclass
class WorkflowResult:
    summary_items: list[tuple[str, str]]
    rollback_script: Path | None
    rollback_powershell_script: Path | None = None


class LibraryDedupWorkflow:
    name = "library_dedup"
    description = "Find duplicate audio files by content signature and keep the best-quality copy."

    def option_definitions(self) -> list[WorkflowOption]:
        home_music = str(Path.home() / "Music")
        return [
            WorkflowOption("library_path", "Library path", home_music),
            WorkflowOption("extensions", "Extensions", ", ".join(sorted(SUPPORTED_EXTENSIONS))),
            WorkflowOption("use_ffprobe", "Use ffprobe (auto/true/false)", "auto"),
            WorkflowOption("mode", "Mode (report-only/quarantine/delete)", "report-only"),
            WorkflowOption("quarantine_folder", "Quarantine folder", ""),
            WorkflowOption("dry_run", "Dry run (true/false)", "false"),
            WorkflowOption("db_path", "Database path", DB_DEFAULT_PATH),
        ]

    def build_plan(self, options: dict[str, str]) -> DedupPlan:
        resolved = normalize_options(options)
        library_root = resolved["library_path"]
        extensions = resolved["extensions"]
        use_ffprobe = resolved["use_ffprobe"]

        signature_groups: dict[str, list[AudioCandidate]] = {}
        skipped = 0

        for audio_path in scan_library(library_root, extensions):
            try:
                signature = compute_audio_signature(audio_path)
                bitrate, sample_rate, format_name = extract_audio_quality(audio_path, use_ffprobe)
                candidate = AudioCandidate(
                    path=audio_path,
                    signature=signature,
                    bitrate=bitrate,
                    sample_rate=sample_rate,
                    format_name=format_name,
                    size_bytes=audio_path.stat().st_size,
                )
                signature_groups.setdefault(signature, []).append(candidate)
            except (OSError, subprocess.SubprocessError, ValueError):
                skipped += 1

        duplicates: list[DuplicateGroup] = []
        actions: list[DedupAction] = []
        for signature, candidates in signature_groups.items():
            if len(candidates) < 2:
                continue
            best = select_best_candidate(candidates)
            duplicates.append(DuplicateGroup(signature=signature, candidates=candidates, best=best))

            for candidate in candidates:
                if candidate.path == best.path:
                    continue
                action = DedupAction(
                    signature=signature,
                    best_path=best.path,
                    candidate_path=candidate.path,
                    action=resolved["mode"],
                )
                if resolved["mode"] == "quarantine":
                    quarantine_root = resolved["quarantine_folder"]
                    if quarantine_root is None:
                        raise ValueError("Quarantine folder is required for quarantine mode.")
                    relative_path = candidate.path.relative_to(library_root)
                    action.destination = resolve_collision(quarantine_root / relative_path, {
                        item.destination for item in actions if item.destination is not None
                    })
                if resolved["mode"] == "delete":
                    delete_backup_root = resolved["delete_backup_folder"]
                    if delete_backup_root is None:
                        raise ValueError("Delete backup folder is required for delete mode.")
                    relative_path = candidate.path.relative_to(library_root)
                    action.destination = resolve_collision(
                        delete_backup_root / relative_path,
                        {item.destination for item in actions if item.destination is not None},
                    )
                actions.append(action)

        return DedupPlan(
            library_root=library_root,
            duplicates=duplicates,
            actions=actions,
            mode=resolved["mode"],
            dry_run=resolved["dry_run"],
            quarantine_folder=resolved["quarantine_folder"],
            skipped=skipped,
            delete_backup_folder=resolved["delete_backup_folder"],
        )

    def preview_items(self, plan: DedupPlan) -> list[tuple[str, str]]:
        items: list[tuple[str, str]] = [
            ("Files skipped", str(plan.skipped)),
            ("Duplicate signatures", str(len(plan.duplicates))),
            ("Mode", plan.mode),
            ("Dry run", str(plan.dry_run).lower()),
            ("Planned actions", str(len(plan.actions))),
        ]
        preview_count = min(8, len(plan.duplicates))
        for group in plan.duplicates[:preview_count]:
            best_name = str(group.best.path.relative_to(plan.library_root))
            other_count = len(group.candidates) - 1
            items.append(
                (
                    f"Group {group.signature[:10]}...",
                    f"Best: {best_name}; candidates: {len(group.candidates)}; actions: {other_count}",
                )
            )
        if len(plan.duplicates) > preview_count:
            items.append(("Additional duplicate groups", str(len(plan.duplicates) - preview_count)))
        return items

    def apply(self, options: dict[str, str], plan: DedupPlan) -> WorkflowResult:
        resolved = normalize_options(options)
        log_dir = ensure_log_dir(plan.library_root)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = log_dir / f"library_dedup_{timestamp}.json"
        rollback_path = log_dir / f"library_dedup_{timestamp}_rollback.sh"
        rollback_ps_path = log_dir / f"library_dedup_{timestamp}_rollback.ps1"

        db = LibraryDB(resolved["db_path"])
        library = db.find_library_by_path(str(resolved["library_path"]))
        kept_count = 0
        recorded_count = 0
        success_actions = 0
        error_actions = 0
        results: list[dict[str, Any]] = []

        try:
            try:
                with db.transaction():
                    for group in plan.duplicates:
                        best = group.best
                        for candidate in group.candidates:
                            kept = candidate.path == best.path
                            db.upsert_audio_signature(
                                path=str(candidate.path),
                                signature=candidate.signature,
                                library_id=library.library_id if library else None,
                                bitrate=candidate.bitrate,
                                sample_rate=candidate.sample_rate,
                                format_name=candidate.format_name,
                                kept=kept,
                                auto_commit=False,
                            )
                            recorded_count += 1
                        kept_count += 1
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to persist dedup signatures after recording {recorded_count} candidates."
                ) from exc

            for action in plan.actions:
                result_entry = {
                    "signature": action.signature,
                    "best": str(action.best_path),
                    "candidate": str(action.candidate_path),
                    "action": action.action,
                    "dry_run": plan.dry_run,
                }
                if action.destination is not None:
                    result_entry["destination"] = str(action.destination)
                try:
                    status = apply_action(action, plan.dry_run)
                    success_actions += 1
                    result_entry["status"] = status
                except OSError as exc:
                    error_actions += 1
                    result_entry["status"] = "error"
                    result_entry["error"] = str(exc)
                except Exception as exc:
                    error_actions += 1
                    result_entry["status"] = "error"
                    result_entry["error"] = str(exc)
                results.append(result_entry)
        finally:
            db.close()

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
            ("Duplicate groups analyzed", str(len(plan.duplicates))),
            ("Best tracks recorded", str(kept_count)),
            ("Signatures stored", str(recorded_count)),
            ("Actions attempted", str(len(plan.actions))),
            ("Actions completed", str(success_actions)),
            ("Action errors", str(error_actions)),
            ("Log file", str(log_path)),
        ]
        if plan.skipped:
            summary_items.append(("Files skipped", str(plan.skipped)))
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


def create_workflow() -> LibraryDedupWorkflow:
    return LibraryDedupWorkflow()


def normalize_options(options: dict[str, str]) -> dict[str, Any]:
    library_value = options.get("library_path", "").strip()
    if not library_value:
        raise ValueError("Library path is required.")
    library_path = Path(os.path.expanduser(library_value)).resolve()
    if not library_path.exists():
        raise ValueError(f"Library path does not exist: {library_path}")

    extensions_value = options.get("extensions", ", ".join(sorted(SUPPORTED_EXTENSIONS)))
    extensions = {normalize_extension(ext) for ext in extensions_value.split(",") if ext}
    extensions = {ext for ext in extensions if ext}
    if not extensions:
        raise ValueError("At least one extension is required.")

    use_ffprobe_value = options.get("use_ffprobe", "auto").strip().lower()
    if use_ffprobe_value not in {"auto", "true", "false"}:
        raise ValueError("Use ffprobe must be auto, true, or false.")
    if use_ffprobe_value == "auto":
        use_ffprobe = shutil_which("ffprobe") is not None
    else:
        use_ffprobe = use_ffprobe_value == "true"

    db_path_value = options.get("db_path", DB_DEFAULT_PATH).strip()
    db_path = Path(os.path.expanduser(db_path_value)).resolve()

    mode = options.get("mode", "report-only").strip().lower()
    if mode not in {"report-only", "quarantine", "delete"}:
        raise ValueError("Mode must be report-only, quarantine, or delete.")

    quarantine_value = options.get("quarantine_folder", "").strip()
    quarantine_folder: Path | None = None
    if mode == "quarantine":
        quarantine_folder = Path(os.path.expanduser(quarantine_value)).resolve() if quarantine_value else library_path / ".library_dedup" / "quarantine"

    delete_backup_folder: Path | None = None
    if mode == "delete":
        delete_backup_folder = library_path / ".library_dedup" / "delete_backup"

    dry_run_value = options.get("dry_run", "false").strip().lower()
    if dry_run_value not in {"true", "false"}:
        raise ValueError("Dry run must be true or false.")
    dry_run = dry_run_value == "true"

    return {
        "library_path": library_path,
        "extensions": extensions,
        "use_ffprobe": use_ffprobe,
        "db_path": db_path,
        "mode": mode,
        "quarantine_folder": quarantine_folder,
        "delete_backup_folder": delete_backup_folder,
        "dry_run": dry_run,
    }


def scan_library(library_root: Path, extensions: set[str]) -> list[Path]:
    files: list[Path] = []
    for root, _dirs, file_names in os.walk(library_root):
        for name in file_names:
            path = Path(root) / name
            if path.suffix.lower() in extensions:
                files.append(path)
    return files


def resolve_collision(destination: Path, planned_destinations: set[Path]) -> Path:
    if destination not in planned_destinations and not destination.exists():
        return destination

    parent = destination.parent
    stem = destination.stem
    suffix = destination.suffix
    counter = 1
    while True:
        candidate = parent / f"{stem} ({counter}){suffix}"
        if candidate not in planned_destinations and not candidate.exists():
            return candidate
        counter += 1


def apply_action(action: DedupAction, dry_run: bool) -> str:
    if action.action == "report-only" or dry_run:
        return "dry-run" if dry_run else "reported"
    if action.action == "quarantine":
        if action.destination is None:
            raise ValueError("Quarantine action is missing a destination.")
        action.destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(action.candidate_path), str(action.destination))
        return "moved"
    if action.action == "delete":
        if action.destination is None:
            raise ValueError("Delete action is missing a backup destination.")
        action.destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(action.candidate_path), str(action.destination))
        return "moved"
    raise ValueError(f"Unsupported action: {action.action}")


def ensure_log_dir(library_root: Path) -> Path:
    log_dir = library_root / ".library_dedup"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def write_rollback_script(rollback_path: Path, results: list[dict[str, Any]]) -> None:
    lines = ["#!/usr/bin/env sh", "set -eu", ""]
    for entry in results:
        if entry.get("status") != "moved":
            continue
        candidate = entry["candidate"]
        destination = entry.get("destination")
        if not destination:
            continue
        lines.append(f"if [ -e {json.dumps(destination)} ]; then")
        lines.append(f"  mkdir -p {json.dumps(os.path.dirname(candidate))}")
        lines.append(f"  mv {json.dumps(destination)} {json.dumps(candidate)}")
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
        candidate = entry["candidate"]
        destination = entry.get("destination")
        if not destination:
            continue
        lines.append(f"if (Test-Path -LiteralPath {json.dumps(destination)}) {{")
        lines.append(
            f"  New-Item -ItemType Directory -Path {json.dumps(os.path.dirname(candidate))} -Force | Out-Null"
        )
        lines.append(
            f"  Move-Item -LiteralPath {json.dumps(destination)} -Destination {json.dumps(candidate)} -Force"
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


def compute_audio_signature(path: Path) -> str:
    hasher = hashlib.sha1()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def extract_audio_quality(path: Path, use_ffprobe: bool) -> tuple[int | None, int | None, str | None]:
    if use_ffprobe:
        bitrate, sample_rate, format_name = probe_with_ffprobe(path)
        if bitrate is not None or sample_rate is not None or format_name is not None:
            return bitrate, sample_rate, format_name
    return fallback_audio_quality(path)


def probe_with_ffprobe(path: Path) -> tuple[int | None, int | None, str | None]:
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "a:0",
                "-show_entries",
                "stream=bit_rate,sample_rate,codec_name",
                "-of",
                "json",
                str(path),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None, None, None
    try:
        payload = json.loads(result.stdout)
        stream = (payload.get("streams") or [{}])[0]
        bitrate = int(stream.get("bit_rate")) if stream.get("bit_rate") else None
        sample_rate = int(stream.get("sample_rate")) if stream.get("sample_rate") else None
        format_name = stream.get("codec_name")
        return bitrate, sample_rate, format_name
    except (ValueError, TypeError, IndexError, KeyError):
        return None, None, None


def fallback_audio_quality(path: Path) -> tuple[int | None, int | None, str | None]:
    format_name = path.suffix.lstrip(".").lower() or None
    if importlib.util.find_spec("pydub") is None:
        return None, None, format_name
    from pydub import AudioSegment  # type: ignore
    try:
        audio = AudioSegment.from_file(path)
        bitrate = None
        frame_rate = getattr(audio, "frame_rate", None)
        sample_width = getattr(audio, "sample_width", None)
        channels = getattr(audio, "channels", None)
        if frame_rate and sample_width and channels:
            bitrate = frame_rate * sample_width * channels * 8
        return bitrate, audio.frame_rate or None, format_name
    except Exception:
        return None, None, format_name


def select_best_candidate(candidates: list[AudioCandidate]) -> AudioCandidate:
    def score(candidate: AudioCandidate) -> tuple[int, int, int, int]:
        format_rank = {
            "flac": 3,
            "alac": 3,
            "wav": 2,
            "aac": 2,
            "m4a": 2,
            "ogg": 2,
            "mp3": 1,
        }
        return (
            candidate.bitrate or 0,
            candidate.sample_rate or 0,
            format_rank.get((candidate.format_name or "").lower(), 0),
            candidate.size_bytes,
        )

    return sorted(candidates, key=score, reverse=True)[0]


def normalize_extension(value: str) -> str:
    value = value.strip().lower()
    if not value:
        return ""
    if not value.startswith("."):
        value = f".{value}"
    return value


def shutil_which(cmd: str) -> str | None:
    try:
        from shutil import which
    except ImportError:
        return None
    return which(cmd)


def build_options_dict(option_pairs: list[tuple[str, str]]) -> dict[str, str]:
    return {key: value for key, value in option_pairs}


def main() -> None:
    parser = argparse.ArgumentParser(description="Library deduplication workflow runner")
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
