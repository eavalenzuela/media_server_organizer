import argparse
import hashlib
import importlib.util
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
    skipped: int


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
        for signature, candidates in signature_groups.items():
            if len(candidates) < 2:
                continue
            best = select_best_candidate(candidates)
            duplicates.append(DuplicateGroup(signature=signature, candidates=candidates, best=best))

        return DedupPlan(library_root=library_root, duplicates=duplicates, skipped=skipped)

    def preview_items(self, plan: DedupPlan) -> list[tuple[str, str]]:
        items: list[tuple[str, str]] = [
            ("Files skipped", str(plan.skipped)),
            ("Duplicate signatures", str(len(plan.duplicates))),
        ]
        preview_count = min(8, len(plan.duplicates))
        for group in plan.duplicates[:preview_count]:
            best_name = group.best.path.name
            other_count = len(group.candidates) - 1
            items.append(
                (
                    f"Keep: {best_name}",
                    f"Discard {other_count} duplicates for signature {group.signature[:10]}...",
                )
            )
        if len(plan.duplicates) > preview_count:
            items.append(("Additional duplicate groups", str(len(plan.duplicates) - preview_count)))
        return items

    def apply(self, options: dict[str, str], plan: DedupPlan) -> WorkflowResult:
        resolved = normalize_options(options)
        db = LibraryDB(resolved["db_path"])
        library = db.find_library_by_path(str(resolved["library_path"]))
        kept_count = 0
        recorded_count = 0

        try:
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
                    )
                    recorded_count += 1
                kept_count += 1
        finally:
            db.close()

        summary_items = [
            ("Duplicate groups analyzed", str(len(plan.duplicates))),
            ("Best tracks recorded", str(kept_count)),
            ("Signatures stored", str(recorded_count)),
        ]
        if plan.skipped:
            summary_items.append(("Files skipped", str(plan.skipped)))
        return WorkflowResult(summary_items=summary_items, rollback_script=None)

    def rollback(self, _rollback_script: Path) -> WorkflowResult:
        return WorkflowResult(
            summary_items=[("Rollback", "No rollback available for deduplication")],
            rollback_script=None,
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

    return {
        "library_path": library_path,
        "extensions": extensions,
        "use_ffprobe": use_ffprobe,
        "db_path": db_path,
    }


def scan_library(library_root: Path, extensions: set[str]) -> list[Path]:
    files: list[Path] = []
    for root, _dirs, file_names in os.walk(library_root):
        for name in file_names:
            path = Path(root) / name
            if path.suffix.lower() in extensions:
                files.append(path)
    return files


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
        if audio.frame_rate and audio.sample_width:
            bitrate = audio.frame_rate * audio.sample_width * 8
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
    parser.add_argument("action", choices=["plan", "apply"], help="Action to perform")
    parser.add_argument("--options", help="Path to JSON file with options")
    args = parser.parse_args()

    workflow = create_workflow()

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
