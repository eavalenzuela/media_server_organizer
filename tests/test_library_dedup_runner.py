import json
from pathlib import Path
import sys

import pytest

import src.media_server_manager as media_server_manager
from src.media_server_manager import LibraryDB

sys.modules.setdefault("media_server_manager", media_server_manager)

from src.workflows.library_dedup.runner import (
    AudioCandidate,
    DedupAction,
    DedupPlan,
    DuplicateGroup,
    LibraryDedupWorkflow,
    apply_action,
    write_rollback_script,
)


def _candidate(path: Path, signature: str, bitrate: int) -> AudioCandidate:
    return AudioCandidate(
        path=path,
        signature=signature,
        bitrate=bitrate,
        sample_rate=44100,
        format_name="mp3",
        size_bytes=1000,
    )


def test_apply_persists_expected_signature_rows(tmp_path):
    workflow = LibraryDedupWorkflow()
    db_path = tmp_path / "dedup.db"

    for filename in ("keep-a.mp3", "drop-a.mp3", "keep-b.mp3", "drop-b.mp3"):
        (tmp_path / filename).write_bytes(filename.encode("utf-8"))

    first = _candidate(tmp_path / "keep-a.mp3", "sig-a", 320000)
    second = _candidate(tmp_path / "drop-a.mp3", "sig-a", 128000)
    third = _candidate(tmp_path / "keep-b.mp3", "sig-b", 256000)
    fourth = _candidate(tmp_path / "drop-b.mp3", "sig-b", 96000)
    plan = DedupPlan(
        library_root=tmp_path,
        duplicates=[
            DuplicateGroup(signature="sig-a", candidates=[first, second], best=first),
            DuplicateGroup(signature="sig-b", candidates=[third, fourth], best=third),
        ],
        actions=[],
        mode="report-only",
        dry_run=False,
        quarantine_folder=None,
        skipped=0,
    )

    result = workflow.apply(
        {"library_path": str(tmp_path), "extensions": ".mp3", "use_ffprobe": "false", "db_path": str(db_path)},
        plan,
    )

    assert ("Signatures stored", "4") in result.summary_items

    db = LibraryDB(str(db_path))
    try:
        rows = {
            str(candidate.path): db.fetch_audio_signature_by_path(str(candidate.path))
            for candidate in (first, second, third, fourth)
        }
    finally:
        db.close()

    assert all(row is not None for row in rows.values())
    assert rows[str(first.path)].kept is True
    assert rows[str(second.path)].kept is False
    assert rows[str(third.path)].kept is True
    assert rows[str(fourth.path)].kept is False


def test_apply_rolls_back_transaction_when_upsert_fails(tmp_path, monkeypatch):
    workflow = LibraryDedupWorkflow()
    db_path = tmp_path / "dedup.db"

    first = _candidate(tmp_path / "ok.mp3", "sig-x", 320000)
    second = _candidate(tmp_path / "boom.mp3", "sig-x", 128000)
    first.path.write_bytes(b"ok")
    second.path.write_bytes(b"boom")
    plan = DedupPlan(
        library_root=tmp_path,
        duplicates=[DuplicateGroup(signature="sig-x", candidates=[first, second], best=first)],
        actions=[],
        mode="report-only",
        dry_run=False,
        quarantine_folder=None,
        skipped=0,
    )

    original_upsert = LibraryDB.upsert_audio_signature

    def flaky_upsert(self, path, *args, **kwargs):
        if path.endswith("boom.mp3"):
            raise ValueError("simulated failure")
        return original_upsert(self, path, *args, **kwargs)

    monkeypatch.setattr(LibraryDB, "upsert_audio_signature", flaky_upsert)

    with pytest.raises(RuntimeError, match="Failed to persist dedup signatures"):
        workflow.apply(
            {"library_path": str(tmp_path), "extensions": ".mp3", "use_ffprobe": "false", "db_path": str(db_path)},
            plan,
        )

    db = LibraryDB(str(db_path))
    try:
        assert db.fetch_audio_signature_by_path(str(first.path)) is None
        assert db.fetch_audio_signature_by_path(str(second.path)) is None
    finally:
        db.close()


def test_build_plan_creates_quarantine_actions(tmp_path, monkeypatch):
    workflow = LibraryDedupWorkflow()
    library = tmp_path / "library"
    quarantine = tmp_path / "quarantine"
    library.mkdir()
    (library / "best.mp3").write_bytes(b"same-content")
    (library / "dup.mp3").write_bytes(b"same-content")

    monkeypatch.setattr(
        "src.workflows.library_dedup.runner.extract_audio_quality",
        lambda _path, _use_ffprobe: (320000, 44100, "mp3"),
    )

    plan = workflow.build_plan(
        {
            "library_path": str(library),
            "extensions": ".mp3",
            "use_ffprobe": "false",
            "mode": "quarantine",
            "quarantine_folder": str(quarantine),
            "dry_run": "false",
            "db_path": str(tmp_path / "dedup.db"),
        }
    )

    assert len(plan.duplicates) == 1
    assert len(plan.actions) == 1
    action = plan.actions[0]
    assert action.action == "quarantine"
    assert action.destination is not None
    assert str(action.destination).startswith(str(quarantine))


def test_apply_action_delete_and_quarantine_behaviors(tmp_path):
    file_to_delete = tmp_path / "delete.mp3"
    file_to_delete.write_bytes(b"x")
    delete_action = DedupAction(
        signature="sig",
        best_path=tmp_path / "best.mp3",
        candidate_path=file_to_delete,
        action="delete",
    )
    status = apply_action(delete_action, dry_run=False)
    assert status == "deleted"
    assert not file_to_delete.exists()

    source = tmp_path / "source.mp3"
    source.write_bytes(b"y")
    destination = tmp_path / "q" / "source.mp3"
    quarantine_action = DedupAction(
        signature="sig",
        best_path=tmp_path / "best.mp3",
        candidate_path=source,
        action="quarantine",
        destination=destination,
    )
    status = apply_action(quarantine_action, dry_run=False)
    assert status == "moved"
    assert not source.exists()
    assert destination.exists()


def test_write_rollback_script_contains_quarantine_reversal(tmp_path):
    rollback_script = tmp_path / "rollback.sh"
    source = tmp_path / "library" / "dup.mp3"
    destination = tmp_path / "quarantine" / "dup.mp3"

    write_rollback_script(
        rollback_script,
        [
            {
                "candidate": str(source),
                "destination": str(destination),
                "status": "moved",
            }
        ],
    )

    content = rollback_script.read_text(encoding="utf-8")
    assert f"mv {json.dumps(str(destination))} {json.dumps(str(source))}" in content
