from pathlib import Path
import sys

import pytest

import src.media_server_manager as media_server_manager
from src.media_server_manager import LibraryDB

sys.modules.setdefault("media_server_manager", media_server_manager)

from src.workflows.library_dedup.runner import (
    AudioCandidate,
    DedupPlan,
    DuplicateGroup,
    LibraryDedupWorkflow,
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
    plan = DedupPlan(
        library_root=tmp_path,
        duplicates=[DuplicateGroup(signature="sig-x", candidates=[first, second], best=first)],
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
