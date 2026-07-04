from pathlib import Path

import pytest

from src.media_server_manager import LibraryDB


from src.workflows.library_dedup.runner import (
    AudioCandidate,
    DedupAction,
    DedupPlan,
    DuplicateGroup,
    LibraryDedupWorkflow,
    apply_action,
    fallback_audio_quality,
    ps_quote,
    scan_library,
    select_best_candidate,
    sh_quote,
    write_rollback_powershell_script,
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
        delete_backup_folder=None,
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
        delete_backup_folder=None,
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
    delete_backup = tmp_path / "backup" / "delete.mp3"
    delete_action.destination = delete_backup
    status = apply_action(delete_action, dry_run=False)
    assert status == "moved"
    assert not file_to_delete.exists()
    assert delete_backup.exists()

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
    assert f"mv {sh_quote(str(destination))} {sh_quote(str(source))}" in content


def test_build_plan_creates_delete_backup_actions(tmp_path, monkeypatch):
    workflow = LibraryDedupWorkflow()
    library = tmp_path / "library"
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
            "mode": "delete",
            "dry_run": "false",
            "db_path": str(tmp_path / "dedup.db"),
        }
    )

    assert len(plan.actions) == 1
    action = plan.actions[0]
    assert action.action == "delete"
    assert action.destination is not None
    assert ".library_dedup" in str(action.destination)
    assert "delete_backup" in str(action.destination)


def test_rollback_scripts_restore_moved_delete_entries(tmp_path):
    rollback_sh = tmp_path / "rollback.sh"
    rollback_ps1 = tmp_path / "rollback.ps1"
    source = tmp_path / "library" / "dup.mp3"
    destination = tmp_path / ".library_dedup" / "delete_backup" / "dup.mp3"

    entries = [
        {
            "candidate": str(source),
            "destination": str(destination),
            "status": "moved",
        }
    ]

    write_rollback_script(rollback_sh, entries)
    write_rollback_powershell_script(rollback_ps1, entries)

    sh_content = rollback_sh.read_text(encoding="utf-8")
    ps_content = rollback_ps1.read_text(encoding="utf-8")

    assert f"mv {sh_quote(str(destination))} {sh_quote(str(source))}" in sh_content
    assert (
        f"Move-Item -LiteralPath {ps_quote(str(destination))} -Destination {ps_quote(str(source))} -Force"
        in ps_content
    )


def test_scan_library_skips_state_directory(tmp_path):
    (tmp_path / "song.mp3").write_bytes(b"a")
    backup_dir = tmp_path / ".library_dedup" / "delete_backup"
    backup_dir.mkdir(parents=True)
    # A backup copy left behind by a previous run must not be re-discovered.
    (backup_dir / "song.mp3").write_bytes(b"a")

    assert scan_library(tmp_path, {".mp3"}) == [tmp_path / "song.mp3"]


def test_select_best_candidate_never_prefers_state_dir_copy(tmp_path):
    live = _candidate(tmp_path / "A.mp3", "sig", 128000)
    backup = _candidate(tmp_path / ".library_dedup" / "delete_backup" / "A.mp3", "sig", 128000)

    # Regardless of ordering, a live file must outrank its own backup so the
    # original is never displaced into the backup tree on a subsequent run.
    assert select_best_candidate([backup, live]).path == live.path
    assert select_best_candidate([live, backup]).path == live.path


def test_fallback_audio_quality_accounts_for_channel_count(tmp_path, monkeypatch):
    sample = tmp_path / "sample.wav"
    sample.write_bytes(b"stub")

    class _Audio:
        frame_rate = 48000
        sample_width = 2
        channels = 1

    monkeypatch.setattr("src.workflows.library_dedup.runner.importlib.util.find_spec", lambda _name: object())
    monkeypatch.setattr("pydub.AudioSegment.from_file", lambda _path: _Audio())

    mono_bitrate, sample_rate, format_name = fallback_audio_quality(sample)
    assert mono_bitrate == 48000 * 2 * 1 * 8
    assert sample_rate == 48000
    assert format_name == "wav"

    _Audio.channels = 2
    stereo_bitrate, _, _ = fallback_audio_quality(sample)
    assert stereo_bitrate == 48000 * 2 * 2 * 8


def test_fallback_audio_quality_returns_none_bitrate_when_fields_missing(tmp_path, monkeypatch):
    sample = tmp_path / "sample.wav"
    sample.write_bytes(b"stub")

    class _Audio:
        frame_rate = 44100
        sample_width = 2
        channels = 2

    monkeypatch.setattr("src.workflows.library_dedup.runner.importlib.util.find_spec", lambda _name: object())
    monkeypatch.setattr("pydub.AudioSegment.from_file", lambda _path: _Audio())

    assert fallback_audio_quality(sample)[0] == 44100 * 2 * 2 * 8

    _Audio.channels = 0
    assert fallback_audio_quality(sample)[0] is None

    _Audio.channels = 2
    _Audio.sample_width = 0
    assert fallback_audio_quality(sample)[0] is None

    _Audio.sample_width = 2
    _Audio.frame_rate = 0
    assert fallback_audio_quality(sample)[0] is None
