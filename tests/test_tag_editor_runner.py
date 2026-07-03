import json
from pathlib import Path

from src.workflows.tag_editor.runner import (
    TagEditorWorkflow,
    TagUpdate,
    build_renamed_filename,
    resolve_collision,
)


def test_build_renamed_filename_uses_expected_tag_order_and_fallbacks():
    name = build_renamed_filename(
        {"track": "07", "artist": "Boards of Canada", "title": "Dayvan Cowboy"},
        ".flac",
    )
    assert name == "07 - Boards of Canada - Dayvan Cowboy.flac"

    fallback_name = build_renamed_filename({}, ".mp3")
    assert fallback_name == "00 - Unknown Artist - Unknown Title.mp3"


def test_resolve_collision_appends_incrementing_suffix(tmp_path):
    base = tmp_path / "07 - Boards of Canada - Dayvan Cowboy.flac"
    base.write_text("existing", encoding="utf-8")

    planned_destinations = {tmp_path / "07 - Boards of Canada - Dayvan Cowboy (1).flac"}
    resolved, collision = resolve_collision(base, planned_destinations)

    assert collision is True
    assert resolved.name == "07 - Boards of Canada - Dayvan Cowboy (2).flac"


def test_build_plan_marks_collision_for_rename(tmp_path, monkeypatch):
    first = tmp_path / "a.mp3"
    second = tmp_path / "b.mp3"
    first.write_bytes(b"a")
    second.write_bytes(b"b")

    monkeypatch.setattr(
        "src.workflows.tag_editor.runner.probe_tags",
        lambda _path: {
            "artist": "Boards of Canada",
            "album": "The Campfire Headphase",
            "title": "Dayvan Cowboy",
            "track": "07",
        },
    )

    workflow = TagEditorWorkflow()
    plan = workflow.build_plan(
        [first, second],
        TagUpdate(title="Dayvan Cowboy"),
        rename_files=True,
    )

    assert len(plan.edits) == 2
    first_edit, second_edit = plan.edits
    assert first_edit.renamed_to is not None
    assert second_edit.renamed_to is not None
    assert first_edit.collision is False
    assert second_edit.collision is True
    assert second_edit.renamed_to.name.endswith("(1).mp3")


def test_apply_rollback_restores_original_content_from_backup(tmp_path, monkeypatch):
    audio = tmp_path / "song.mp3"
    audio.write_bytes(b"original")

    monkeypatch.setattr(
        "src.workflows.tag_editor.runner.shutil.which", lambda _name: "/usr/bin/stub"
    )
    monkeypatch.setattr(
        "src.workflows.tag_editor.runner.probe_tags",
        lambda _path: {"artist": "Old", "album": "", "title": "", "track": ""},
    )

    def fake_write_tags(_source: Path, destination: Path, _tags: dict[str, str]) -> None:
        destination.write_bytes(b"modified")

    monkeypatch.setattr("src.workflows.tag_editor.runner.write_tags", fake_write_tags)

    workflow = TagEditorWorkflow()
    plan = workflow.build_plan([audio], TagUpdate(artist="New"), rename_files=False)
    assert len(plan.edits) == 1
    result = workflow.apply(plan, tmp_path)

    assert audio.read_bytes() == b"modified"
    backup = next(tmp_path.glob(".song.mp3.mso-backup*"))
    assert backup.read_bytes() == b"original"

    assert result.rollback_script is not None
    script = result.rollback_script.read_text(encoding="utf-8")
    assert f"mv {json.dumps(str(backup))} {json.dumps(str(audio))}" in script
    # In-place edits must not generate broken self-move (mv X X) entries.
    assert f"mv {json.dumps(str(audio))} {json.dumps(str(audio))}" not in script
