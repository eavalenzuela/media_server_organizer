import pytest

from src.workflows.library_merge.runner import (
    LibraryMergeWorkflow,
    normalize_extension,
    normalize_options,
    scan_library,
)


def test_normalize_extension_adds_leading_dot():
    assert normalize_extension("MP3") == ".mp3"
    assert normalize_extension(".flac") == ".flac"
    assert normalize_extension("") == ""


def test_normalize_options_requires_both_paths(tmp_path):
    with pytest.raises(ValueError, match="Source library path is required"):
        normalize_options({"destination_library_path": str(tmp_path)})
    with pytest.raises(ValueError, match="Destination library path is required"):
        normalize_options({"source_library_path": str(tmp_path)})


def test_scan_library_filters_extensions_and_sorts(tmp_path):
    (tmp_path / "b.mp3").write_bytes(b"b")
    (tmp_path / "a.mp3").write_bytes(b"a")
    (tmp_path / "notes.txt").write_bytes(b"n")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "c.mp3").write_bytes(b"c")

    files = scan_library(tmp_path, {".mp3"})
    assert files == sorted(files)
    assert [path.name for path in files] == ["a.mp3", "b.mp3", "c.mp3"]


def test_build_plan_preserves_structure_and_resolves_collisions(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    (source / "Artist").mkdir(parents=True)
    (source / "Artist" / "song.mp3").write_bytes(b"new")
    (destination / "Artist").mkdir(parents=True)
    (destination / "Artist" / "song.mp3").write_bytes(b"old")

    workflow = LibraryMergeWorkflow()
    plan = workflow.build_plan(
        {
            "source_library_path": str(source),
            "destination_library_path": str(destination),
            "extensions": ".mp3",
        }
    )

    assert len(plan.moves) == 1
    move = plan.moves[0]
    assert move.collision is True
    assert move.destination == destination / "Artist" / "song (1).mp3"


def test_build_plan_skips_files_already_in_destination(tmp_path):
    library = tmp_path / "library"
    library.mkdir()
    (library / "song.mp3").write_bytes(b"same")

    workflow = LibraryMergeWorkflow()
    plan = workflow.build_plan(
        {
            "source_library_path": str(library),
            "destination_library_path": str(library),
            "extensions": ".mp3",
        }
    )

    assert plan.moves == []
    assert plan.skipped == [library / "song.mp3"]


def test_apply_moves_files_and_writes_rollback_scripts(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    (source / "Artist").mkdir(parents=True)
    moved_file = source / "Artist" / "song.mp3"
    moved_file.write_bytes(b"audio")
    destination.mkdir()

    workflow = LibraryMergeWorkflow()
    options = {
        "source_library_path": str(source),
        "destination_library_path": str(destination),
        "extensions": ".mp3",
    }
    plan = workflow.build_plan(options)
    result = workflow.apply(options, plan)

    target = destination / "Artist" / "song.mp3"
    assert not moved_file.exists()
    assert target.exists()
    assert ("Moves completed", "1") in result.summary_items
    assert result.rollback_script is not None and result.rollback_script.exists()
    assert result.rollback_powershell_script is not None
    assert result.rollback_powershell_script.exists()
    content = result.rollback_script.read_text(encoding="utf-8")
    assert str(target) in content
    assert str(moved_file) in content
