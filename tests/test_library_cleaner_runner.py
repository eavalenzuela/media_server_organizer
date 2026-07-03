from pathlib import Path

from src.workflows.library_cleaner.runner import (
    LibraryCleanerWorkflow,
    TagInfo,
    normalize_component,
    parse_filename,
    parse_track_number,
    render_template,
    resolve_collision,
)


def test_parse_filename_handles_component_counts(tmp_path):
    four = parse_filename(tmp_path / "Boards of Canada - Geogaddi - 07 - 1969.mp3")
    assert four == TagInfo(artist="Boards of Canada", album="Geogaddi", title="1969", track="07")

    three = parse_filename(tmp_path / "Boards of Canada - 07 - 1969.mp3")
    assert three.artist == "Boards of Canada"
    assert three.track == "07"
    assert three.title == "1969"

    two = parse_filename(tmp_path / "07 - 1969.mp3")
    assert two.artist == "Unknown Artist"
    assert two.track == "07"
    assert two.title == "1969"

    one = parse_filename(tmp_path / "1969.mp3")
    assert one.title == "1969"
    assert one.track == "00"


def test_parse_track_number_extracts_digits():
    assert parse_track_number("07") == 7
    assert parse_track_number("Track 12") == 12
    assert parse_track_number("no digits") is None


def test_normalize_component_strips_forbidden_characters():
    assert normalize_component('AC/DC: "Best*Of"', "Unknown Artist") == "AC-DC- -Best-Of-"
    assert normalize_component("   ", "Unknown Artist") == "Unknown Artist"


def test_render_template_builds_sanitized_relative_path():
    tag_info = TagInfo(artist="AC/DC", album="Back in Black", title="Hells Bells", track="01")
    rendered = render_template("{artist}/{album}/{track} - {title}", tag_info)
    assert rendered == Path("AC-DC/Back in Black/01 - Hells Bells")


def test_resolve_collision_increments_suffix(tmp_path):
    existing = tmp_path / "01 - Song.mp3"
    existing.write_bytes(b"x")
    resolved, collision = resolve_collision(existing, set())
    assert collision is True
    assert resolved.name == "01 - Song (1).mp3"


def test_build_plan_moves_and_skips_organized_files(tmp_path):
    library = tmp_path / "library"
    organized = library / "Unknown Artist" / "Unknown Album"
    organized.mkdir(parents=True)
    loose = library / "Boards of Canada - Geogaddi - 07 - 1969.mp3"
    loose.write_bytes(b"audio")
    already = organized / "00 - 1969.mp3"
    already.write_bytes(b"audio")

    workflow = LibraryCleanerWorkflow()
    plan = workflow.build_plan(
        {
            "library_path": str(library),
            "destination_root": str(library),
            "template": "{artist}/{album}/{track} - {title}",
            "extensions": ".mp3",
            "use_ffprobe": "false",
        }
    )

    assert plan.skipped == [already]
    assert len(plan.moves) == 1
    move = plan.moves[0]
    assert move.source == loose
    assert move.destination == library / "Boards of Canada" / "Geogaddi" / "07 - 1969.mp3"


def test_apply_moves_files_and_writes_rollback_scripts(tmp_path):
    library = tmp_path / "library"
    library.mkdir()
    loose = library / "Artist - Album - 01 - Song.mp3"
    loose.write_bytes(b"audio")

    workflow = LibraryCleanerWorkflow()
    options = {
        "library_path": str(library),
        "destination_root": str(library),
        "template": "{artist}/{album}/{track} - {title}",
        "extensions": ".mp3",
        "use_ffprobe": "false",
    }
    plan = workflow.build_plan(options)
    result = workflow.apply(options, plan)

    destination = library / "Artist" / "Album" / "01 - Song.mp3"
    assert not loose.exists()
    assert destination.exists()
    assert ("Moves completed", "1") in result.summary_items
    assert result.rollback_script is not None and result.rollback_script.exists()
    assert result.rollback_powershell_script is not None
    assert result.rollback_powershell_script.exists()
    content = result.rollback_script.read_text(encoding="utf-8")
    assert str(destination) in content
    assert str(loose) in content
