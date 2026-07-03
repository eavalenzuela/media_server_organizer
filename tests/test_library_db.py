from src.media_server_manager import LibraryDB


def _make_db(tmp_path) -> LibraryDB:
    return LibraryDB(str(tmp_path / "test.db"))


def test_search_items_escapes_like_wildcards(tmp_path):
    db = _make_db(tmp_path)
    try:
        library = db.add_library("Music", "local", str(tmp_path), None, None)
        cursor = db.connection.cursor()
        cursor.executemany(
            "INSERT INTO library_items (library_id, path, name, entry_type) VALUES (?, ?, ?, ?)",
            [
                (library.library_id, "/music/100% Hits.mp3", "100% Hits.mp3", "File"),
                (library.library_id, "/music/plain.mp3", "plain.mp3", "File"),
                (library.library_id, "/music/under_score.mp3", "under_score.mp3", "File"),
            ],
        )
        db.connection.commit()

        percent_results = db.search_items("100%")
        assert [row[2] for row in percent_results] == ["100% Hits.mp3"]

        underscore_results = db.search_items("under_")
        assert [row[2] for row in underscore_results] == ["under_score.mp3"]

        assert db.search_items("zzz-no-match") == []
    finally:
        db.close()


def test_delete_library_cleans_up_related_rows(tmp_path):
    db = _make_db(tmp_path)
    try:
        library = db.add_library("Music", "local", str(tmp_path), None, None)
        cursor = db.connection.cursor()
        cursor.execute(
            "INSERT INTO library_items (library_id, path, name, entry_type) VALUES (?, ?, ?, ?)",
            (library.library_id, "/music/song.mp3", "song.mp3", "File"),
        )
        db.connection.commit()
        db.upsert_audio_signature(
            path="/music/song.mp3",
            signature="sig",
            library_id=library.library_id,
        )

        db.delete_library(library.library_id)

        assert db.fetch_libraries() == []
        assert db.fetch_library_items(library.library_id) == []
        signature = db.fetch_audio_signature_by_path("/music/song.mp3")
        assert signature is not None
        assert signature.library_id is None
    finally:
        db.close()


def test_fetch_library_items_returns_sorted_rows(tmp_path):
    db = _make_db(tmp_path)
    try:
        library = db.add_library("Music", "local", str(tmp_path), None, None)
        other = db.add_library("Other", "local", str(tmp_path / "other"), None, None)
        cursor = db.connection.cursor()
        cursor.executemany(
            "INSERT INTO library_items (library_id, path, name, entry_type) VALUES (?, ?, ?, ?)",
            [
                (library.library_id, "/music/b.mp3", "b.mp3", "File"),
                (library.library_id, "/music/a.mp3", "a.mp3", "File"),
                (other.library_id, "/other/z.mp3", "z.mp3", "File"),
            ],
        )
        db.connection.commit()

        items = db.fetch_library_items(library.library_id)
        assert items == [
            ("/music/a.mp3", "a.mp3", "File"),
            ("/music/b.mp3", "b.mp3", "File"),
        ]
    finally:
        db.close()
