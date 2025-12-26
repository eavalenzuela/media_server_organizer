import math
import time
import types
from pathlib import Path

import pytest
from pydub import AudioSegment

import src.media_server_manager as msm
from src.media_server_manager import LibraryDB, MediaServerApp


class DummyVar:
    def __init__(self, value=None) -> None:
        self._value = value

    def get(self):
        return self._value

    def set(self, value) -> None:
        self._value = value


class DummyProgress:
    def __init__(self) -> None:
        self.config: dict[str, object] = {}

    def configure(self, **kwargs) -> None:
        self.config.update(kwargs)

    def cget(self, key: str):
        return self.config.get(key)


class DummyFrame:
    def __init__(self) -> None:
        self._visible = False

    def grid(self, *args, **kwargs) -> None:
        self._visible = True

    def grid_remove(self, *args, **kwargs) -> None:
        self._visible = False

    def winfo_ismapped(self) -> bool:
        return self._visible


class DummyRoot:
    def __init__(self) -> None:
        self.after_calls: list[tuple[str, int, object]] = []
        self.cancelled: list[str] = []

    def after(self, delay: int, callback):
        job = f"job-{len(self.after_calls) + 1}"
        self.after_calls.append((job, delay, callback))
        return job

    def after_cancel(self, job: str) -> None:
        self.cancelled.append(job)

    def withdraw(self) -> None:
        pass

    def destroy(self) -> None:
        pass


@pytest.fixture
def db():
    database = LibraryDB(":memory:")
    yield database
    database.close()


@pytest.fixture
def app(db):
    root = DummyRoot()
    app = MediaServerApp.__new__(MediaServerApp)
    app.root = root
    app.db = db
    app.audio_segment = None
    app.audio_segment_path = None
    app.audio_play_obj = None
    app.audio_path = None
    app.audio_paused_position_ms = 0
    app.audio_playback_start_time = None
    app.audio_progress_job = None
    app.audio_is_paused = False
    app.audio_title_var = DummyVar("No audio loaded")
    app.audio_time_var = DummyVar("00:00 / 00:00")
    app.audio_volume = DummyVar(100.0)
    app.audio_progress = DummyProgress()
    app.audio_player_frame = DummyFrame()

    def _toggle_audio_player(visible: bool) -> None:
        if visible:
            app.audio_player_frame.grid()
        else:
            app.audio_player_frame.grid_remove()

    app._toggle_audio_player = _toggle_audio_player  # type: ignore[method-assign]
    return app


@pytest.fixture
def messagebox_spy(monkeypatch):
    calls: dict[str, list[tuple[str, str]]] = {"error": [], "info": []}

    def fake_error(title: str, message: str) -> None:
        calls["error"].append((title, message))

    def fake_info(title: str, message: str) -> None:
        calls["info"].append((title, message))

    monkeypatch.setattr(
        msm,
        "messagebox",
        types.SimpleNamespace(showerror=fake_error, showinfo=fake_info),
    )
    return calls


@pytest.fixture
def audio_loader(monkeypatch):
    segment = AudioSegment.silent(duration=1000)
    loaded_paths: list[str] = []

    def fake_from_file(path: str):
        loaded_paths.append(path)
        return segment

    monkeypatch.setattr(msm.AudioSegment, "from_file", staticmethod(fake_from_file))
    return loaded_paths, segment


class StubPlayObject:
    def __init__(self, playing: bool = True) -> None:
        self.playing = playing
        self.stopped = False

    def is_playing(self) -> bool:
        return self.playing

    def stop(self) -> None:
        self.playing = False
        self.stopped = True


@pytest.fixture
def play_buffer_stub(monkeypatch):
    calls: list[dict[str, object]] = []

    def fake_play_buffer(*args, **kwargs):
        obj = StubPlayObject()
        calls.append({"args": args, "kwargs": kwargs, "obj": obj})
        return obj

    monkeypatch.setattr(msm.simpleaudio, "play_buffer", fake_play_buffer)
    return calls


def test_play_audio_file_rejects_invalid_inputs(app, messagebox_spy, tmp_path):
    app.audio_path = "existing.mp3"
    app.audio_title_var.set("Existing")

    missing_path = tmp_path / "missing.mp3"
    app._play_audio_file(str(missing_path))

    assert messagebox_spy["error"] == [("Play Audio", "Selected audio file is unavailable.")]
    assert app.audio_path == "existing.mp3"
    assert app.audio_title_var.get() == "Existing"
    assert app.audio_play_obj is None

    text_path = tmp_path / "note.txt"
    text_path.write_text("content")
    messagebox_spy["error"].clear()
    app._play_audio_file(str(text_path))

    assert messagebox_spy["info"] == [("Play Audio", "The selected item is not an audio file.")]
    assert app.audio_path == "existing.mp3"
    assert app.audio_play_obj is None


def test_start_audio_playback_loads_and_schedules(app, audio_loader, play_buffer_stub, monkeypatch):
    loaded_paths, segment = audio_loader
    after_calls: list[tuple[int, object]] = []
    cancelled: list[str] = []

    def fake_after(delay: int, callback):
        after_calls.append((delay, callback))
        return f"job-{len(after_calls)}"

    monkeypatch.setattr(app.root, "after", fake_after)
    monkeypatch.setattr(app.root, "after_cancel", lambda job: cancelled.append(job))

    applied_segments: list[AudioSegment] = []

    def fake_apply_volume(seg: AudioSegment) -> AudioSegment:
        applied_segments.append(seg)
        return seg

    monkeypatch.setattr(app, "_apply_volume", fake_apply_volume)

    app.audio_path = "song.mp3"
    app._start_audio_playback()
    app._start_audio_playback()

    assert loaded_paths == ["song.mp3"]
    assert applied_segments
    assert play_buffer_stub[-1]["obj"].is_playing() is True
    assert after_calls, "root.after should be invoked to schedule progress updates"
    assert app.audio_progress_job == f"job-{len(after_calls)}"


def test_pause_audio_preserves_position(app, monkeypatch):
    app.audio_segment = AudioSegment.silent(duration=1000)
    app.audio_play_obj = StubPlayObject(playing=True)
    app.audio_paused_position_ms = 100
    app.audio_playback_start_time = 100.0
    monkeypatch.setattr(time, "time", lambda: 100.25)

    app._pause_audio()

    assert app.audio_is_paused is True
    assert app.audio_play_obj is None
    assert 300 <= app.audio_paused_position_ms <= 400
    assert app.audio_playback_start_time is None


def test_resume_or_restart_audio_behaviour(app, monkeypatch):
    app.audio_path = "song.mp3"
    app.audio_paused_position_ms = 500
    app.audio_is_paused = True

    paused_calls: list[int] = []
    monkeypatch.setattr(app, "_start_audio_playback", lambda: paused_calls.append(app.audio_paused_position_ms))
    app._resume_or_restart_audio()
    assert paused_calls == [500]

    app.audio_is_paused = False
    app.audio_paused_position_ms = 350
    restarted_calls: list[int] = []
    monkeypatch.setattr(app, "_start_audio_playback", lambda: restarted_calls.append(app.audio_paused_position_ms))

    app._resume_or_restart_audio()

    assert restarted_calls == [0]
    assert app.audio_paused_position_ms == 0


def test_stop_audio_clears_state_and_hides_player(app, monkeypatch):
    app.audio_play_obj = StubPlayObject()
    app.audio_segment = AudioSegment.silent(duration=1000)
    app.audio_path = "song.mp3"
    app.audio_title_var.set("Song")
    app.audio_progress_job = "job-1"
    app._toggle_audio_player(True)

    cancelled: list[str] = []
    monkeypatch.setattr(app.root, "after_cancel", lambda job: cancelled.append(job))

    app._stop_audio()

    assert cancelled == ["job-1"]
    assert app.audio_play_obj is None
    assert app.audio_segment is None
    assert app.audio_path is None
    assert app.audio_title_var.get() == "No audio loaded"
    assert app.audio_time_var.get() == "00:00 / 00:00"
    assert not app.audio_player_frame.winfo_ismapped()


def test_update_audio_progress_stops_when_not_playing(app, monkeypatch):
    stopped = False

    def fake_stop():
        nonlocal stopped
        stopped = True

    app.audio_is_paused = False
    app.audio_play_obj = StubPlayObject(playing=False)
    monkeypatch.setattr(app, "_stop_audio", fake_stop)

    app._update_audio_progress()

    assert stopped is True
    assert app.audio_progress_job is None


def test_apply_volume_clamps_and_adjusts_gain(app):
    class GainSpySegment:
        def __init__(self) -> None:
            self.add_calls: list[float] = []
            self.sub_calls: list[float] = []

        def __add__(self, gain: float):
            self.add_calls.append(gain)
            return self

        def __sub__(self, reduction: float):
            self.sub_calls.append(reduction)
            return self

    segment = GainSpySegment()

    app.audio_volume.set(-20)
    muted = app._apply_volume(segment)
    assert muted is segment
    assert segment.sub_calls == [120]

    app.audio_volume.set(50)
    segment.sub_calls.clear()
    segment.add_calls.clear()
    half_volume = app._apply_volume(segment)
    assert half_volume is segment
    assert math.isclose(segment.add_calls[-1], 20 * math.log10(0.5), abs_tol=0.01)

    app.audio_volume.set(150)
    segment.add_calls.clear()
    full_volume = app._apply_volume(segment)
    assert full_volume is segment
    assert segment.add_calls[-1] == 0


def test_zero_length_segment_surfaces_error(app, messagebox_spy, monkeypatch):
    empty_segment = AudioSegment.silent(duration=0)

    monkeypatch.setattr(msm.AudioSegment, "from_file", staticmethod(lambda path: empty_segment))
    monkeypatch.setattr(app, "_apply_volume", lambda seg: seg)
    monkeypatch.setattr(app.root, "after", lambda *args, **kwargs: None)
    monkeypatch.setattr(app.root, "after_cancel", lambda *args, **kwargs: None)
    monkeypatch.setattr(msm.simpleaudio, "play_buffer", lambda *args, **kwargs: StubPlayObject())

    audio_path = Path("empty.mp3")
    app.audio_path = str(audio_path)

    app._start_audio_playback()

    assert messagebox_spy["error"] == [("Play Audio", "Audio file contains no playable data.")]
    assert app.audio_play_obj is None
    assert app.audio_progress_job is None
    assert app.audio_playback_start_time is None
