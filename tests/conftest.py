import sys
import types
from typing import Any


class _StubAudioSegment:
    def __init__(self, duration_ms: int = 0, frame_rate: int = 44100, channels: int = 2, sample_width: int = 2, raw_data: bytes | None = None) -> None:
        self.duration_ms = max(0, int(duration_ms))
        self.frame_rate = frame_rate
        self.channels = channels
        self.sample_width = sample_width
        if raw_data is None:
            frame_count = int(self.frame_rate * (self.duration_ms / 1000))
            raw_data = bytes(frame_count * self.channels * self.sample_width)
        self.raw_data = raw_data

    def __len__(self) -> int:
        return self.duration_ms

    def __getitem__(self, key: Any) -> "_StubAudioSegment":
        if isinstance(key, slice):
            start = 0 if key.start is None else int(key.start)
            stop = self.duration_ms if key.stop is None else int(key.stop)
        else:
            start = int(key)
            stop = self.duration_ms
        start = max(0, start)
        stop = max(start, stop)
        return _StubAudioSegment(
            duration_ms=max(0, stop - start),
            frame_rate=self.frame_rate,
            channels=self.channels,
            sample_width=self.sample_width,
        )

    def __add__(self, _gain: float) -> "_StubAudioSegment":
        return _StubAudioSegment(
            duration_ms=self.duration_ms,
            frame_rate=self.frame_rate,
            channels=self.channels,
            sample_width=self.sample_width,
            raw_data=self.raw_data,
        )

    def __sub__(self, _reduction: float) -> "_StubAudioSegment":
        return _StubAudioSegment(
            duration_ms=self.duration_ms,
            frame_rate=self.frame_rate,
            channels=self.channels,
            sample_width=self.sample_width,
            raw_data=self.raw_data,
        )

    @classmethod
    def silent(cls, duration: int = 0) -> "_StubAudioSegment":
        return cls(duration_ms=duration)

    @classmethod
    def from_file(cls, _path: str) -> "_StubAudioSegment":
        return cls(duration_ms=1000)


try:
    import pydub as _pydub  # type: ignore
except ModuleNotFoundError:
    stub_module = types.ModuleType("pydub")
    stub_module.AudioSegment = _StubAudioSegment
    sys.modules["pydub"] = stub_module


class _StubPlayObject:
    def __init__(self) -> None:
        self._playing = False

    def is_playing(self) -> bool:
        return self._playing

    def stop(self) -> None:
        self._playing = False


def _stub_play_buffer(*_args, **_kwargs) -> _StubPlayObject:
    obj = _StubPlayObject()
    obj._playing = True
    return obj


try:
    import simpleaudio as _simpleaudio  # type: ignore
except ModuleNotFoundError:
    sa_module = types.ModuleType("simpleaudio")
    sa_module.PlayObject = _StubPlayObject
    sa_module.play_buffer = _stub_play_buffer
    sys.modules["simpleaudio"] = sa_module
