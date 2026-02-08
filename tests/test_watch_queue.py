from src.media_server_manager import EventCoalescingQueue, MediaServerApp


class FakeRoot:
    def __init__(self) -> None:
        self.after_calls: list[tuple[int, object]] = []
        self.cancelled: list[str] = []

    def after(self, delay: int, callback):
        token = f"job-{len(self.after_calls)+1}"
        self.after_calls.append((delay, callback))
        return token

    def after_cancel(self, token: str) -> None:
        self.cancelled.append(token)


class FakeBoolVar:
    def __init__(self, value: bool) -> None:
        self._value = value

    def get(self) -> bool:
        return self._value

    def set(self, value: bool) -> None:
        self._value = value


def test_event_queue_coalesces_paths_and_filters_extensions(tmp_path):
    queue = EventCoalescingQueue(debounce_seconds=1.0, extensions={".mp3"})
    allowed = tmp_path / "song.mp3"
    disallowed = tmp_path / "video.mkv"
    queue.enqueue([str(allowed), str(disallowed)], now=10.0)
    queue.enqueue([str(allowed)], now=10.2)

    assert queue.pop_ready(now=11.1) == []
    assert queue.pop_ready(now=11.2) == [str(allowed.resolve())]


def test_process_watch_queue_runs_ready_paths_and_reschedules(monkeypatch):
    app = MediaServerApp.__new__(MediaServerApp)
    app.root = FakeRoot()
    app.watch_enabled_var = FakeBoolVar(True)
    app.watch_processor_job = "existing"

    queue = EventCoalescingQueue(debounce_seconds=0.0, extensions=set())
    queue.enqueue(["/tmp/a.mp3", "/tmp/b.mp3"], now=0.0)
    app.watch_task_queue = queue

    processed: list[str] = []
    app._run_watch_workflows_for_path = lambda path: processed.append(path)

    app._process_watch_queue()

    assert processed == ["/tmp/a.mp3", "/tmp/b.mp3"]
    assert app.watch_processor_job == "job-1"
    assert app.root.after_calls and app.root.after_calls[0][0] == 500


def test_stop_watch_mode_stops_watcher_and_cancels_scheduled_job():
    app = MediaServerApp.__new__(MediaServerApp)
    app.root = FakeRoot()
    app.watch_enabled_var = FakeBoolVar(True)
    app.watch_status_var = type("Status", (), {"set": lambda self, value: None})()
    app.watch_processor_job = "job-9"

    class Watcher:
        def __init__(self):
            self.stopped = False

        def stop(self):
            self.stopped = True

    app.library_watcher = Watcher()

    app._stop_watch_mode()

    assert app.library_watcher.stopped is True
    assert app.root.cancelled == ["job-9"]
    assert app.watch_enabled_var.get() is False
    assert app.watch_processor_job is None
