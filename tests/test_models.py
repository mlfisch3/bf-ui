from ui.models import thread_id_for


def test_thread_id_stable() -> None:
    first = thread_id_for("Test Thread", "subforum.key")
    second = thread_id_for("Test Thread", "subforum.key")
    assert first == second
