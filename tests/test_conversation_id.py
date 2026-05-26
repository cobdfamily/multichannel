from multichannel.lib.conversation_id import derive_conversation_id


def test_conversation_id_is_deterministic_for_same_provider_thread() -> None:
    first = derive_conversation_id("postmark", "thread-123")
    second = derive_conversation_id("postmark", "thread-123")

    assert first == second


def test_conversation_id_differs_for_different_threads() -> None:
    first = derive_conversation_id("postmark", "thread-123")
    second = derive_conversation_id("postmark", "thread-456")

    assert first != second


def test_missing_thread_id_returns_unique_random_uuid_each_call() -> None:
    first = derive_conversation_id("postmark", None)
    second = derive_conversation_id("postmark", None)

    assert first != second
