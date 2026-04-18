from __future__ import annotations

from threading import Lock

from app.models.schemas import ConversationTurn

_STORE: dict[str, list[ConversationTurn]] = {}
_LOCK = Lock()


def append_turns(conversation_id: str, turns: list[ConversationTurn]) -> None:
    if not conversation_id or not turns:
        return

    with _LOCK:
        existing = _STORE.setdefault(conversation_id, [])
        existing.extend(turns)


def get_turns(conversation_id: str) -> list[ConversationTurn]:
    if not conversation_id:
        return []

    with _LOCK:
        return list(_STORE.get(conversation_id, []))
