from __future__ import annotations

from pathlib import Path

_LOADED = False


def load_project_env() -> None:
    global _LOADED
    if _LOADED:
        return
    _LOADED = True
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return

    base_dir = Path(__file__).resolve().parents[2]
    # Load local secrets first; process env still wins because override=False.
    for name in (".env.local", ".env"):
        path = base_dir / name
        if path.exists():
            load_dotenv(path, override=False)
