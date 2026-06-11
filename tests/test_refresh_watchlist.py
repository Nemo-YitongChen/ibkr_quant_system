from pathlib import Path

import yaml

from src.tools import refresh_watchlist


def test_source_config_value_prefers_repo_relative_path():
    path = refresh_watchlist.BASE_DIR / "config/watchlists/hk_top100_bluechip.yaml"
    assert refresh_watchlist._source_config_value(path) == "config/watchlists/hk_top100_bluechip.yaml"


def test_source_config_value_keeps_absolute_path_for_external_file(tmp_path: Path):
    external = tmp_path / "watchlist.yaml"
    external.write_text("name: external\n", encoding="utf-8")
    assert refresh_watchlist._source_config_value(external) == str(external.resolve())


def test_generated_at_value_reuses_existing_timestamp_when_symbols_are_unchanged(tmp_path: Path):
    out_path = tmp_path / "resolved.yaml"
    out_path.write_text(
        "generated_at: '2026-03-27 23:28:47'\nsymbols:\n  - 0001.HK\n  - 0002.HK\n",
        encoding="utf-8",
    )
    assert refresh_watchlist._generated_at_value(out_path, ["0001.HK", "0002.HK"]) == "2026-03-27 23:28:47"


def test_generated_at_value_refreshes_when_symbols_change(tmp_path: Path, monkeypatch):
    out_path = tmp_path / "resolved.yaml"
    out_path.write_text(
        "generated_at: '2026-03-27 23:28:47'\nsymbols:\n  - 0001.HK\n  - 0002.HK\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(refresh_watchlist.time, "strftime", lambda _fmt: "2026-03-28 10:00:00")
    assert refresh_watchlist._generated_at_value(out_path, ["0001.HK", "0003.HK"]) == "2026-03-28 10:00:00"


def test_refresh_preserves_last_known_good_when_all_dynamic_sources_fail(tmp_path: Path):
    config_path = tmp_path / "watchlist.yaml"
    out_path = tmp_path / "resolved.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "name": "hk",
                "target_n": 100,
                "manual_include": ["0700.HK", "9988.HK"],
                "sources": [{"url": "https://example.test/hsi"}],
            }
        ),
        encoding="utf-8",
    )
    original = "generated_at: old\ncount: 4\nsymbols:\n- 0700.HK\n- 9988.HK\n- 0005.HK\n- 0388.HK\n"
    out_path.write_text(original, encoding="utf-8")

    def fail_fetch(_url: str) -> str:
        raise ConnectionError("offline")

    payload, written, reason = refresh_watchlist.refresh_watchlist(
        config_path,
        out_path,
        fetcher=fail_fetch,
    )

    assert written is False
    assert reason == "all_dynamic_sources_failed"
    assert payload["symbols"] == ["0700.HK", "9988.HK", "0005.HK", "0388.HK"]
    assert out_path.read_text(encoding="utf-8") == original


def test_refresh_preserves_existing_on_partial_source_failure_with_severe_shrink(tmp_path: Path):
    config_path = tmp_path / "watchlist.yaml"
    out_path = tmp_path / "resolved.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "name": "hk",
                "target_n": 100,
                "manual_include": ["0700.HK"],
                "sources": [
                    {"url": "https://example.test/hsi"},
                    {"url": "https://example.test/hscei"},
                ],
                "min_existing_retention_ratio": 0.6,
            }
        ),
        encoding="utf-8",
    )
    existing_symbols = [f"{value:04d}.HK" for value in range(1, 11)]
    out_path.write_text(
        yaml.safe_dump({"generated_at": "old", "count": 10, "symbols": existing_symbols}),
        encoding="utf-8",
    )

    def partial_fetch(url: str) -> str:
        if url.endswith("hscei"):
            raise ConnectionError("offline")
        return "0001.HK 0002.HK"

    _, written, reason = refresh_watchlist.refresh_watchlist(
        config_path,
        out_path,
        fetcher=partial_fetch,
    )

    assert written is False
    assert reason.startswith("partial_source_failure_shrink:")
    assert yaml.safe_load(out_path.read_text(encoding="utf-8"))["symbols"] == existing_symbols


def test_refresh_replaces_watchlist_atomically_after_success(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "watchlist.yaml"
    out_path = tmp_path / "resolved.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "name": "hk",
                "target_n": 3,
                "manual_include": ["0700.HK"],
                "sources": [{"url": "https://example.test/hsi"}],
            }
        ),
        encoding="utf-8",
    )
    out_path.write_text("generated_at: old\ncount: 1\nsymbols:\n- 0700.HK\n", encoding="utf-8")
    monkeypatch.setattr(refresh_watchlist.time, "strftime", lambda _fmt: "2026-06-11 20:00:00")

    payload, written, reason = refresh_watchlist.refresh_watchlist(
        config_path,
        out_path,
        fetcher=lambda _url: "0005.HK 0388.HK",
    )

    assert written is True
    assert reason == ""
    assert payload["symbols"] == ["0700.HK", "0005.HK", "0388.HK"]
    assert payload["source_success_count"] == 1
    assert payload["source_failure_count"] == 0
    assert not list(tmp_path.glob("*.tmp"))
