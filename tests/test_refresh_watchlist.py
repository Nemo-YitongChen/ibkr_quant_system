from pathlib import Path

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
