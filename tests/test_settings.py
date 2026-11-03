"""Settings store + API, internal-IP classification, and snapshot annotations."""

from __future__ import annotations

import asyncio
import json
import time

import pytest
from fastapi.testclient import TestClient

from netplot.session import Hop, SessionManager
from netplot.settings import (
    DEFAULT_INTERNAL_CIDRS,
    MAX_SETTINGS_FILE_BYTES,
    SETTINGS_FILENAME,
    default_settings,
    load_settings,
    make_classifier,
    normalize_cidrs,
    normalize_influx,
    save_settings,
)
from netplot.web import SettingsRequest, create_app

# ------------------------------------------------------------ store: CIDRs


def test_load_settings_missing_file_returns_defaults(tmp_path) -> None:
    settings = load_settings(tmp_path)
    assert settings["internal_cidrs"] == list(DEFAULT_INTERNAL_CIDRS)
    # No presets: the influx overlay is fully blank until configured.
    assert settings["influx"] == {
        "url": "",
        "database": "",
        "interface": "",
        "host_tag": "",
    }


def test_save_then_load_roundtrips(tmp_path) -> None:
    settings = default_settings()
    settings["internal_cidrs"] = ["100.64.0.0/10", "fc00::/7"]
    settings["influx"] = {
        "url": "http://influx-host:8086",
        "database": "telegraf",
        "interface": "wan",
        "host_tag": "",
    }
    save_settings(tmp_path, settings)
    loaded = load_settings(tmp_path)
    assert loaded["internal_cidrs"] == ["100.64.0.0/10", "fc00::/7"]
    assert loaded["influx"]["url"] == "http://influx-host:8086"
    assert loaded["influx"]["interface"] == "wan"


def test_load_settings_corrupt_file_falls_back_to_defaults(tmp_path) -> None:
    (tmp_path / SETTINGS_FILENAME).write_text("{not json", encoding="utf-8")
    settings = load_settings(tmp_path)
    assert settings == default_settings()


def test_load_settings_invalid_values_fall_back_to_defaults(tmp_path) -> None:
    (tmp_path / SETTINGS_FILENAME).write_text(
        json.dumps({"version": 1, "internal_cidrs": ["300.0.0.0/8"], "influx": {}}),
        encoding="utf-8",
    )
    assert load_settings(tmp_path) == default_settings()


def test_load_settings_oversized_file_falls_back_to_defaults(tmp_path) -> None:
    path = tmp_path / SETTINGS_FILENAME
    path.write_bytes(b'{"version": 1}' + b" " * (MAX_SETTINGS_FILE_BYTES + 1))
    assert path.stat().st_size > MAX_SETTINGS_FILE_BYTES
    assert load_settings(tmp_path) == default_settings()


def test_load_settings_file_exactly_at_limit_still_loads(tmp_path) -> None:
    """Boundary preserved: a valid file of exactly the limit (JSON
    tolerates the trailing-space padding) loads normally."""
    doc = json.dumps({"version": 1, "internal_cidrs": ["10.0.0.0/8"], "influx": {}})
    payload = doc.encode("utf-8").ljust(MAX_SETTINGS_FILE_BYTES, b" ")
    path = tmp_path / SETTINGS_FILENAME
    path.write_bytes(payload)
    assert path.stat().st_size == MAX_SETTINGS_FILE_BYTES
    loaded = load_settings(tmp_path)
    assert loaded["internal_cidrs"] == ["10.0.0.0/8"]
    assert loaded["influx"] == default_settings()["influx"]


def test_load_settings_caps_read_when_file_grows_after_stat(tmp_path, monkeypatch) -> None:
    """TOCTOU: if the file grows between the stat gate and the read (the
    stat is patched to under-report), at most limit + 1 bytes are ever
    pulled in and the file is still rejected with defaults."""
    import os
    import pathlib

    (tmp_path / SETTINGS_FILENAME).write_bytes(
        b'{"version": 1}' + b" " * (MAX_SETTINGS_FILE_BYTES + 4096)
    )

    real_open = pathlib.Path.open
    read_sizes = {"max": 0}

    class _SpyFile:
        def __init__(self, fh):
            self._fh = fh

        def read(self, size=-1):
            data = self._fh.read(size)
            read_sizes["max"] = max(read_sizes["max"], len(data))
            return data

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return self._fh.__exit__(*exc)

    def spying_open(self, *args, **kwargs):
        fh = real_open(self, *args, **kwargs)
        mode = args[0] if args else kwargs.get("mode", "r")
        return _SpyFile(fh) if "b" in mode else fh

    def shrunken_stat(self, **kwargs):
        # Only st_size is consulted; pretend the gate saw a tiny file.
        return os.stat_result((0, 0, 0, 0, 0, 0, 10, 0, 0, 0))

    monkeypatch.setattr(pathlib.Path, "open", spying_open)
    monkeypatch.setattr(pathlib.Path, "stat", shrunken_stat)
    assert load_settings(tmp_path) == default_settings()
    # The read path was exercised, but the pull was capped.
    assert 0 < read_sizes["max"] <= MAX_SETTINGS_FILE_BYTES + 1


def test_load_settings_deeply_nested_file_falls_back_to_defaults(tmp_path) -> None:
    """A pathologically nested (yet size-legal) file must not crash boot.

    Tens of thousands of nested brackets fit inside the 64 KiB size
    cap. A depth beyond the C JSON parser's recursion budget — which
    varies by interpreter build and version — makes ``json.loads``
    raise ``RecursionError``, which is NOT a ``ValueError``;
    ``load_settings`` runs inside ``create_app`` at boot, where an
    escape would crash the process. The contract "never raises" must
    hold for every unusable file, whatever exception class the parser
    picks: on a build with more headroom the same file parses and is
    rejected as "root is not an object" instead — the assertion covers
    both outcomes.
    """
    (tmp_path / SETTINGS_FILENAME).write_text(
        "[" * 16000 + "]" * 16000, encoding="utf-8"
    )
    assert (tmp_path / SETTINGS_FILENAME).stat().st_size < MAX_SETTINGS_FILE_BYTES
    assert load_settings(tmp_path) == default_settings()


def test_normalize_cidrs_normalizes_host_bits_and_dedupes() -> None:
    assert normalize_cidrs(["10.0.1.5/8", "10.0.0.0/8", "192.168.1.0/24"]) == [
        "10.0.0.0/8",
        "192.168.1.0/24",
    ]


def test_normalize_cidrs_accepts_empty_list() -> None:
    assert normalize_cidrs([]) == []


@pytest.mark.parametrize("bad", ["300.0.0.0/8", "not-a-cidr", "10.0.0.0/8/9", 42, None])
def test_normalize_cidrs_rejects_invalid(bad) -> None:
    with pytest.raises(ValueError):
        normalize_cidrs([bad])


def test_normalize_cidrs_caps_list_length() -> None:
    with pytest.raises(ValueError):
        normalize_cidrs([f"10.{i}.0.0/16" for i in range(65)])


# -------------------------------------------------------- classification


def test_classifier_matches_defaults_and_rejects_garbage() -> None:
    classify = make_classifier(list(DEFAULT_INTERNAL_CIDRS))
    assert classify("10.1.2.3") is True
    assert classify("172.31.255.1") is True
    assert classify("192.168.0.1") is True
    # RFC4193 ULA is the full fc00::/7 block: both fc00:: and fd00::
    # generated addresses classify as internal.
    assert classify("fd12:3456::1") is True
    assert classify("fc00::1") is True
    assert classify("8.8.8.8") is False
    assert classify("172.32.0.1") is False  # just outside 172.16/12
    assert classify("fe80::1") is False  # link-local, not ULA
    assert classify("2001:db8::1") is False  # global, not ULA
    assert classify(None) is False
    assert classify("") is False
    assert classify("router.lan") is False  # hostnames never classify


def test_classifier_honors_custom_ranges() -> None:
    classify = make_classifier(["100.64.0.0/10"])
    assert classify("100.64.0.1") is True
    assert classify("100.127.255.255") is True
    assert classify("100.128.0.1") is False
    assert classify("10.0.0.1") is False  # RFC1918 no longer internal


# ---------------------------------------------------------- influx validation


def test_normalize_influx_accepts_plain_http_url() -> None:
    out = normalize_influx(
        {"url": "http://influx.example:8086/", "database": "telegraf", "interface": "eth0", "host_tag": ""}
    )
    assert out == {
        "url": "http://influx.example:8086/",
        "database": "telegraf",
        "interface": "eth0",
        "host_tag": "",
    }


def test_normalize_influx_requires_database_and_interface_with_url() -> None:
    out = normalize_influx(
        {"url": "http://h:8086", "database": "telegraf", "interface": "wan", "host_tag": ""}
    )
    assert out["database"] == "telegraf"
    with pytest.raises(ValueError, match="interface is required"):
        normalize_influx({"url": "http://h:8086", "database": "d", "interface": "", "host_tag": ""})
    with pytest.raises(ValueError, match="database is required"):
        normalize_influx({"url": "http://h:8086", "database": "", "interface": "i", "host_tag": ""})
    # Unconfigured (empty url): everything may stay blank — no preset/fallback.
    assert normalize_influx({"url": "", "database": "", "interface": "", "host_tag": ""}) == {
        "url": "",
        "database": "",
        "interface": "",
        "host_tag": "",
    }


@pytest.mark.parametrize(
    "url",
    [
        "ftp://host:8086",
        "http://user:pass@host:8086",
        "http://user@host",
        "http://host:8086#frag",
        "http://host:8086?q=1",
        "http://host:8086/?db=other",
        "http://",
        "not a url",
    ],
)
def test_normalize_influx_rejects_unsafe_urls(url: str) -> None:
    with pytest.raises(ValueError):
        normalize_influx({"url": url, "database": "d", "interface": "eth0", "host_tag": ""})


@pytest.mark.parametrize("field", ["database", "interface", "host_tag"])
@pytest.mark.parametrize("bad", ["has space", "quo'te", "back\\slash", "semi;colon"])
def test_normalize_influx_rejects_unsafe_identifiers(field: str, bad: str) -> None:
    payload = {"url": "http://h:8086", "database": "d", "interface": "i", "host_tag": ""}
    payload[field] = bad
    with pytest.raises(ValueError):
        normalize_influx(payload)


# ----------------------------------------------------------------- API


def _client(tmp_path) -> TestClient:
    return TestClient(create_app(data_dir=tmp_path), base_url="http://localhost")


def test_get_internal_settings_defaults(tmp_path) -> None:
    with _client(tmp_path) as client:
        r = client.get("/api/settings/internal")
        assert r.status_code == 200
        assert r.json() == {"cidrs": list(DEFAULT_INTERNAL_CIDRS)}


def test_put_internal_settings_roundtrip_and_persistence(tmp_path) -> None:
    with _client(tmp_path) as client:
        r = client.put("/api/settings/internal", json={"cidrs": ["100.64.0.0/10"]})
        assert r.status_code == 200
        assert r.json() == {"cidrs": ["100.64.0.0/10"]}
        assert client.get("/api/settings/internal").json() == {"cidrs": ["100.64.0.0/10"]}
    # Persisted: a fresh app over the same data dir reads it back.
    with _client(tmp_path) as client:
        assert client.get("/api/settings/internal").json() == {"cidrs": ["100.64.0.0/10"]}


def test_put_internal_settings_422_on_invalid_cidr(tmp_path) -> None:
    with _client(tmp_path) as client:
        r = client.put("/api/settings/internal", json={"cidrs": ["10.0.0.0/8", "bogus"]})
        assert r.status_code == 422
        assert "bogus" in r.json()["detail"]
        # Untouched on failure.
        assert client.get("/api/settings/internal").json() == {
            "cidrs": list(DEFAULT_INTERNAL_CIDRS)
        }


def test_get_put_influx_settings_roundtrip(tmp_path) -> None:
    with _client(tmp_path) as client:
        # Defaults are fully blank (no database preset).
        assert client.get("/api/settings/influx").json() == {
            "url": "",
            "database": "",
            "interface": "",
            "host_tag": "",
        }
        r = client.put(
            "/api/settings/influx",
            json={"url": "http://influx-host:8086", "database": "telegraf", "interface": "wan"},
        )
        assert r.status_code == 200
        assert r.json() == {
            "url": "http://influx-host:8086",
            "database": "telegraf",
            "interface": "wan",
            "host_tag": "",
        }
        assert client.get("/api/settings/influx").json()["url"] == "http://influx-host:8086"


def test_put_influx_settings_422_without_database(tmp_path) -> None:
    with _client(tmp_path) as client:
        r = client.put(
            "/api/settings/influx",
            json={"url": "http://influx-host:8086", "database": "", "interface": "wan"},
        )
        assert r.status_code == 422
        assert "database is required" in r.json()["detail"]


def test_put_influx_settings_422_on_userinfo_url(tmp_path) -> None:
    with _client(tmp_path) as client:
        r = client.put(
            "/api/settings/influx",
            json={"url": "http://u:p@h:8086", "database": "d", "interface": "i"},
        )
        assert r.status_code == 422
        assert "credentials" in r.json()["detail"]


# ------------------------------------------ combined transactional save


def _good_influx() -> dict:
    return {
        "url": "http://influx-host:8086",
        "database": "telegraf",
        "interface": "wan",
        "host_tag": "",
    }


def test_combined_put_applies_both_sections_and_persists(tmp_path) -> None:
    with _client(tmp_path) as client:
        r = client.put(
            "/api/settings",
            json={"cidrs": ["100.64.0.0/10"], "influx": _good_influx()},
        )
        assert r.status_code == 200
        assert r.json() == {"cidrs": ["100.64.0.0/10"], "influx": _good_influx()}
        assert client.get("/api/settings/internal").json() == {"cidrs": ["100.64.0.0/10"]}
        assert client.get("/api/settings/influx").json() == _good_influx()
    # Persisted: a fresh app over the same data dir reads it back.
    with _client(tmp_path) as client:
        assert client.get("/api/settings/internal").json() == {"cidrs": ["100.64.0.0/10"]}
        assert client.get("/api/settings/influx").json() == _good_influx()


def test_combined_put_422_leaves_both_sections_unchanged(tmp_path) -> None:
    with _client(tmp_path) as client:
        # Seed one prior good save so "unchanged" means the old values,
        # not just the defaults.
        seed = client.put(
            "/api/settings",
            json={"cidrs": ["10.0.0.0/8"], "influx": _good_influx()},
        )
        assert seed.status_code == 200
        # Valid influx, INVALID cidr: the whole candidate is rejected.
        r = client.put(
            "/api/settings",
            json={"cidrs": ["bogus"], "influx": _good_influx()},
        )
        assert r.status_code == 422
        assert client.get("/api/settings/internal").json() == {"cidrs": ["10.0.0.0/8"]}
        assert client.get("/api/settings/influx").json() == _good_influx()
    assert load_settings(tmp_path)["internal_cidrs"] == ["10.0.0.0/8"]


def test_combined_put_failed_persist_leaves_runtime_and_disk_intact(
    tmp_path, monkeypatch
) -> None:
    import netplot.web as webmod

    calls = {"n": 0}
    real_save = webmod.save_settings

    def flaky_save(data_dir, settings):
        calls["n"] += 1
        if calls["n"] > 1:  # seed save succeeds, the next one fails
            raise OSError("disk full")
        real_save(data_dir, settings)

    monkeypatch.setattr(webmod, "save_settings", flaky_save)
    app = create_app(data_dir=tmp_path)
    client = TestClient(app, base_url="http://localhost", raise_server_exceptions=False)
    with client:
        seed = client.put(
            "/api/settings",
            json={"cidrs": ["10.0.0.0/8"], "influx": _good_influx()},
        )
        assert seed.status_code == 200
        r = client.put(
            "/api/settings",
            json={"cidrs": ["100.64.0.0/10"], "influx": _good_influx()},
        )
        assert r.status_code == 500
        # Runtime state, classifier, and disk all still hold the seed save.
        assert client.get("/api/settings/internal").json() == {"cidrs": ["10.0.0.0/8"]}
        assert load_settings(tmp_path)["internal_cidrs"] == ["10.0.0.0/8"]
        manager: SessionManager = client.app.state.session_manager
        session = manager.create("example.com")
        session.route_state = "ready"
        session.target_ip = "10.2.3.4"
        snap = client.get(f"/api/traces/{session.id}/snapshot").json()
        assert snap["target_internal"] is True  # 10/8 still classified internal


def test_concurrent_combined_puts_serialize_no_interleaving(tmp_path, monkeypatch) -> None:
    """Two overlapping combined PUTs never interleave sections.

    The persist must be serialized (no overlapping save calls) and the
    final disk state must be ONE complete candidate — never writer A's
    cidrs married to writer B's influx.
    """

    import netplot.web as webmod

    overlap = {"active": 0, "seen": False}
    real_save = webmod.save_settings

    def tracking_save(data_dir, settings):
        overlap["active"] += 1
        if overlap["active"] > 1:
            overlap["seen"] = True
        time.sleep(0.02)  # widen the window for an unsynchronized writer
        overlap["active"] -= 1
        real_save(data_dir, settings)

    monkeypatch.setattr(webmod, "save_settings", tracking_save)
    app = create_app(data_dir=tmp_path)
    endpoint = next(
        route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/settings"
    )

    async def run():
        return await asyncio.gather(
            endpoint(
                SettingsRequest(
                    cidrs=["10.0.0.0/8"],
                    influx={"url": "http://a:8086", "database": "da", "interface": "ia"},
                )
            ),
            endpoint(
                SettingsRequest(
                    cidrs=["172.16.0.0/12"],
                    influx={"url": "http://b:8086", "database": "db", "interface": "ib"},
                )
            ),
        )

    asyncio.run(run())
    assert not overlap["seen"]
    final = load_settings(tmp_path)
    # One complete writer won, not a mix of A's cidrs with B's influx.
    a_won = final["internal_cidrs"] == ["10.0.0.0/8"] and final["influx"]["url"] == "http://a:8086"
    b_won = final["internal_cidrs"] == ["172.16.0.0/12"] and final["influx"]["url"] == "http://b:8086"
    assert a_won or b_won


# ------------------------------------------- snapshot internal annotations


def _annotated_snapshot(tmp_path, cidrs, hops, target_ip="8.8.8.8"):
    """Create an app, PUT ``cidrs``, register a (not started) session with
    ``hops``, and return its snapshot payload."""
    with _client(tmp_path) as client:
        if cidrs is not None:
            put = client.put("/api/settings/internal", json={"cidrs": cidrs})
            assert put.status_code == 200
        manager: SessionManager = client.app.state.session_manager
        session = manager.create("example.com")
        session.route_state = "ready"
        session.target_ip = target_ip
        session.target_dns = "example.com"
        session.hops = [Hop(index=h.index, ip=h.ip, dns=h.dns) for h in hops]
        return client.get(f"/api/traces/{session.id}/snapshot").json()


def test_snapshot_annotates_internal_hops_with_defaults(tmp_path) -> None:
    snap = _annotated_snapshot(
        tmp_path,
        None,
        [
            Hop(index=1, ip="192.168.1.1", dns="router.lan"),
            Hop(index=2, ip="1.1.1.1", dns="one.one.one.one"),
        ],
    )
    by_index = {h["index"]: h for h in snap["hops"]}
    assert by_index[1]["internal"] is True
    assert by_index[2]["internal"] is False
    assert snap["target_internal"] is False


def test_snapshot_annotates_target_internal(tmp_path) -> None:
    snap = _annotated_snapshot(
        tmp_path,
        None,
        [Hop(index=1, ip="1.1.1.1", dns=None)],
        target_ip="10.2.3.4",
    )
    assert snap["target_internal"] is True
    assert snap["hops"][0]["internal"] is False


def test_snapshot_internal_follows_custom_ranges(tmp_path) -> None:
    snap = _annotated_snapshot(
        tmp_path,
        ["100.64.0.0/10"],
        [Hop(index=1, ip="10.0.0.1", dns=None), Hop(index=2, ip="100.64.0.1", dns=None)],
    )
    by_index = {h["index"]: h for h in snap["hops"]}
    assert by_index[1]["internal"] is False  # RFC1918 no longer internal
    assert by_index[2]["internal"] is True


def test_hop_snapshot_carries_internal_flag(tmp_path) -> None:
    with _client(tmp_path) as client:
        manager: SessionManager = client.app.state.session_manager
        session = manager.create("example.com")
        session.route_state = "ready"
        session.target_ip = "8.8.8.8"
        session.hops = [Hop(index=1, ip="172.16.5.5", dns="gw.lan")]
        body = client.get(f"/api/traces/{session.id}/snapshot?hop=1").json()
        assert body["internal"] is True


def test_masking_is_display_only_csv_and_export_unchanged(tmp_path) -> None:
    with _client(tmp_path) as client:
        manager: SessionManager = client.app.state.session_manager
        session = manager.create("example.com")
        session.route_state = "ready"
        session.target_ip = "8.8.8.8"
        from netplot.session import Sample

        hop = Hop(index=1, ip="192.168.1.1", dns="router.lan")
        hop.samples.append(Sample(t=time.time(), rtt_ms=5.0))
        session.hops = [hop]
        csv_text = client.get(f"/api/traces/{session.id}/export?format=csv").text
        assert "router.lan" in csv_text  # export never masked
        assert "Internal host" not in csv_text
