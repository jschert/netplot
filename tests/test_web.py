from types import SimpleNamespace

from fastapi.testclient import TestClient

from netplot import cli, web


def test_root_dashboard_contains_controls() -> None:
    client = TestClient(web.app, base_url="http://localhost")

    response = client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    body = response.text
    # Target / interval / play-pause controls (PingPlotter layout).
    assert 'id="target"' in body
    assert 'id="interval"' in body
    # Slice 6b: the single Start/Pause/Resume toggle was split into separate
    # Play (#btn-play, Start/Resume) and Pause (#btn-pause) buttons.
    assert 'id="btn-play"' in body
    assert 'id="btn-pause"' in body
    assert 'id="start-stop"' not in body  # legacy toggle ID removed
    # Slice 6b: target history dropdown, Focus window dropdown, packet-type
    # dropdown, focus caption line.
    assert 'id="target-history"' in body
    assert 'id="focus-window"' in body
    assert 'id="packet-type"' in body
    assert 'id="focus-caption"' in body
    # Slice 4 additions: reset control + per-hop table.
    assert 'id="reset"' in body
    assert 'id="hop-table"' in body
    assert 'id="hop-table-body"' in body
    assert 'id="status-line"' in body
    assert 'id="route-banner"' in body
    # PingPlotter default column set.
    for column in ("Hop", "Count", "IP", "DNS", "Avg", "Min", "Cur", "PL%"):
        assert column in body
    # Slice 6 graph-column chrome: status column header, legend
    # with 100ms/200ms zone ticks, live axis-max label, connector SVG overlay.
    assert 'id="graph-legend"' in body
    assert 'id="graph-legend-bar"' in body
    assert 'id="graph-legend-green"' in body
    assert 'id="graph-legend-yellow"' in body
    assert 'id="graph-legend-red"' in body
    assert 'id="graph-legend-tick-green"' in body
    assert 'id="graph-legend-tick-yellow"' in body
    assert 'id="graph-axis-max"' in body
    assert 'id="graph-connector"' in body
    assert "100ms" in body
    assert "200ms" in body
    # Default interval per spec (the <select> option's value attribute).
    assert 'value="2.5"' in body
    # Focus window preset options.
    assert 'value="auto"' in body
    assert 'value="60"' in body
    assert 'value="300"' in body
    assert 'value="600"' in body
    # Packet-type dropdown is disabled and ICMP is the only option.
    assert "ICMP" in body
    assert '<select id="packet-type" name="packet-type" disabled>' in body
    # Internal-DNS masking toggle (screenshot mode) + settings dialog entry.
    assert 'id="mask-toggle"' in body
    assert "Mask internal DNS" in body
    assert 'id="btn-settings"' in body
    assert 'id="settings-dialog"' in body
    assert 'id="settings-cidrs"' in body
    assert 'id="settings-cidrs-default"' in body
    assert 'id="settings-influx-url"' in body
    assert 'id="settings-influx-db"' in body
    assert 'id="settings-influx-interface"' in body
    assert 'id="settings-influx-host"' in body
    assert 'id="settings-save"' in body
    # Static asset wiring.
    assert 'href="/static/style.css"' in body
    assert 'src="/static/app.js"' in body


def test_dashboard_table_headers_are_associated_with_columns() -> None:
    body = TestClient(web.app, base_url="http://localhost").get("/").text
    assert body.count('scope="col"') == 10


def test_healthz_returns_expected_payload() -> None:
    client = TestClient(web.app, base_url="http://localhost")

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_static_assets_are_served() -> None:
    """The Slice 4 UI ships as package static files; verify they are wired."""
    client = TestClient(web.app, base_url="http://localhost")

    for name, expected_ct in (
        ("app.js", "javascript"),
        ("style.css", "css"),
    ):
        r = client.get(f"/static/{name}")
        assert r.status_code == 200, (name, r.status_code)
        assert expected_ct in r.headers["content-type"], (
            name,
            r.headers["content-type"],
        )
        assert r.text  # non-empty


def test_static_assets_are_packaged() -> None:
    """package-data must include the static dir so wheels/sdists ship them.

    Verifies the setuptools config + the on-disk layout that an editable or
    wheel install would expose. ``StaticFiles`` reads from this path at app
    construction time.
    """
    from pathlib import Path

    from netplot import web as web_mod

    static_dir = Path(web_mod.__file__).resolve().parent / "static"
    assert (static_dir / "index.html").exists()
    assert (static_dir / "app.js").exists()
    assert (static_dir / "style.css").exists()


def test_cli_reports_startup_error(monkeypatch, capsys) -> None:
    def fake_run(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(cli, "uvicorn", SimpleNamespace(run=fake_run))

    result = cli.main([])

    captured = capsys.readouterr()
    assert result == 1
    assert "Failed to start netplot server" in captured.out
