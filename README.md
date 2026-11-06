# netplot

Netplot is a PingPlotter-style network latency and packet-loss dashboard with
continuous per-hop probing, live timelines, persistent background collections,
CSV export, and Prometheus metrics.

![Netplot dashboard showing per-hop latency, packet loss, and a live timeline](docs/netplot-dashboard.png)

> **Public source; private prebuilt images.** This repository can be browsed,
> cloned, and downloaded without authentication. The container image
> (`ghcr.io/jschert/netplot`) remains **private**: pulling it requires package
> access and a credential with `read:packages`. Native installs and local
> source builds do not require registry access.

## Quick Start (Docker)

Requirements: Docker (Engine 24+ with the Compose v2 plugin, or Docker
Desktop), package access, and a `read:packages` credential for the private
image. The [GitHub CLI](https://cli.github.com/) (`gh`) is optional if you
use the dedicated-token login below. No Git checkout, Python, or build
toolchain is needed.
The command examples use Bash (Linux/macOS, or WSL/Git Bash on Windows).

### 1. One-time: authenticate to GHCR

```bash
GH_BROWSER=true BROWSER=true gh auth login --hostname github.com --git-protocol https --web --scopes read:packages
```

The command prints a one-time device code and URL. Open that URL yourself
and enter the displayed code; the `true` browser command prevents automatic
browser launches. This device code is not a personal access token.

This requests the usual source-repository access plus `read:packages` for
private GHCR pulls. If you were already logged in without that scope, add it
once with `GH_BROWSER=true BROWSER=true gh auth refresh --hostname github.com --scopes read:packages`.
Then pass the token to Docker through stdin, not as a command-line argument:

```bash
gh auth token --hostname github.com | docker login ghcr.io -u YOUR_GITHUB_USERNAME --password-stdin
```

Docker stores the credential in its configured credential helper, or as
recoverable base64 in `~/.docker/config.json` when no helper is configured.
The CLI token may also grant repository access or other permissions beyond
image pulls. Prefer a dedicated `read:packages`-only token below when you
want a least-privilege Docker credential.

Replace `YOUR_GITHUB_USERNAME` with your own GitHub login. Alternatively,
enter a classic personal access token with `read:packages` at a hidden
Bash prompt, without putting its value in shell history:

```bash
read -r -s -p "GHCR token: " GHCR_TOKEN
printf '\n'
printf '%s' "$GHCR_TOKEN" | docker login ghcr.io -u YOUR_GITHUB_USERNAME --password-stdin
unset GHCR_TOKEN
```

Never commit real tokens, and never pipe a remote script into a shell —
the commands here only use your own local `gh`/`docker` binaries.

### 2. Download the Compose file (version-pinned)

```bash
mkdir netplot && cd netplot
curl -fsSL https://raw.githubusercontent.com/jschert/netplot/v0.1.3/docker-compose.yml \
  -o docker-compose.yml
printf 'NETPLOT_VERSION=0.1.3\n' > .env
```

The Compose file and image are both pinned to the `v0.1.3` release; `.env`
keeps the selected image version across later commands. Substitute another
published stable `vX.Y.Z` tag and matching `X.Y.Z` image version if desired.
Downloading the Compose file needs no GitHub login; pulling the private
image still requires the registry authentication above.

### 3. Start

```bash
docker compose up -d
```

Open **http://127.0.0.1:8000**, enter a target (DNS name or IP), pick an
interval, and press the green **Play** button. The first start pulls
`ghcr.io/jschert/netplot:0.1.3` for your host (`linux/amd64` or
`linux/arm64`). A `denied`/`not found` error means either your account
lacks access or the selected tag is unavailable.

### Configuration

All knobs are environment variables read by `docker-compose.yml`. Put them
in a `.env` file next to it to make them stick:

| Variable                | Default                     | Purpose                                                                   |
| ----------------------- | --------------------------- | ------------------------------------------------------------------------- |
| `NETPLOT_VERSION`       | `latest`                    | Image tag to pull, e.g. `0.1.3`                                           |
| `NETPLOT_BIND_ADDRESS`  | `127.0.0.1`                 | Host interface the published port binds to                                |
| `NETPLOT_PORT`          | `8000`                      | Host port                                                                 |
| `NETPLOT_ALLOWED_HOSTS` | `localhost,127.0.0.1,::1`   | Extra exact Host values to accept, e.g. `netplot.example` (see *LAN access*) |
| `FORWARDED_ALLOW_IPS`   | `127.0.0.1`                 | Uvicorn: direct peers whose `X-Forwarded-Proto`/`X-Forwarded-For` headers are trusted (see *Reverse proxy*) |

```dotenv
# .env
NETPLOT_VERSION=0.1.3
NETPLOT_BIND_ADDRESS=127.0.0.1
NETPLOT_PORT=8000
```

- The default bind is **loopback only**, deliberately: netplot has **no
  authentication** — anyone who can reach the port can see and control
  every trace and download its full history (CSV). See *LAN access* below
  before changing it.
- The `netplot-data` named volume persists `settings.json` (internal CIDR
  ranges, optional InfluxDB source — edited via the dashboard's ⚙ Settings
  dialog) and background session definitions across upgrades and container
  recreation. Sample history is **not** persisted.
- `restart: unless-stopped` keeps the dashboard running across reboots and
  Docker daemon restarts.

### Updating

To upgrade, change `NETPLOT_VERSION` in `.env` to the published release you
want, then run:

```bash
docker compose pull
docker compose up -d
```

Keep the selected version in `.env`, rather than a one-command environment
override that disappears on the next invocation. To follow stable releases
automatically on each explicit pull, set `NETPLOT_VERSION=latest`.

Keep the same project directory/name when updating: Compose's volume name
is project-scoped (`<project>_netplot-data`), so changing projects selects a
different data volume. Image updates do not replace the existing volume.

### Optional: LAN access (explicit)

netplot has no authentication or access control. Only expose it on
networks you fully trust (e.g. an isolated lab VLAN), and never to the
public internet:

```bash
NETPLOT_ALLOWED_HOSTS=192.168.1.10 NETPLOT_BIND_ADDRESS=0.0.0.0 docker compose up -d        # all interfaces
NETPLOT_ALLOWED_HOSTS=192.168.1.10 NETPLOT_BIND_ADDRESS=192.168.1.10 docker compose up -d   # one interface only
```

Set `NETPLOT_ALLOWED_HOSTS` to additional exact comma-separated hostnames/IPs clients
use (no schemes, ports, or wildcards; IPv6 addresses without brackets). The
loopback hosts `localhost,127.0.0.1,::1` are always allowed, including for
the container health check. This also applies to native installs.
Unlisted Host headers return 400. Browser requests from another origin
return 403 — this includes cross-site *and same-site sibling-origin*
subresource reads such as `<img>`, `<script>`, `fetch()`, and EventSource
loads driven by a visited page (a sibling origin is a different origin
sharing your registrable domain, e.g. `other.netplot.example` next to
`netplot.example` under a wildcard DNS zone), not just form/scripted
mutations, so another web page cannot make this server query its
configured InfluxDB or pull dashboard responses.
Top-level navigation still works (typed address, bookmark, or a followed
link from another site), as do the dashboard's own same-origin requests
and CLI/monitoring clients (curl, Prometheus) that send no browser
metadata at all. The dashboard also refuses cross-origin framing
(`X-Frame-Options: SAMEORIGIN` + `Content-Security-Policy:
frame-ancestors 'self'`) while same-origin embedding keeps working; no
legitimate cross-origin embedding of the unauthenticated UI is supported.
These are browser boundaries, not access control: they depend on standard
browser metadata (Fetch Metadata/Origin headers) and framing enforcement,
so they do not protect against non-browser or modified clients — keep the
service on trusted networks. For a reverse proxy, preserve Host and
configure the externally visible hostname; only trust forwarded scheme
headers from that proxy (see *Reverse proxy* below for the full setup).
Shared deployments should use an authenticating reverse proxy and prevent
direct backend access. Host/Origin validation does not provide user
authentication.

Request bodies are capped at 1 MiB and the cap is enforced *before*
parsing: a larger declared body is refused with 413 (a malformed or
self-contradicting `Content-Length` with 400) before the server allocates
or parses it, and streamed (chunked) bodies are refused with the same 413
the moment their running total crosses the cap — never masked as an
ordinary body-parse 400. Every netplot endpoint exchanges small JSON
bodies, so no legitimate client — dashboard, curl, or monitoring — sends
anywhere near the cap.

Throughput queries allow four concurrent requests per process and have a
five-second total deadline; excess requests receive 429 with Retry-After.
Event streams allow eight subscribers per trace and 32 per process, also
returning 429 when full. Closing a stream releases its slot. Influx redirects,
compressed responses, and environment-configured outbound proxies are disabled.

For a persistent LAN binding, set both `NETPLOT_BIND_ADDRESS` and
`NETPLOT_ALLOWED_HOSTS` in `.env`. The
one-command overrides above do not persist; a later bare `docker compose
up -d` uses the saved `.env` value or the loopback default.

### Optional: reverse proxy (HTTPS front end)

netplot itself serves plain HTTP, but an authenticating TLS reverse proxy can
front it. The proxy is then the *only* component that should be able to reach
the backend, and the backend needs two pieces of configuration:

1. **`NETPLOT_ALLOWED_HOSTS`** must include the externally visible hostname,
   and the proxy must **preserve the `Host` header as the browser sent it** —
   including any non-default port (Host `netplot.example:8443` pairs with
   Origin `https://netplot.example:8443`; the allowlist itself takes the
   bare hostname `netplot.example`, never a scheme or port). Rewriting or
   dropping the port gets the request rejected with 400/403.
2. **`FORWARDED_ALLOW_IPS`** (Uvicorn's native setting — `docker-compose.yml`
   passes it through) must name the proxy **exactly as the backend sees the
   connection peer**, because the backend trusts `X-Forwarded-Proto: https`
   (and `X-Forwarded-For`) only from that address. The proxy must send
   `X-Forwarded-Proto: https`; without it, browser POST/PUT/DELETE requests
   from the HTTPS page are rejected with 403 while GETs keep working.
   With Docker published ports the peer is often the **container network
   gateway** (e.g. `172.18.0.1`), not the proxy machine's LAN address —
   check what the backend actually sees before configuring, and prefer an
   exact IP (a CIDR is also accepted). Trust only that one proxy address;
   `*` trusts every peer and must not be used.

Both variables reach the container through Compose, so put them in `.env`
next to `docker-compose.yml` and they apply on every `docker compose up -d`:

```dotenv
# .env
NETPLOT_VERSION=0.1.3
NETPLOT_ALLOWED_HOSTS=netplot.example
FORWARDED_ALLOW_IPS=192.0.2.10
```

Keep the backend unreachable except from the proxy: leave
`NETPLOT_BIND_ADDRESS` at its loopback default **on the proxy's Docker
host**, or bind it to an interface only the proxy can reach. The proxy's
authentication protects nothing if clients can bypass it and hit the
no-auth backend directly — and note that Host/Origin validation is a
browser boundary, not authentication.

### Stop and uninstall

```bash
docker compose stop     # stop; container kept
docker compose down     # stop; remove container and network (data kept)
```

Both preserve the `netplot-data` volume (settings and background session
definitions). Removing the volume is **destructive** — it permanently
deletes those settings (sample history is never persisted anyway):

```bash
docker compose down -v  # DESTRUCTIVE: also deletes the netplot-data volume
```

### Running without Compose

The same image runs with plain Docker. This example uses a distinct volume
named `netplot-data`; it does not automatically reuse Compose's project-scoped
volume. Specify the existing volume name if switching an installed instance:

```bash
docker run -d --name netplot \
  -p 127.0.0.1:8000:8000 \
  -v netplot-data:/data \
  --restart unless-stopped \
  ghcr.io/jschert/netplot:0.1.3
```

## Route-origin caveat (containers)

Inside the container, `ping`/`traceroute` run in the container's network
namespace. Linux's default bridge network can add a Docker bridge/gateway hop;
it is not the host network. Docker Desktop for macOS/Windows adds a Linux
VM as well, so routes can differ further from native host probes. Use native
installation when you need the route as seen by the host itself.

## Native installation (no Docker)

Native probing is supported on Linux and macOS. Requirements: Python ≥ 3.11,
Git, and `ping` + `traceroute` on `PATH`. No GitHub login, registry access,
or `[dev]` extras are needed:

```bash
git clone https://github.com/jschert/netplot.git
cd netplot
python -m venv .venv && source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install .
netplot                # or: python -m netplot
```

The server listens on http://127.0.0.1:8000 by default (`--host` /
`--port` to change; see *Development* for `--reload` behavior). Set
`NETPLOT_DATA_DIR` to a writable directory outside the checkout (default
`<cwd>/data`) to keep `settings.json` and background session definitions
out of the source tree. Native installs probe from the host itself — no
VM indirection, so the Docker Desktop caveat above does not apply.

## Development

Everything below is for working on the source; end users never need it.

### Source checkout and checks

```bash
git clone https://github.com/jschert/netplot.git
cd netplot
python -m venv .venv && source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

Run the Python and JavaScript checks (the latter requires Node.js):

```bash
python -m pytest
node tests/test_app.js
```

Data-sensitive metrics, history, and export fixtures must control probe and
DNS dependencies before creating sessions; a live trace can collect a sample
before an immediate pause. Keep real system-probe coverage in the marked
integration tests and container smoke gates rather than weakening data assertions.

### Run from source

```bash
python -m netplot
# or override bind options:
python -m netplot --host 0.0.0.0 --port 8080
```

The CLI starts Uvicorn with the `netplot.web:app` import path, allowing
`--reload` to restart the application while preserving the selected `--host`
and `--port`.

### Docker: source-build override

The default `docker-compose.yml` has **no `build:` stanza** — it uses the
published image. To build your working tree instead, merge the explicit
override:

```bash
docker compose -f docker-compose.yml -f compose.build.yml up -d --build
```

`compose.build.yml` replaces the `ghcr.io/jschert/netplot` image with
`build: .` tagged `netplot:dev`; the loopback port mapping, restart
policy, and `netplot-data` volume still come from the base file. A bare
`docker compose up -d` never builds source; use `docker compose pull` to update.

### Endpoints

- `GET /` serves the dashboard UI: a PingPlotter-style toolbar (Play/Pause split, target combobox with recent-targets history, interval preset dropdown, Focus window dropdown, disabled ICMP packet-type dropdown, focus caption line), a live hop table (status / Hop / Count / IP / DNS / Avg / Min / Cur / PL% / Graph), and per-hop latency timeline graphs driven by SSE. The Graph column shows shared 100/200 ms zones, min–max whiskers, average/current markers, packet-loss hatching, and an average connector. The target timeline is always shown; double-click any hop row to add or remove a timeline for that hop. Drag a region on any timeline to focus the table stats on that period (the Focus dropdown is the rolling-window size; manual drag-focus overrides it until cleared). Pausing a trace freezes the view (timelines, stats, focus caption) at the server-recorded pause time; resuming continues from current wall time. The **Copy summary** toolbar button copies a plain-text summary of the active trace's target: the Focus/drag-selected window in UTC, probe interval, target avg/min/max RTT and packet loss — exactly the values the table shows for that window, with the Mask internal DNS toggle respected.
- `GET /static/app.js`, `GET /static/style.css` serve the dashboard assets.
- `GET /healthz` returns a simple readiness payload.
- `POST /api/traces` creates and starts a continuous trace against a target (host or IP); route discovery and concurrent per-hop probing begin automatically. See `docs/spec.md` for the full snapshot/SSE contract.
- `GET /api/traces/{id}/export?format=csv` downloads the full server-held sample history as a flat CSV table (`timestamp_utc,timestamp_epoch,hop_index,ip,dns,rtt_ms,packet_lost`; `packet_lost` is `1` for a lost probe and `0` for a reply, with empty `rtt_ms` for losses). Per-hop loss percentage over the retained history is `100 * SUM(packet_lost) / COUNT(*)`, grouping by `hop_index`; it may differ from the dashboard's selected-window PL%. Works while running and while paused; 404 unknown session, 400 unsupported format, **429 with `Retry-After`** when two exports are already in flight (the build runs off the event loop under a per-process concurrency guard). Note: the export returns the FULL retained history (up to 10 000 samples per stream ≈ hours), and the endpoint has no authentication — any party that can reach the dashboard can pull any session's history. The dashboard's **Export** toolbar button (next to Reset) triggers a browser download for the active tab's session, enabled once that session has at least one sample.
- `GET /api/traces/{id}/history?hop=<index>&start=<epoch_s>&end=<epoch_s>&resolution=<raw|1m|auto>` returns per-stream sample history, clamped to `[start, end]`. `resolution=raw` (default) returns the retained raw samples (`{id, hop, resolution: "raw", earliest_t, samples: [{t, rtt_ms}]}`); `resolution=1m` returns per-minute rollup buckets (`{..., resolution: "1m", earliest_t, buckets: [{t, avg_ms, min_ms, max_ms, count, loss_count}]}`, full-loss buckets carry null avg/min/max, empty buckets are omitted); `resolution=auto` picks raw when the requested window fits raw retention (`SAMPLE_CAP * interval_s`), else 1m — the response carries the actual resolution used. `hop=0` is the target stream; `hop>=1` must match a discovered route hop. `earliest_t` (null when empty) lets the client distinguish "no data" from "older than retention". Works while running and while paused; 400 on malformed params or unsupported `resolution`; 404 on unknown session/hop. The dashboard fetches this on restore/reload/reconnect/range-widen so timelines populate immediately. 1h/3h timeline ranges are served raw; 12h/24h ranges use `resolution=auto` (resolved against raw retention) and are rendered from the bucket tier (Slice D2 — the server-side 1-minute rollup keeps memory bounded independent of probe interval; cap 1440 buckets/stream = 24h).
- `GET /metrics` exposes a Prometheus exposition endpoint (`text/plain; version=0.0.4`) for scraping by a Prometheus instance. Seven gauge families: `netplot_session_info` (one series per session, carrying a `state` label for alert gating), `netplot_target_rtt_current_ms`/`_avg_ms`/`_loss_pct`/`_up`, and `netplot_hop_rtt_current_ms`/`_loss_pct`. Avg and loss are computed over the last 60 samples per stream; the current-RTT gauge is absent when the last probe was a loss (no stale values). Paused sessions keep emitting last-known values truthfully labeled `state="paused"` — alert rules should gate on it. See `docs/spec.md` "Metrics" for the full series/label contract.
- `GET /api/settings/internal` / `PUT /api/settings/internal {cidrs: [...]}` manage the internal IP ranges (default RFC1918 + IPv6 ULA; other CIDRs editable, one per line in the settings dialog). Hops and the resolved target are classified server-side with the stdlib `ipaddress` module and annotated on the snapshot (`hops[].internal`, `target_internal`). The dashboard's **Mask internal DNS** toolbar toggle then replaces the DNS names of internal hosts with a neutral alias (`Internal host`) everywhere they are displayed — hop table, timeline labels, tab label/tooltips. The mask is display-only: IP addresses are never masked, and API/CSV payloads keep the real names. Persisted to `<data_dir>/settings.json` (atomic writes).
- `GET /api/settings/influx` / `PUT /api/settings/influx {url, database, interface, host_tag}` configure the optional InfluxDB 1.x throughput overlay (generic, settings-only — no presets: url/database/interface all blank by default, and a non-empty URL requires an explicit database and interface; the dialog placeholders are just examples). http(s) only, no userinfo/query/fragments; identifiers are charset-restricted so they cannot break out of the InfluxQL string literal. No credentials are created, stored, or sent; only existing no-auth sources are supported in this bounded first version.
- `PUT /api/settings {cidrs, influx}` is the combined transactional save the settings dialog's Save button sends: both sections validate, persist, and go live together — an invalid section (422) or a failed persist leaves runtime and `settings.json` untouched. The individual `GET/PUT /api/settings/internal|influx` endpoints remain; all settings writes are serialized server-side.
- `GET /api/throughput?start=<epoch_s>&end=<epoch_s>` returns normalized interface throughput for the span: `{start, end, step_s, points: [{t, in_mbps, out_mbps}]}` (null Mbps = missing bucket, never zero). The server queries backend-side via async HTTPX with a 5-second total deadline (connect plus the whole streamed body; cancellation closes the upstream connection), no redirects, no compression (raw response bodies capped at 1 MiB), and no environment-configured outbound proxies; HTTPS sources verify against the runtime's standard CA bundle. The query uses `non_negative_derivative(last(field), 1s)` over the `net` measurement's `bytes_recv`/`bytes_sent` counters — the fixed `1s` unit makes every bucket bytes/second regardless of the aggregation step, and `last()` reads the counter's end-of-bucket value (counter-reset safe). The counters are grouped by `time(step)` plus the interface/host tags so multiple hosts surface as an error instead of silently merging, bucketed by a step ladder (5 s native for short zooms, aggregated up to 300 s for 24 h; span capped at 24 h). 400 on malformed/unconfigured requests, 502 when InfluxDB is unreachable, ambiguous (set a host tag), or returns a malformed payload. The dashboard overlays it on the primary/target timeline only, behind a per-tab **Throughput** checkbox, on its own labeled Mbps axis with two distinguishable direction lines that break at missing buckets and cadence-sized gaps (never bridged, never zero); latency/loss rendering and focus drag are unchanged, and a missing/unreachable source surfaces in the legend without breaking the chart. Saving the Influx settings clears every tab's overlay data, invalidates in-flight responses, and refetches (or disables the toggle) accordingly.

## Notes

Route discovery (`traceroute`), per-hop probing (`ping`), reverse DNS, the
live hop table API, the live dashboard UI (Slices 4 and 6), the timeline
graph with range/focus controls (Slice 5), and the PingPlotter-style
toolbar (Slice 6b) are implemented.

### Timing, resource limits, and sample accounting

Probing follows PingPlotter's timing model: the configured interval spaces
round SEND deadlines (monotonic-clock driven; the first round launches
immediately on start/resume) and the 3 s reply timeout is independent of
it. Rounds overlap, so a hop that holds its full reply window cannot
stretch the send cadence; a reply arriving after the interval but within
its reply deadline is recorded as a success, and work still pending is
never a loss.

Concurrency is bounded per session: at most 45 in-flight `ping` processes
(each permit held until the subprocess is reaped) and at most 45
launched-but-uncommitted rounds. A round is admitted whole — when the
budgets are exhausted the launch opportunity is skipped and the next
scheduled tick retries: no partial rounds, no catch-up burst after a
stall, and no fabricated packet-loss samples for probes that were never
sent (any resulting gap is visible in the actual launch timestamps). 45
is a netplot safety policy inspired by legacy outstanding-request
guidance, not a verified current PingPlotter default; with the
eight-session cap, ping-process concurrency stays bounded at 360,
excluding route discovery. The ~interval cadence holds while the workload
fits the budget.

Completed rounds are published complete and in launch order. A slow hop
can delay initial publication by up to its reply window; after warm-up,
completed rounds stream at roughly the configured interval while under
budget. `t` on SSE round events and committed samples is the round's
launch time, not reply time.

Snapshot payloads carry an explicit coverage contract: `captured_at` is
the single effective window right edge for the whole payload — frozen at
`paused_at` while paused, so a paused trace's view and baselines share
one window edge — and `sample_from_t`/`sample_through_t` bracket the
exact window behind each stats block (null when empty), per stream. The
client adopts snapshot aggregates only up to their matching cutoffs and
backfills windows from the history endpoint, so the hop table, the
copied summary, and the footer sample count all describe the same
primary-stream focused statistics. The footer's count is the number of
committed samples in the active window — not a count of received SSE
messages. Table Count likewise means finalized samples, not outstanding
probes: admission-skipped or cancelled rounds add no samples and are
never counted as loss.

### Slice 6b toolbar

The toolbar clones the PingPlotter Cloud layout. Play and Pause are
separate buttons (Play is the green primary action, Pause is enabled only
while running). The Target input is a free-text combobox with a dropdown
of recent targets (most-recent-first, deduped, capped at 10, persisted in
`localStorage`). The Interval control is a preset dropdown (1 / 2.5 / 5 /
10 / 30 / 60 s, 2.5 preselected). The Focus dropdown (Auto / 1 minute /
5 minutes / 10 minutes) drives the table stats window, the snapshot fetch,
and the seed-baseline expiry; manual drag-focus overrides it until
cleared. A disabled ICMP-only packet-type dropdown and a Focus caption
line with live local-time bounds round out the row.

### Slice 6 graph column

The graph column uses one live axis shared by all hop rows. Its green, yellow,
and red zones are bounded at 100 ms and 200 ms; the axis ceiling is shown in
the header and recomputed from the largest observed hop latency. Each row has
a status dot, min–max whisker with end caps, navy average marker, open current
marker, and proportional packet-loss hatch. A red SVG connector joins average
markers after each table render and after resize; silent and loss-only rows do
not contribute points.

### Persistent background collections (Slice B)

A trace can be marked as a **background collection** via the
**Background** toolbar checkbox. Background sessions are saved to
`$NETPLOT_DATA_DIR/sessions.json` (default `<cwd>/data/sessions.json`)
and auto-restored when the server restarts, surviving container
restarts and redeploys. Only the session *definition* is saved
(target, interval, name, desired run/pause state) — sample history
restarts empty. Persistent tabs carry a `⇄` marker in the tab bar.
Mark a trace background, restart the server, and the trace re-attaches
in the browser on next load with probing resumed (or stays paused if
it was paused). The state file is atomic-write, schema-versioned, and
ignored-with-warning if corrupt — boot never crashes. See
`docs/spec.md` for the full state-file format and the
`POST /api/traces/{id}/persist` endpoint.

### Configuration and private data

Keep deployment-specific configuration and runtime data outside the source
checkout. Set `NETPLOT_DATA_DIR` to an external writable directory, or let
Compose/mount put the `netplot-data` named volume at `/data` in the
container (see *Quick Start*). The directory holds `settings.json`
(internal CIDRs and optional InfluxDB connection settings) and
`sessions.json` (background collection definitions, including targets and
names), plus any recovery/quarantine files.

Configure network ranges and the optional throughput source through the dashboard's
Settings dialog; connection fields start blank, with no deployment-specific presets.
Do not commit real hostnames, addresses, credentials, `.env` files, local Compose
overrides, captures, or exports. Runtime data, local configuration, and diagnostic
artifacts are excluded from Git and the Docker build context; example environment
templates may be tracked, but must contain only generic values. DNS masking is
display-only, not a sanitization step for API responses, CSV exports, or captures.

## Image internals

The published image (`ghcr.io/jschert/netplot`, tags `X.Y.Z` and `latest`)
is a multi-stage build on `python:3.12-slim`: the build stage installs a
pinned `uv`, exports the frozen runtime dependency set from `uv.lock`, and
builds the `netplot` wheel; the runtime stage installs `iputils-ping`,
`traceroute`, and `ca-certificates` alongside the wheel and runs the
`netplot` console script as a non-root user (uid 1000). `ping` works
unprivileged under Docker's default `ping_group_range`; `traceroute` uses
UDP probes, so neither binary needs `cap_net_raw` or `--privileged`.
Port 8000 is exposed; a stdlib-based `HEALTHCHECK` polls `GET /healthz`.
`ENV NETPLOT_DATA_DIR=/data` plus a `VOLUME /data` make background
session definitions writable by the non-root process. See `docs/spec.md`
for the packaging contract.

## Maintainer notes (releases)

- After main-branch CI passes, push a stable `vX.Y.Z` tag matching the
  package version. The initial release is `v0.1.0`; do not replace a
  published release tag.
- Release CI builds and runs real container smoke checks for `linux/amd64`
  and `linux/arm64`. It transfers the exact smoke-tested images as
  checksummed artifacts and verifies their identities before publication:
  there is no rebuild after the runtime gate.
- The publisher pushes internal `staging-X.Y.Z-<arch>` tags, then assembles
  `X.Y.Z` and `latest` multi-architecture manifests from immutable image
  digests. Stable release runs are serialized; consumer tags move only after
  all test and smoke jobs pass.
- A final new-user gate downloads only the tagged Compose file, pulls the
  private release with the job's short-lived `GITHUB_TOKEN`, starts it on
  loopback, and verifies health and image identity. Anonymous pulls must
  remain denied. The Release run summary records the digest and results.
- The source repository is **public**; the GHCR package remains **private**.
  Nothing automates package visibility changes. The Compose file downloads
  anonymously; image pulls require package access and a `read:packages` login.

## Layout

- `src/netplot/` — application package
- `tests/` — automated tests
- `docs/` — project documentation
- `Dockerfile`, `.dockerignore` — container image build
- `docker-compose.yml` — end-user runner; pulls `ghcr.io/jschert/netplot`
- `compose.build.yml` — developer override; builds from source (`netplot:dev`)
