# syntax=docker/dockerfile:1.7
#
# netplot container image.
#
# Two-stage build:
#   1. builder — installs uv, exports the pinned runtime dep set from
#      uv.lock, and builds the netplot wheel.
#   2. runtime — same python:3.12-slim base, installs iputils-ping +
#      traceroute + ca-certificates, drops in the built wheel, and runs
#      the `netplot` console script as a non-root user.
#
# Ping and traceroute both work unprivileged under Docker's default
# ping_group_range; traceroute uses UDP probes so no cap_net_raw is
# required.
#
# Reproducibility note: the python runtime dep versions come from
# uv.lock (frozen). uv itself is pinned at the top. For digest-pinned
# bases, replace `-slim` with `@sha256:...`.

ARG PYTHON_VERSION=3.12
ARG UV_VERSION=0.12.0

# ─── build stage ────────────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim AS builder

ARG UV_VERSION
ENV UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

# uv is a single binary; pull it via pip from the slim image's bundled pip.
RUN pip install --no-cache-dir "uv==${UV_VERSION}"

WORKDIR /app

# Manifest + lock first: this layer is reused across code-only edits, so
# the (slow) dep export and wheel build of dependencies caches well.
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ ./src/

# Export the pinned runtime dependency set from the frozen lock. We omit
# the project itself (--no-emit-project); it is installed in the runtime
# stage from the wheel built below.
RUN uv export --frozen --no-dev --no-emit-project --format requirements-txt \
        -o /tmp/requirements.txt

# Build the netplot wheel (no deps bundled; deps come from requirements).
RUN uv build --wheel --out-dir /tmp/wheels

# ─── runtime stage ──────────────────────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim AS runtime

# OCI source annotation. Setting org.opencontainers.image.source lets GHCR
# associate the pushed image with this repository: a package created on
# first push inherits the repository's private access instead of sitting
# unlinked (and unassociatable) in the registry. CI passes the exact
# repository URL as a build-arg; the default covers plain local
# `docker build .` followed by a manual push.
ARG NETPLOT_SOURCE_URL=https://github.com/jschert/netplot

LABEL org.opencontainers.image.source="${NETPLOT_SOURCE_URL}" \
      org.opencontainers.image.title="netplot" \
      org.opencontainers.image.description="PingPlotter-style network latency and packet-loss dashboard (FastAPI + ping/traceroute)." \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.license="AGPL-3.0-only"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    NETPLOT_DATA_DIR=/data

# System tools the engine shells out to, plus CA bundle for HTTPS to any
# upstream resolver. Clean the apt lists in the same layer to keep the
# image lean.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        iputils-ping \
        traceroute \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install pinned runtime deps first (cacheable; only changes when uv.lock
# changes), then the small project wheel on top.
COPY --from=builder /tmp/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY --from=builder /tmp/wheels /tmp/wheels
RUN pip install --no-cache-dir --no-deps /tmp/wheels/netplot-*.whl \
 && rm -rf /tmp/wheels /tmp/requirements.txt

# Non-root runtime. No capabilities are required; ping_group_range
# (default "0 2147483647" under Docker) already covers the container's
# gid, so unprivileged ICMP works. Persistent (background) sessions are
# written to NETPLOT_DATA_DIR=/data — create it owned by the runtime
# user so the non-root process can write sessions.json without a bind
# mount. Operators wanting persistence across container recreation mount
# a named volume at /data (see README).
RUN useradd --create-home --uid 1000 netplot \
 && mkdir -p /data \
 && chown -R netplot:netplot /data
USER netplot

# Declared so a plain `docker run -v netplot-data:/data netplot:dev`
# keeps sessions.json across container recreation without the operator
# having to remember the path. Mounting is still optional: with no mount,
# persistence is in-effect per-process (state file lives in the
# container's ephemeral /data).
VOLUME /data

EXPOSE 8000

# HEALTHCHECK against GET /healthz. Use the python stdlib (urllib) that
# already ships with the base image instead of installing curl — keeps
# the image smaller.
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import sys,urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3).status == 200 else 1)"

CMD ["netplot", "--host", "0.0.0.0", "--port", "8000"]
