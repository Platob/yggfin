# syntax=docker/dockerfile:1
#
# The image a pod runs one bundled task in: `rekep` on the PATH with the
# locked `runner` group (dbt, the Glue and S3 Tables catalogs), and the dbt
# project `build_dbt` builds, at the working directory its defaults name.
#
# Build it from the commit the DAGs ship from. `EksRekepOperator` hands the pod
# every parameter that checkout declares, so the image's tasks must take them:
#
#   docker build -t rekep:$(git rev-parse --short HEAD) .
#
# Behind a TLS-intercepting proxy, hand its CA bundle in as a build secret;
# nothing of it stays in the image:
#
#   docker build --secret id=ca-bundle,src=/path/to/ca.pem -t rekep .

ARG PYTHON_IMAGE=public.ecr.aws/docker/library/python:3.13-slim-bookworm
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.8.17

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE} AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/rekep/.venv
WORKDIR /opt/rekep
# The locked dependencies alone, then the package as a wheel beside them: the
# environment changes only with the lock, and a change to the source rebuilds
# one small layer rather than the whole environment.
COPY python/pyproject.toml python/uv.lock python/
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=secret,id=ca-bundle \
    if [ -f /run/secrets/ca-bundle ]; then export SSL_CERT_FILE=/run/secrets/ca-bundle; fi; \
    uv sync --project python --locked --no-default-groups --group runner --no-install-project
COPY python/src python/src
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=secret,id=ca-bundle \
    if [ -f /run/secrets/ca-bundle ]; then export SSL_CERT_FILE=/run/secrets/ca-bundle; fi; \
    uv build --project python --wheel --out-dir /opt/rekep/dist

FROM ${PYTHON_IMAGE}
# A task runs as this user and never as root. `data/dbt` is its own, for dbt's
# staging and log; `data/` itself is not, so a run left on the local default
# catalog fails in a pod rather than landing in a filesystem the pod discards.
# `data/` is made here, as root: a `COPY --chown` would give it to the user.
RUN useradd --uid 10001 --create-home rekep && mkdir -p /opt/rekep/data
WORKDIR /opt/rekep
COPY --from=build /opt/rekep/.venv .venv
RUN --mount=from=uv,source=/uv,target=/usr/local/bin/uv \
    --mount=from=build,source=/opt/rekep/dist,target=/tmp/dist \
    uv pip install --python .venv/bin/python --no-deps --no-cache --compile-bytecode /tmp/dist/*.whl
COPY --chown=rekep data/dbt data/dbt
ENV PATH=/opt/rekep/.venv/bin:$PATH
USER 10001
ENTRYPOINT ["rekep"]
CMD ["tasks", "list"]
