# The dacli code-execution sandbox image.
#
# A per-session container is launched from this image (one per data-role
# session). The agent's code runs here as a NON-ROOT user, inside a writable
# virtualenv it can `pip install` into, with no host filesystem access except
# the bind-mounted /workspace data dir. It reaches real platforms only through
# the governed bridge on the host (host.docker.internal) — credentials never
# enter the container.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Non-root identity for the sandboxed code.
RUN useradd --create-home --uid 1000 sandbox

# A virtualenv owned by the sandbox user so unprivileged `pip install <pkg>`
# works at runtime (the agent can add dependencies for a job) and `python`
# resolves to it via PATH.
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv "$VIRTUAL_ENV"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Bake the common data-engineering stack so most jobs need no install at all.
RUN pip install --no-cache-dir \
        pandas \
        numpy \
        pyarrow \
        python-dateutil

# The standalone worker (stdlib-only; the host code is never mounted).
RUN mkdir -p /opt/dacli
COPY worker.py /opt/dacli/worker.py

# Hand the venv + worker to the unprivileged user, then drop privileges.
RUN chown -R sandbox:sandbox "$VIRTUAL_ENV" /opt/dacli

USER sandbox
WORKDIR /workspace

# The container is a long-lived host for `docker exec` runs; it does nothing on
# its own until the runtime execs a worker against a mounted run dir.
CMD ["sleep", "infinity"]
