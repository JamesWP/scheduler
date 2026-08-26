# Single image bundling the unmodified upstream kube-scheduler with a fake,
# in-process API server (server/fakeapi.py) that stands in for etcd,
# kube-apiserver, kube-controller-manager and the KWOK controller.
FROM debian:bookworm-slim AS downloader

ARG TARGETARCH=amd64
ARG K8S_VERSION=v1.31.4

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://dl.k8s.io/${K8S_VERSION}/bin/linux/${TARGETARCH}/kube-scheduler" -o /usr/local/bin/kube-scheduler \
    && curl -fsSL "https://dl.k8s.io/${K8S_VERSION}/bin/linux/${TARGETARCH}/kubectl" -o /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kube-scheduler /usr/local/bin/kubectl

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-venv \
    && rm -rf /var/lib/apt/lists/*

# The run API + fake apiserver (image/server/) is the only Python in the
# image; an isolated venv avoids fighting Debian's PEP 668 "externally
# managed" pip.
RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir fastapi 'uvicorn[standard]'
ENV PATH="/opt/venv/bin:${PATH}"

COPY --from=downloader /usr/local/bin/kube-scheduler /usr/local/bin/kubectl /usr/local/bin/

COPY server/ /opt/schedsim/server/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
