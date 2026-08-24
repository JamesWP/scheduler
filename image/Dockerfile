# Single image bundling etcd + kube-apiserver + kube-scheduler + kwok + kubectl.
FROM debian:bookworm-slim AS downloader

ARG TARGETARCH=amd64
ARG K8S_VERSION=v1.31.4
ARG ETCD_VERSION=v3.5.17
ARG KWOK_VERSION=v0.6.1

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates tar \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://dl.k8s.io/${K8S_VERSION}/bin/linux/${TARGETARCH}/kube-apiserver" -o /usr/local/bin/kube-apiserver \
    && curl -fsSL "https://dl.k8s.io/${K8S_VERSION}/bin/linux/${TARGETARCH}/kube-scheduler" -o /usr/local/bin/kube-scheduler \
    && curl -fsSL "https://dl.k8s.io/${K8S_VERSION}/bin/linux/${TARGETARCH}/kube-controller-manager" -o /usr/local/bin/kube-controller-manager \
    && curl -fsSL "https://dl.k8s.io/${K8S_VERSION}/bin/linux/${TARGETARCH}/kubectl" -o /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kube-apiserver /usr/local/bin/kube-scheduler /usr/local/bin/kube-controller-manager /usr/local/bin/kubectl

RUN curl -fsSL "https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-${TARGETARCH}.tar.gz" \
      | tar -xz -C /tmp \
    && mv "/tmp/etcd-${ETCD_VERSION}-linux-${TARGETARCH}/etcd" /usr/local/bin/etcd

RUN curl -fsSL "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}/kwok-linux-${TARGETARCH}" -o /usr/local/bin/kwok \
    && chmod +x /usr/local/bin/kwok

# Official kwok lifecycle stages (fast variants: nodes ready and pods running immediately).
RUN mkdir -p /etc/kwok/stages \
    && base="https://raw.githubusercontent.com/kubernetes-sigs/kwok/${KWOK_VERSION}/kustomize/stage" \
    && for f in node/fast/node-initialize pod/fast/pod-ready pod/fast/pod-complete pod/fast/pod-delete; do \
         curl -fsSL "${base}/${f}.yaml" -o "/etc/kwok/stages/$(basename "$f").yaml"; \
       done

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends openssl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=downloader /usr/local/bin/kube-apiserver /usr/local/bin/kube-scheduler \
     /usr/local/bin/kube-controller-manager /usr/local/bin/kubectl \
     /usr/local/bin/etcd /usr/local/bin/kwok /usr/local/bin/

COPY --from=downloader /etc/kwok/stages /etc/kwok/stages
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 6443
ENTRYPOINT ["/entrypoint.sh"]
