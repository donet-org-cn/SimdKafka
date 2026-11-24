ARG DOTNET_RUNTIME=mcr.microsoft.com/dotnet/runtime:10.0-noble
ARG BUILD_RUNTIME=${DOTNET_RUNTIME}
ARG SIMDKAFKA_VERSION=1.0.0
ARG SIMDKAFKA_RELEASE_SHA256=89fa96591a6263dbc5f405d00e8f6f4a411666e014bfd1f449782234c1e6b613
ARG SIMDKAFKA_REVISION=ce2b571d196eff99be4e30d3c1689b8339db194b

FROM scratch AS release-bundle
ARG SIMDKAFKA_VERSION
ARG SIMDKAFKA_RELEASE_SHA256
ADD --checksum=sha256:${SIMDKAFKA_RELEASE_SHA256} https://github.com/donet-org-cn/SimdKafka/releases/download/v${SIMDKAFKA_VERSION}/SimdKafka-${SIMDKAFKA_VERSION}-portable.tar.gz /simdkafka.tar.gz

FROM --platform=$BUILDPLATFORM ${BUILD_RUNTIME} AS release
ARG SIMDKAFKA_RELEASE_SHA256
COPY --from=release-bundle /simdkafka.tar.gz /tmp/simdkafka.tar.gz
RUN printf '%s  /tmp/simdkafka.tar.gz\n' "$SIMDKAFKA_RELEASE_SHA256" | sha256sum -c - \
    && mkdir /release /data \
    && tar -xzf /tmp/simdkafka.tar.gz -C /release --strip-components=1

FROM ${DOTNET_RUNTIME}
ARG SIMDKAFKA_VERSION
ARG SIMDKAFKA_REVISION
LABEL org.opencontainers.image.title="SimdKafka" \
      org.opencontainers.image.description="Standalone .NET broker for the Kafka wire protocol" \
      org.opencontainers.image.source="https://github.com/donet-org-cn/SimdKafka" \
      org.opencontainers.image.version="${SIMDKAFKA_VERSION}" \
      org.opencontainers.image.revision="${SIMDKAFKA_REVISION}"

WORKDIR /opt/simdkafka
COPY --from=release /release/ ./
COPY --from=release --chown=app:app /data/ /var/lib/kafka/data/
COPY --chmod=755 docker/entrypoint.sh /usr/local/bin/simdkafka
USER $APP_UID
EXPOSE 9092/tcp
ENTRYPOINT ["/usr/local/bin/simdkafka"]
