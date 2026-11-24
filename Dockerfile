ARG DOTNET_RUNTIME=mcr.microsoft.com/dotnet/runtime:10.0-noble
ARG BUILD_RUNTIME=${DOTNET_RUNTIME}

FROM scratch AS release-bundle
ADD --checksum=sha256:89fa96591a6263dbc5f405d00e8f6f4a411666e014bfd1f449782234c1e6b613 https://github.com/donet-org-cn/SimdKafka/releases/download/v1.0.0/SimdKafka-1.0.0-portable.tar.gz /simdkafka.tar.gz

FROM --platform=$BUILDPLATFORM ${BUILD_RUNTIME} AS release
COPY --from=release-bundle /simdkafka.tar.gz /tmp/simdkafka.tar.gz
RUN echo '89fa96591a6263dbc5f405d00e8f6f4a411666e014bfd1f449782234c1e6b613  /tmp/simdkafka.tar.gz' | sha256sum -c - \
    && mkdir /release /data \
    && tar -xzf /tmp/simdkafka.tar.gz -C /release --strip-components=1

FROM ${DOTNET_RUNTIME}
LABEL org.opencontainers.image.title="SimdKafka" \
      org.opencontainers.image.description="Standalone .NET broker for the Kafka wire protocol" \
      org.opencontainers.image.source="https://github.com/donet-org-cn/SimdKafka" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.revision="ce2b571d196eff99be4e30d3c1689b8339db194b"

WORKDIR /opt/simdkafka
COPY --from=release /release/ ./
COPY --from=release --chown=app:app /data/ /var/lib/kafka/data/
COPY --chmod=755 docker/entrypoint.sh /usr/local/bin/simdkafka
USER $APP_UID
EXPOSE 9092/tcp
ENTRYPOINT ["/usr/local/bin/simdkafka"]
