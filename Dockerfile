# oss-server 镜像（多阶段：Rust workspace 构建 → 精简运行层）。
# 构建需要 protoc（tonic-build 生成 xl-proto）；sqlx/reed-solomon 的 C 依赖用镜像自带 cc。
FROM rust:1-slim-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# BuildKit 缓存挂载：registry 与 target 复用，迭代重编从「全量 ~10 分钟」降到分钟级；
# 二进制在同一 RUN 内拷出挂载（挂载不跨层持久）
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p oss-server \
    && cp target/release/oss-server /oss-server-bin

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 10001 oss
WORKDIR /app
COPY --from=build /oss-server-bin /usr/local/bin/oss-server
# 默认单机配置（compose 可只读挂载覆盖：docker/oss.toml 或 docker/cluster/n*/oss.toml）
COPY docker/oss.toml /app/oss.toml
RUN mkdir -p /data /staging && chown -R oss:oss /data /staging /app
USER oss
EXPOSE 9000
# 首启引导管理员（已存在则忽略）；生产请改口令与 jwt_secret（见 README）
ENV OSS_ADMIN_USER=erik \
    OSS_ADMIN_PASSWORD=pw
VOLUME ["/data", "/staging"]
ENTRYPOINT ["oss-server"]
CMD ["serve"]
