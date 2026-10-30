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
    && apt-get install -y --no-install-recommends ca-certificates gosu curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 10001 oss
WORKDIR /app
COPY --from=build /oss-server-bin /usr/local/bin/oss-server
COPY docker/entrypoint.sh /usr/local/bin/oss-entrypoint
# 默认单机配置（compose 可只读挂载覆盖：docker/oss.toml 或 docker/cluster/n*/oss.toml）
COPY docker/oss.toml /app/oss.toml
RUN chmod +x /usr/local/bin/oss-entrypoint \
    && mkdir -p /data /staging && chown -R oss:oss /data /staging /app
EXPOSE 9000
# 首启引导管理员（已存在则忽略）。口令来源：OSS_ADMIN_PASSWORD，或在配了
# OSS_SECRETS_FILE 时自动生成并落盘（docker 栈即此路径；见 README「Docker」）。
ENV OSS_ADMIN_USER=erik
VOLUME ["/data", "/staging"]
# root 起步：入口脚本修正 /secrets 属主后 gosu 降权到 oss 运行（服务进程不以 root 跑）
ENTRYPOINT ["oss-entrypoint"]
CMD ["serve"]
