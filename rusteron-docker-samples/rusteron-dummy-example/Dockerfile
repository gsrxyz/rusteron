FROM ubuntu:latest AS builder

WORKDIR /usr/src/app
ARG RUST_VERSION=1.98.1
# clang, lld and llvm-ar of the LLVM major version rustc uses (`rustc -vV`), for cross-language LTO
ARG LLVM_VERSION=22

RUN apt-get update && apt-get install -y default-jdk curl pkg-config libssl-dev uuid-dev ca-certificates make cmake gcc g++ clang zlib1g-dev libbsd-dev lsb-release wget gnupg software-properties-common \
    && curl -fsSL https://apt.llvm.org/llvm.sh -o /tmp/llvm.sh && bash /tmp/llvm.sh ${LLVM_VERSION} \
    && apt-get install -y lld-${LLVM_VERSION} \
    && apt-get clean  \
    && rm -rf /var/lib/apt/lists/* /tmp/llvm.sh
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain ${RUST_VERSION}

COPY Cargo.toml ./
COPY src ./src

# build profile dev or release
ARG BUILD_TYPE=release

# -C linker-plugin-lto makes rusteron compile Aeron C to LLVM bitcode with $CC, so the linker
# can inline it into the Rust that calls it
ENV CC=clang-${LLVM_VERSION}
ENV RUSTFLAGS="-C target-cpu=native -C linker-plugin-lto -C linker=clang-${LLVM_VERSION} -C link-arg=-fuse-ld=lld-${LLVM_VERSION}"
RUN . "$HOME/.cargo/env" && cargo update && cargo build --profile ${BUILD_TYPE}

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y libssl-dev uuid-dev libatomic1 ca-certificates  \
    && apt-get clean  \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/local/bin

# build directory debug or release
ARG BUILD_DIR_NAME=release

COPY --from=builder /usr/src/app/target/${BUILD_DIR_NAME}/ticker_reader /usr/local/bin/ticker_reader
COPY --from=builder /usr/src/app/target/${BUILD_DIR_NAME}/ticker_writer /usr/local/bin/ticker_writer
#COPY --from=builder /usr/src/app/target/${BUILD_DIR_NAME}/build/rusteron-archive-*/out/build/lib/*.so /usr/lib
#COPY --from=builder /usr/src/app/target/${BUILD_DIR_NAME}/build/rusteron-media-driver-*/out/build/lib/*.so /usr/lib
