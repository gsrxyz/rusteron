ARG RUST_VERSION=1.89.0
FROM --platform=linux/amd64 rust:${RUST_VERSION}

ENV HOME=/work/target/asan
ENV TMP=/work/target/asan/tmp
ENV TEMP=/work/target/asan/tmp
ENV GRADLE_USER_HOME=/work/target/asan/gradle
ENV CARGO_HOME=/work/target/asan/cargo-home
ENV CARGO_TARGET_DIR=/work/target/asan/target
ENV RUST_TEST_THREADS=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jdk-headless \
        curl \
        pkg-config \
        libssl-dev \
        uuid-dev \
        ca-certificates \
        make \
        cmake \
        gcc \
        g++ \
        clang \
        zlib1g-dev \
        libbsd-dev \
        gdb \
        valgrind \
        libclang-rt-19-dev \
        openssh-server \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work


RUN mkdir -p /var/run/sshd && \
    { \
      echo 'Port 22'; \
      echo 'ListenAddress 127.0.0.1'; \
      echo 'PasswordAuthentication no'; \
      echo 'PubkeyAuthentication yes'; \
      echo 'PermitRootLogin prohibit-password'; \
    } >> /etc/ssh/sshd_config

RUN mkdir -p /root/.ssh && chmod 700 /root/.ssh
COPY id_ed25519.pub /root/.ssh/authorized_keys
RUN chmod 600 /root/.ssh/authorized_keys && chown -R root:root /root/.ssh

CMD ["cargo", "test", "--workspace", "--all", "--all-targets", "--", "--nocapture", "--test-threads=1"]
