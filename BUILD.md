## How to build

Install these tools to build rusteron-archive from source:

### Requirements

- Rust: install via [rustup.rs](https://rustup.rs/)
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```

* Java 17+: required for Aeron C bindings.

* CMake and Clang: required to compile C bindings.

  ```bash
  sudo apt install cmake clang uuid-dev libbsd-dev -y
  # or
  sudo snap install cmake --classic
  ```

  The default build compiles Aeron C from source. On macOS and Linux, `features = ["static", "precompile"]` or `precompile-rustls` downloads prebuilt libraries instead. Linux still needs `uuid-dev` and `libbsd-dev` to link.

* just (optional): command runner for build tasks.

  ```bash
  cargo install just
  ```

---

### Installing Java 17

macOS (via Homebrew):

```bash
brew install openjdk@17
sudo ln -sfn $(brew --prefix openjdk@17)/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
```

Linux:

* Ubuntu/Debian:

  ```bash
  sudo apt install openjdk-17-jdk
  ```
* Fedora:

  ```bash
  sudo dnf install java-17-openjdk
  ```
* Arch:

  ```bash
  sudo pacman -S jdk-openjdk
  ```

Windows:
Install from [Adoptium](https://adoptium.net/) (Temurin 17). Set the Java environment variables during installation.

---

### Build commands

Build with Cargo:

```bash
cargo build --release
```

If using `just`:

```bash
just build   # Build project
just test    # Run tests
```

---

### Troubleshooting

If Aeron files are missing, initialise the submodules:

```bash
git submodule update --init --recursive
```
