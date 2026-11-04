## How to build

To build rusteron-archive, ensure the following are installed:

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

  Java, CMake and Clang are only needed to build Aeron C from source (the default). With `features = ["static", "precompile"]` (or `precompile-rustls`) prebuilt libraries are downloaded instead (macOS and Linux); Linux still needs `uuid-dev` and `libbsd-dev` to link.

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
Install from [Adoptium](https://adoptium.net/) (Temurin 17). Ensure environment variables are set during installation.

---

### Build commands

With everything installed:

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
