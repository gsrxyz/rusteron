## How to Build

To build **rusteron-archive**, ensure the following are installed:

### Requirements

- **Rust**: Install via [rustup.rs](https://rustup.rs/)
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
````

* **Java 17+**: Required for Aeron C bindings.

* **CMake & Clang**: Required to compile C bindings.

  ```bash
  sudo apt install cmake clang uuid-dev libbsd-dev -y
  # or
  sudo snap install cmake --classic
  ```

* **just** (optional): Command runner for build tasks.

  ```bash
  cargo install just
  ```

---

### Installing Java 17

**macOS** (via Homebrew):

```bash
brew install openjdk@17
sudo ln -sfn $(brew --prefix openjdk@17)/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
```

**Linux**:

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

**Windows**:
Install from [Adoptium](https://adoptium.net/) (Temurin 17). Ensure environment variables are set during installation.

---

### Build Commands

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

**Missing Aeron files?**
Initialize submodules:

```bash
git submodule update --init --recursive
```

```
