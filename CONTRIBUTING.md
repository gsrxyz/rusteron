# Contributing to rusteron

Submit bug fixes, documentation changes and new features through pull requests.

## Before you start

- Submit changes via pull requests from a fork.
- Run `cargo fmt --all`, `cargo clippy --all -- --deny warnings` and `cargo test` before submitting (CI enforces all three).
- All contributions are dual-licensed under MIT and Apache-2.0.

## Reporting issues

Use [GitHub Issues](https://github.com/gsrxyz/rusteron/issues) to report bugs, request features, or ask questions. Include reproduction steps, error messages and the relevant configuration.

## Pull request workflow

1. Fork the repo and clone it with its Aeron submodules:
   ```bash
   git clone --recurse-submodules https://github.com/your-username/rusteron.git
   cd rusteron
   ```

2. Create a new branch:

   ```bash
   git checkout -b your-branch-name
   ```

3. Make changes, run tests, and format:

   ```bash
   cargo fmt --all
   cargo clippy --all -- --deny warnings
   cargo test
   ```

4. Commit and push:

   ```bash
   git commit -m "Describe your change"
   git push origin your-branch-name
   ```

5. Open a pull request on GitHub.

## Documentation

To fix typos, clarify text or fill in missing details, open a PR. Documentation-only changes do not need new tests.

## Code style and commit tips

* Keep each pull request focused on one change.
* Use clear commit messages (e.g. `"Fix: prevent panic on empty buffer"`).
* Test coverage is expected for fixes or new features.
