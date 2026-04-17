# pgserver-search-tools

> **Disclaimer — this was vibe-coded** (in [Simon Willison's sense](https://simonwillison.net/2025/Mar/19/vibe-coding/):
> I kept prompting an LLM until it worked, without reviewing the generated
> code line-by-line). **Intended use:** throwaway Postgres instances in
> Jupyter notebooks and quick test scripts — a richer alternative to
> SQLite for when you need real Postgres features like `pg_trgm`
> trigram similarity or `pgvector` vector search.
>
> **Not intended for production-parity testing.** Embedded Postgres
> differs from your hosted/RDS Postgres in build flags, OS libraries
> (glibc / ICU collation), default `postgresql.conf`, and available
> extensions. If you need audited infrastructure or prod-equivalent
> integration tests, use [testcontainers](https://testcontainers.com/)
> or a staging Postgres instead — or at minimum, read the diff
> against [orm011/pgserver](https://github.com/orm011/pgserver) and
> build from source (see below).

A self-contained Postgres server for Python applications, with the
`pg_trgm` trigram-matching extension and `pgvector` bundled in.

## What this is

This is a fork of [orm011/pgserver](https://github.com/orm011/pgserver)
that adds `contrib/pg_trgm` to the bundled Postgres build. All credit
for the core design and implementation goes to Oscar Moll — this fork
only changes the build recipe and packaging.

The upstream `pgserver` bundles Postgres + `pgvector`. This fork
additionally builds and installs `pg_trgm` into the packaged Postgres
layout, so you can `CREATE EXTENSION pg_trgm` without any extra steps.

## Why

`pg_trgm` ships in Postgres' `contrib/` tree but is not installed by
the default `make install` that upstream pgserver runs. If you want
trigram similarity / fuzzy string matching alongside vector search in
an embedded Postgres, you previously had to build it yourself. This
fork does that for you.

## Install

### From a GitHub Release wheel (recommended)

Pre-built wheels for Linux, macOS (x86_64 + arm64), and Windows are
attached to each tagged release:

```bash
pip install https://github.com/stantonius/pgserver-search-tools/releases/download/v0.1.4/pgserver_search_tools-0.1.4-<tag>.whl
```

Pick the wheel matching your platform/Python version from the
[releases page](https://github.com/stantonius/pgserver-search-tools/releases).

### From source (security-conscious install)

If you'd rather inspect the code and build Postgres yourself instead
of trusting a binary wheel, do this:

```bash
# 1. Clone and inspect
git clone https://github.com/stantonius/pgserver-search-tools.git
cd pgserver-search-tools

# 2. Audit what will run during the build
less pgbuild/Makefile   # downloads postgres-18.3.tar.gz from ftp.postgresql.org
less setup.py           # hooks `make` into setuptools' build_py
less pyproject.toml

# 3. Install system build deps (Debian/Ubuntu)
sudo apt-get install -y build-essential curl tar zlib1g-dev

# macOS: Xcode command line tools are enough
# xcode-select --install

# 4. Build the Postgres binaries into src/pgserver/pginstall/
make build

# 5. Build a wheel from those binaries and install it
make install-wheel
```

The `make install-wheel` target runs `make build` (downloads Postgres
18.3 source, configures, compiles, installs `pg_trgm` + `pgvector`
into the package layout) and then `pip install dist/*.whl`. Takes
~5–10 minutes the first time; everything is cached after that.

## Usage

The Python import name stays `pgserver` (so existing code keeps
working) — only the distribution name is `pgserver-search-tools`.

```python
import pgserver, tempfile

with tempfile.TemporaryDirectory() as d:
    pg = pgserver.get_server(d, cleanup_mode='delete')
    pg.psql("CREATE EXTENSION pg_trgm;")
    pg.psql("CREATE EXTENSION vector;")
    print(pg.psql("SELECT similarity('hello', 'helo');"))
    pg.cleanup()
```

## License

Same as upstream pgserver — PostgreSQL license (MIT-family). See
[`LICENSE`](./LICENSE).

## Differences from upstream

- `pgbuild/Makefile` — adds a `pg_trgm` target that builds and
  installs `contrib/pg_trgm` into `src/pgserver/pginstall/`.
- `setup.py` — hooks `make build` into setuptools so source
  installs (`pip install git+https://...`) produce a working wheel
  automatically.
- `pyproject.toml` — package renamed to `pgserver-search-tools`; adds
  `include-package-data` and a `pginstall/**` glob so the built
  binaries are shipped inside the wheel.
- `.github/workflows/build-and-test.yml` — tagged releases upload
  wheels as GitHub Release assets (upstream uses PyPI Trusted
  Publishing).
