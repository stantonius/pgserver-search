# pgserver-search

> **Disclaimer — this was vibe-coded** (in [Simon Willison's sense](https://simonwillison.net/2025/Mar/19/vibe-coding/):
> I kept prompting an LLM until it worked, without reviewing the generated
> code line-by-line). **Intended use:** throwaway Postgres instances in
> Jupyter notebooks and quick test scripts — a richer alternative to
> SQLite for when you need real Postgres search features like
> `pg_trgm` trigram similarity, `pgvector` vector search, and
> `btree_gist`-backed `tstzrange` filters for search results.
>
> **Not intended for production-parity testing.** Embedded Postgres
> differs from your hosted/RDS Postgres in build flags, OS libraries
> (glibc / ICU collation), default `postgresql.conf`, and available
> extensions. If you need audited infrastructure or prod-equivalent
> integration tests, use [testcontainers](https://testcontainers.com/)
> or a staging Postgres instead — or at minimum, read the diff
> against [orm011/pgserver](https://github.com/orm011/pgserver) and
> build from source (see below).

A self-contained Postgres server for Python applications, with
`pg_trgm`, `pgvector`, and `btree_gist` bundled in for trigram,
vector, and time-window-filtered search workflows.

## What this is

This is a fork of [orm011/pgserver](https://github.com/orm011/pgserver)
that adds selected Postgres contrib/search extensions to the bundled
Postgres build. All credit for the core design and implementation goes
to Oscar Moll — this fork only changes the build recipe and packaging.

The upstream `pgserver` bundles Postgres + `pgvector`. This fork
additionally builds and installs `pg_trgm` and `btree_gist` into the
packaged Postgres layout, so you can combine vector/trigram search with
Postgres `tstzrange` filters, for example "search only records visible
during this time window", without extra local Postgres setup.

## Why

`pg_trgm` and `btree_gist` ship in Postgres' `contrib/` tree but are
not installed by the default `make install` that upstream pgserver
runs. If you want fuzzy text search or time-window filtering over
search results alongside vector search in an embedded Postgres, you
previously had to build those pieces yourself. This fork does that for
you.

## Install

### From PyPI (recommended)

Pre-built wheels are published to PyPI for the platforms built by this
repo's release workflow:

```bash
pip install pgserver-search
```

The same wheels are also attached to tagged GitHub releases.

### From source (security-conscious install)

If you'd rather inspect the code and build Postgres yourself instead
of trusting a binary wheel, do this:

```bash
# 1. Clone and inspect
git clone https://github.com/stantonius/pgserver-search.git
cd pgserver-search

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
18.3 source, configures, compiles, installs `pg_trgm`, `pgvector`, and
`btree_gist` into the package layout) and then `pip install dist/*.whl`. Takes
~5–10 minutes the first time; everything is cached after that.

## Usage

The Python import name stays `pgserver` (so existing code keeps
working) — only the distribution name is `pgserver-search`.

```python
import pgserver, tempfile

with tempfile.TemporaryDirectory() as d:
    pg = pgserver.get_server(d, cleanup_mode='delete')
    pg.psql("CREATE EXTENSION pg_trgm;")
    pg.psql("CREATE EXTENSION vector;")
    pg.psql("CREATE EXTENSION btree_gist;")
    print(pg.psql("SELECT similarity('hello', 'helo');"))
    pg.cleanup()
```

## License

Same as upstream pgserver — PostgreSQL license (MIT-family). See
[`LICENSE`](./LICENSE).

## Differences from upstream

- `pgbuild/Makefile` — adds `pg_trgm` and `btree_gist` targets that
  build and install Postgres contrib extensions into
  `src/pgserver/pginstall/`.
- `setup.py` — hooks `make build` into setuptools so source
  installs (`pip install git+https://...`) produce a working wheel
  automatically.
- `pyproject.toml` — package renamed to `pgserver-search`; adds
  `include-package-data` and a `pginstall/**` glob so the built
  binaries are shipped inside the wheel.
- `.github/workflows/build-and-test.yml` — tagged releases upload
  wheels as GitHub Release assets (upstream uses PyPI Trusted
  Publishing).
