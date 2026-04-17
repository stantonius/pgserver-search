import pytest
import pgserver
import subprocess
import tempfile
from typing import Optional, Union
import multiprocessing as mp
import shutil
from pathlib import Path
import pgserver.utils
import socket
from pgserver.utils import find_suitable_port, process_is_running
import psutil
import platform
import sqlalchemy as sa
import datetime
from sqlalchemy_utils import database_exists, create_database
import logging
import os

def _check_sqlalchemy_works(srv : pgserver.PostgresServer):
    database_name = 'testdb'
    uri = srv.get_uri(database_name)

    if not database_exists(uri):
        create_database(uri)

    engine = sa.create_engine(uri)
    conn = engine.connect()

    table_name = 'table_foo'
    with conn.begin():
        # if table exists already, drop it
        if engine.dialect.has_table(conn, table_name):
            conn.execute(sa.text(f"drop table {table_name};"))
        conn.execute(sa.text(f"create table {table_name} (id int);"))
        conn.execute(sa.text(f"insert into {table_name} values (1);"))
        cur = conn.execute(sa.text(f"select * from {table_name};"))
        result = cur.fetchone()
        assert result
        assert result[0] == 1

def _check_postmaster_info(pgdata : Path, postmaster_info : pgserver.utils.PostmasterInfo):
    assert postmaster_info is not None
    assert postmaster_info.pgdata is not None
    assert postmaster_info.pgdata == pgdata

    assert postmaster_info.is_running()

    if postmaster_info.socket_dir is not None:
        assert postmaster_info.socket_dir.exists()
        assert postmaster_info.socket_path is not None
        assert postmaster_info.socket_path.exists()
        assert postmaster_info.socket_path.is_socket()


def _check_server(pg : pgserver.PostgresServer) -> int:
    assert pg.pgdata.exists()
    postmaster_info = pgserver.utils.PostmasterInfo.read_from_pgdata(pg.pgdata)
    assert postmaster_info is not None
    assert postmaster_info.pid is not None
    _check_postmaster_info(pg.pgdata, postmaster_info)

    ret = pg.psql("show data_directory;")
    # parse second row (first two are headers)
    ret_path = Path(ret.splitlines()[2].strip())
    assert pg.pgdata == ret_path
    _check_sqlalchemy_works(pg)
    return postmaster_info.pid

def _kill_server(pid : Union[int,psutil.Process,None]) -> None:
    if pid is None:
        return
    elif isinstance(pid, psutil.Process):
        proc = pid
    else:
        try:
            proc = psutil.Process(pid)
        except psutil.NoSuchProcess:
            return

    if proc.is_running():
        proc.terminate() # attempt cleaner shutdown
        try:
            proc.wait(3) # wait at most a few seconds
        except psutil.TimeoutExpired:
            pass

        if proc.is_running():
            proc.kill()

def test_get_port():
    address = '127.0.0.1'
    port = find_suitable_port(address)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.bind((address, port))
    except OSError as err:
        if 'Address already in use' in str(err):
            raise RuntimeError(f"Port {port} is already in use.")
        raise err
    finally:
        sock.close()

def test_get_server():
    with tempfile.TemporaryDirectory() as tmpdir:
        pid = None
        try:
            # check case when initializing the pgdata dir
            with pgserver.get_server(tmpdir) as pg:
                pid = _check_server(pg)

            assert not process_is_running(pid)
            assert pg.pgdata.exists()

            # check case when pgdata dir is already initialized
            with pgserver.get_server(tmpdir) as pg:
                pid = _check_server(pg)

            assert not process_is_running(pid)
            assert pg.pgdata.exists()
        finally:
            _kill_server(pid)

def test_reentrant():
    with tempfile.TemporaryDirectory() as tmpdir:
        pid = None
        try:
            with pgserver.get_server(tmpdir) as pg:
                pid = _check_server(pg)
                with pgserver.get_server(tmpdir) as pg2:
                    assert pg2 is pg
                    _check_server(pg)

                _check_server(pg)

            assert not process_is_running(pid)
            assert pg.pgdata.exists()
        finally:
            _kill_server(pid)

def _start_server_in_separate_process(pgdata, queue_in : Optional[mp.Queue], queue_out : mp.Queue, cleanup_mode : Optional[str]):
    with pgserver.get_server(pgdata, cleanup_mode=cleanup_mode) as pg:
        pid = _check_server(pg)
        queue_out.put(pid)

        if queue_in is not None:
            _ = queue_in.get() # wait for signal
            return

def test_unix_domain_socket():
    if platform.system() == 'Windows':
        pytest.skip("This test is for unix domain sockets, which are not available on Windows.")

    long_prefix = '_'.join(['long'] + ['1234567890']*12)
    assert len(long_prefix) > 120
    prefixes = ['short', long_prefix]

    for prefix in prefixes:
        with tempfile.TemporaryDirectory(dir='/tmp/', prefix=prefix) as tmpdir:
            pid = None
            try:
                with pgserver.get_server(tmpdir) as pg:
                    pid = _check_server(pg)

                assert not process_is_running(pid)
                assert pg.pgdata.exists()
                if len(prefix) > 120:
                    assert str(tmpdir) not in pg.get_uri()
                else:
                    assert str(tmpdir) in pg.get_uri()
            finally:
                _kill_server(pid)

def test_pg_ctl():
    if platform.system() != 'Windows' and os.geteuid() == 0:
        # on Linux root, this test would fail.
        # we'd need to create a user etc to run the command, which is not worth it
        # pgserver does this internally, but not worth it for this test
        pytest.skip("This test is not run as root on Linux.")

    with tempfile.TemporaryDirectory() as tmpdir:
        pid = None
        try:
            with pgserver.get_server(tmpdir) as pg:
                output = pgserver.pg_ctl(['status'], str(pg.pgdata))
                assert 'server is running' in output.splitlines()[0]

        finally:
            _kill_server(pid)

def test_stale_postmaster():
    """  To simulate a stale postmaster.pid file, we create a postmaster.pid file by starting a server,
        back the file up, then restore the backup to the original location after killing the server.
        ( our method to kill the server is graceful to avoid running out of shmem, but this seems to also
            remove the postmaster.pid file, so we need to go to these lengths to simulate a stale postmaster.pid file )
    """
    if platform.system() != 'Windows' and os.geteuid() == 0:
        # on Linux as root, this test fails bc of permissions for the postmaster.pid file
        # we simply skip it in this case, as in practice, the permissions issue would not occur
        pytest.skip("This test is not run as root on Linux.")

    with tempfile.TemporaryDirectory() as tmpdir:
        pid = None
        pid2 = None

        try:
            with pgserver.get_server(tmpdir, cleanup_mode='stop') as pg:
                pid = _check_server(pg)
                pgdata = pg.pgdata
                postmaster_pid = pgdata / 'postmaster.pid'

                ## make a backup of the postmaster.pid file
                shutil.copy2(str(postmaster_pid), str(postmaster_pid) + '.bak')

            # restore the backup to gurantee a stale postmaster.pid file
            shutil.copy2(str(postmaster_pid) + '.bak', str(postmaster_pid))
            with pgserver.get_server(tmpdir) as pg:
                pid2 = _check_server(pg)
        finally:
            _kill_server(pid)
            _kill_server(pid2)


def test_cleanup_delete():
    with tempfile.TemporaryDirectory() as tmpdir:
        pid = None
        try:
            with pgserver.get_server(tmpdir, cleanup_mode='delete') as pg:
                pid = _check_server(pg)

            assert not process_is_running(pid)
            assert not pg.pgdata.exists()
        finally:
            _kill_server(pid)

def test_cleanup_none():
    with tempfile.TemporaryDirectory() as tmpdir:
        pid = None
        try:
            with pgserver.get_server(tmpdir, cleanup_mode=None) as pg:
                pid = _check_server(pg)

            assert process_is_running(pid)
            assert pg.pgdata.exists()
        finally:
            _kill_server(pid)

@pytest.fixture
def tmp_postgres():
    tmp_pg_data = tempfile.mkdtemp()
    with pgserver.get_server(tmp_pg_data, cleanup_mode='delete') as pg:
        yield pg

def _parse_psql_value(psql_output: str) -> str:
    """ psql's default text-mode output has three leading lines:
            header
            -------
            <value>
            (1 row)
        This helper returns the <value> line stripped, for single-cell queries.
    """
    return psql_output.splitlines()[2].strip()

def test_pgvector(tmp_postgres):
    """ pgvector extension installs AND basic vector ops work.

    Upstream pgserver bundles pgvector; this test confirms the bundled
    version (pinned at v0.8.2 in pgbuild/Makefile) actually functions —
    create a vector column, insert two vectors, and query by the L2
    distance operator (<->). Just checking CREATE EXTENSION succeeded
    wouldn't catch e.g. a missing vector.so or ABI mismatch.
    """
    tmp_postgres.psql("CREATE EXTENSION vector;")
    tmp_postgres.psql("CREATE TABLE items (id int, embedding vector(3));")
    tmp_postgres.psql("INSERT INTO items VALUES (1, '[1,2,3]'), (2, '[4,5,6]');")

    nearest = tmp_postgres.psql(
        "SELECT id FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 1;"
    )
    assert _parse_psql_value(nearest) == '1'

def test_pg_trgm(tmp_postgres):
    """ pg_trgm (trigram similarity) — the headline feature of this fork.

    Upstream pgserver does NOT install pg_trgm; our pgbuild/Makefile
    explicitly builds contrib/pg_trgm into pginstall/. This test is the
    regression guard for that build step. Verifies both the extension
    loads and similarity() returns plausible values across three cases:
    identical, unrelated, and partial-overlap strings.
    """
    create_result = tmp_postgres.psql("CREATE EXTENSION pg_trgm;")
    assert create_result.strip() == "CREATE EXTENSION"

    identical = tmp_postgres.psql("SELECT similarity('foo', 'foo');")
    assert float(_parse_psql_value(identical)) == pytest.approx(1.0)

    unrelated = tmp_postgres.psql("SELECT similarity('foo', 'zzzzzz');")
    assert float(_parse_psql_value(unrelated)) == pytest.approx(0.0)

    close_val = float(_parse_psql_value(
        tmp_postgres.psql("SELECT similarity('hello', 'helo');")
    ))
    assert 0.0 < close_val < 1.0

def _declared_postgres_version() -> str:
    """ Reads POSTGRES_VERSION from pgbuild/Makefile — the single source of
        truth for which Postgres we intended to build.
    """
    import re
    makefile = Path(__file__).resolve().parent.parent / 'pgbuild' / 'Makefile'
    match = re.search(r'^POSTGRES_VERSION\s*:?=\s*(\S+)', makefile.read_text(), re.MULTILINE)
    assert match, f"POSTGRES_VERSION not found in {makefile}"
    return match.group(1)

def test_postgres_version(tmp_postgres):
    """ The running server reports the version declared in pgbuild/Makefile.

    Catches the build-system class of bug where the Makefile declares one
    version but the built binary reports another — stale tarball under
    pgbuild/, cached pginstall/ surviving `make clean`, etc. Self-syncing:
    bumping POSTGRES_VERSION in the Makefile (e.g. 18.3 -> 18.4 for a CVE
    patch) requires no test change.
    """
    declared = _declared_postgres_version()
    reported = _parse_psql_value(tmp_postgres.psql("SHOW server_version;"))
    assert reported.startswith(declared), (
        f"Makefile declares POSTGRES_VERSION={declared!r} but running server "
        f"reports server_version={reported!r}. Likely cause: stale build "
        f"artifacts under pgbuild/ or src/pgserver/pginstall/. Try `make clean && make build`."
    )

def test_start_failure_log(caplog):
    """ Test server log contents are shown in python log when failures
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        with pgserver.get_server(tmpdir) as _:
            pass

        ## now delete some files to make it fail
        for f in Path(tmpdir).glob('**/postgresql.conf'):
            f.unlink()

        with pytest.raises(subprocess.CalledProcessError):
            with pgserver.get_server(tmpdir) as _:
                pass

        assert 'postgres: could not access the server configuration file' in caplog.text


def test_no_conflict():
    """ test we can start pgservers on two different datadirs with no conflict (eg port conflict)
    """
    pid1 = None
    pid2 = None
    try:
        with tempfile.TemporaryDirectory() as tmpdir1, tempfile.TemporaryDirectory() as tmpdir2:
            with pgserver.get_server(tmpdir1) as pg1, pgserver.get_server(tmpdir2) as pg2:
                pid1 = _check_server(pg1)
                pid2 = _check_server(pg2)
    finally:
        _kill_server(pid1)
        _kill_server(pid2)


def _reuse_deleted_datadir(prefix: str):
    """ test common scenario where we repeatedly delete the datadir and start a new server on it """
    """ NB: currently this test is not reproducing the problem """
    # one can reproduce the problem by running the following in a loop:
    # python -c 'import pixeltable as pxt; pxt.Client()'; rm -rf ~/.pixeltable/; python -c 'import pixeltable as pxt; pxt.Client()'
    # which creates a database with more contents etc
    tmpdir = tempfile.mkdtemp(prefix=prefix)
    pgdata = Path(tmpdir) / 'pgdata'
    server_processes = []
    shmem_ids = []

    num_tries = 3
    try:
        for _ in range(num_tries):
            assert not pgdata.exists()

            queue_from_child = mp.Queue()
            child = mp.Process(target=_start_server_in_separate_process, args=(pgdata, None, queue_from_child, None))
            child.start()
            # wait for child to start server
            curr_pid = queue_from_child.get()
            child.join()
            server_proc = psutil.Process(curr_pid)
            assert server_proc.is_running()
            server_processes.append(server_proc)
            postmaster = pgserver.utils.PostmasterInfo.read_from_pgdata(pgdata)

            if postmaster.shmget_id is not None:
                shmem_ids.append(postmaster.shmget_id)

            if platform.system() == 'Windows':
                # windows will not allow deletion of the directory while the server is running
                _kill_server(server_proc)

            shutil.rmtree(pgdata)
    finally:
        if platform.system() != 'Windows':
            # if sysv_ipc is installed (eg locally), remove the shared memory segment
            # done this way because of CI/CD issues with sysv_ipc
            # this avoids having to restart the machine to clear the shared memory
            try:
                import sysv_ipc
                do_shmem_cleanup = True
            except ImportError:
                do_shmem_cleanup = False
                logging.warning("sysv_ipc not installed, skipping shared memory cleanup...")

            if do_shmem_cleanup:
                for shmid in shmem_ids:
                    try:
                        sysv_ipc.remove_shared_memory(shmid)
                    except sysv_ipc.ExistentialError as e:
                        logging.info(f"shared memory already removed: {e}")

        for proc in server_processes:
            _kill_server(proc)

    shutil.rmtree(tmpdir)

def test_reuse_deleted_datadir_short():
    """ test that new server starts normally on same datadir after datadir is deleted
    """
    _reuse_deleted_datadir('short_prefix')

def test_reuse_deleted_datadir_long():
    """ test that new server starts normally on same datadir after datadir is deleted
    """
    long_prefix = '_'.join(['long_prefix'] + ['1234567890']*12)
    assert len(long_prefix) > 120
    _reuse_deleted_datadir(long_prefix)

def test_multiprocess_shared():
    """ Test that multiple processes can share the same server.

        1. get server in a child process,
        2. then, get server in the parent process
        3. then, exiting the child process
        4. checking the parent can still use the server.
    """
    pid = None
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            queue_to_child = mp.Queue()
            queue_from_child = mp.Queue()
            child = mp.Process(target=_start_server_in_separate_process, args=(tmpdir,queue_to_child,queue_from_child, 'stop'))
            child.start()
            # wait for child to start server
            server_pid_child = queue_from_child.get()

            with pgserver.get_server(tmpdir) as pg:
                server_pid_parent = _check_server(pg)
                assert server_pid_child == server_pid_parent

                # tell child to continue
                queue_to_child.put(None)
                child.join()

                # check server still works
                _check_server(pg)

            assert not process_is_running(server_pid_parent)
    finally:
        _kill_server(pid)
