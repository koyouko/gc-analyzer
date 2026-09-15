"""Deployment tests use tiny release archives and a recording installer only."""

import hashlib
import io
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import textwrap

import pytest


ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "offline/deploy.sh"
ARCHIVE = "gc-analyzer-rhel8.10-x86_64-offline.tar.gz"
BUNDLE = "gc-analyzer-offline"
INSTALLER = textwrap.dedent(
    r"""
    #!/bin/bash
    set -euo pipefail
    [[ "$#" == 0 ]]
    [[ "$EUID" == 0 ]]
    [[ "$(stat -c %a ..)" == 755 ]]
    [[ "$(stat -c '%u:%g' ..)" == 0:0 ]]
    printf '%s\n' "$PWD" > "$DEPLOY_TEST_LOG"
    [[ -f data && "$(< data)" == 'payload' ]]
    exit "${DEPLOY_TEST_STATUS:-0}"
    """
).lstrip().encode()


def entry(name, kind="file", data=b"payload", link="", mode=0o644):
    member = tarfile.TarInfo(name)
    member.mode = mode
    member.type = {
        "file": tarfile.REGTYPE,
        "directory": tarfile.DIRTYPE,
        "symlink": tarfile.SYMTYPE,
        "hardlink": tarfile.LNKTYPE,
        "fifo": tarfile.FIFOTYPE,
        "device": tarfile.CHRTYPE,
    }[kind]
    member.linkname = link
    member.size = len(data) if kind == "file" else 0
    return member, io.BytesIO(data) if kind == "file" else None


def write_release(directory, extra=(), installer=None, chunk_size=137):
    directory.mkdir(parents=True, exist_ok=True)
    content = io.BytesIO()
    with tarfile.open(fileobj=content, mode="w:gz") as archive:
        members = [
            entry(BUNDLE, "directory", mode=0o755),
            entry(BUNDLE + "/data"),
            installer or entry(BUNDLE + "/install-offline.sh", data=INSTALLER, mode=0o755),
            *extra,
        ]
        for info, data in members:
            archive.addfile(info, data)
    blob = content.getvalue()
    records = []
    for number, offset in enumerate(range(0, len(blob), chunk_size)):
        name = "bundle.part-{:04d}".format(number)
        part = blob[offset:offset + chunk_size]
        (directory / name).write_bytes(part)
        records.append("{}  {}\n".format(hashlib.sha256(part).hexdigest(), name))
    (directory / "parts.sha256").write_text("".join(records))
    (directory / "archive.sha256").write_text(
        "{}  {}\n".format(hashlib.sha256(blob).hexdigest(), ARCHIVE)
    )


@pytest.fixture
def deployment(tmp_path):
    if os.geteuid() != 0:
        pytest.skip("deployment behavioral tests require a root test container")
    bash = shutil.which("bash")
    assert bash
    major = subprocess.check_output(
        [bash, "-c", 'printf "%s" "${BASH_VERSINFO[0]}"'], text=True
    )
    if int(major) < 4:
        pytest.skip("deployment requires Bash 4 or newer")
    root = tmp_path.resolve()
    release = root / "release with spaces"
    write_release(release)
    temporary = root / "temporary"
    temporary.mkdir()
    binaries = root / "bin"
    binaries.mkdir()
    for command in ("docker", "curl", "wget", "dnf", "yum", "tar", "gzip"):
        path = binaries / command
        path.write_text('#!/bin/bash\nprintf "%s\\n" "forbidden command" >&2\nexit 98\n')
        path.chmod(0o755)
    environment = {
        key: value for key, value in os.environ.items()
        if not key.startswith(("GC_", "DEPLOY_TEST_", "PYTHON"))
        and key not in ("BASH_ENV", "ENV", "CDPATH", "TMPDIR", "TMP", "TEMP")
    }
    environment.update(
        GC_ANALYZER_RELEASE_DIR=str(release),
        TMPDIR=str(temporary),
        PATH=str(binaries) + os.pathsep + os.environ["PATH"],
        DEPLOY_TEST_LOG=str(root / "installer.log"),
    )
    return {"root": root, "release": release, "temp": temporary, "env": environment, "bash": bash}


def run_deploy(deployment, *arguments, **environment):
    return subprocess.run(
        [deployment["bash"], str(DEPLOY), *arguments],
        cwd=deployment["root"],
        env={**deployment["env"], **environment},
        capture_output=True, text=True, timeout=20,
    )


def assert_rejected(deployment, result):
    assert result.returncode != 0, result.stdout
    assert "ERROR" in result.stderr, result.stderr
    assert not Path(deployment["env"]["DEPLOY_TEST_LOG"]).exists()
    assert not list(deployment["temp"].iterdir())


def rewrite_parts(release, records):
    (release / "parts.sha256").write_text("".join(records))


def test_verified_parts_install_without_tar_docker_or_downloads(deployment):
    before = {path.name: path.read_bytes() for path in deployment["release"].iterdir()}
    result = run_deploy(deployment)
    assert result.returncode == 0, result.stderr
    extracted = Path(Path(deployment["env"]["DEPLOY_TEST_LOG"]).read_text().strip())
    assert extracted.name == BUNDLE
    assert extracted.parent.parent == deployment["temp"]
    assert not extracted.exists()
    assert not list(deployment["temp"].iterdir())
    assert {path.name: path.read_bytes() for path in deployment["release"].iterdir()} == before


@pytest.mark.parametrize("status", [1, 42])
def test_installer_failure_propagates_and_cleans_temporary_files(deployment, status):
    result = run_deploy(deployment, DEPLOY_TEST_STATUS=str(status))
    assert result.returncode == status, result.stderr
    assert Path(deployment["env"]["DEPLOY_TEST_LOG"]).exists()
    assert not list(deployment["temp"].iterdir())


@pytest.mark.parametrize("argument", ["--help", "--container-test", "--skip-root", "extra"])
def test_rejects_all_arguments(deployment, argument):
    assert_rejected(deployment, run_deploy(deployment, argument))


@pytest.mark.parametrize("kind", ["empty", "relative", "missing", "root", "file", "symlink",
                                 "symlink-parent", "dot", "parent", "trailing-slash"])
def test_release_path_must_be_absolute_normalized_real_directory(deployment, kind):
    release = deployment["release"]
    if kind == "empty":
        release = ""
    elif kind == "relative":
        release = release.name
    elif kind == "missing":
        release = deployment["root"] / "missing"
    elif kind == "root":
        release = "/"
    elif kind == "file":
        release = release / "parts.sha256"
    elif kind in ("symlink", "symlink-parent"):
        link = deployment["root"] / "link"
        link.symlink_to(release if kind == "symlink" else deployment["root"], target_is_directory=True)
        release = link if kind == "symlink" else link / release.name
    elif kind == "dot":
        release = str(release) + "/."
    elif kind == "parent":
        release = str(release) + "/../" + release.name
    elif kind == "trailing-slash":
        release = str(release) + "/"
    assert_rejected(deployment, run_deploy(deployment, GC_ANALYZER_RELEASE_DIR=str(release)))


@pytest.mark.parametrize("filename", ["parts.sha256", "archive.sha256", "bundle.part-0000"])
@pytest.mark.parametrize("kind", ["missing", "symlink", "directory", "fifo"])
def test_manifest_and_parts_must_be_regular_nonsymlink_files(deployment, filename, kind):
    path = deployment["release"] / filename
    outside = deployment["root"] / "outside"
    path.rename(outside)
    if kind == "symlink":
        path.symlink_to(outside)
    elif kind == "directory":
        path.mkdir()
    elif kind == "fifo":
        os.mkfifo(path)
    assert_rejected(deployment, run_deploy(deployment))


@pytest.mark.parametrize("mutation", ["empty", "malformed-hash", "one-space", "duplicate", "reordered",
                                     "gap", "absolute", "traversal", "shell", "extra-column", "crlf"])
def test_rejects_malformed_or_unsafe_part_manifests(deployment, mutation):
    release = deployment["release"]
    records = (release / "parts.sha256").read_text().splitlines(keepends=True)
    if mutation == "empty":
        records = []
    elif mutation == "malformed-hash":
        records[0] = "z" + records[0][1:]
    elif mutation == "one-space":
        records[0] = records[0].replace("  ", " ")
    elif mutation == "duplicate":
        records.insert(1, records[0])
    elif mutation == "reordered":
        records.reverse()
    elif mutation == "gap":
        records.pop(1)
    elif mutation == "extra-column":
        records[0] = records[0].rstrip() + " extra\n"
    elif mutation == "crlf":
        records[0] = records[0].replace("\n", "\r\n")
    else:
        filename = {"absolute": "/bundle.part-0000", "traversal": "../bundle.part-0000",
                    "shell": "$(touch injected)"}[mutation]
        records[0] = records[0].replace("bundle.part-0000", filename)
    rewrite_parts(release, records)
    assert_rejected(deployment, run_deploy(deployment))
    assert not (deployment["root"] / "injected").exists()


@pytest.mark.parametrize("extra", ["bundle.part-9999", "bundle.part-0000.bak", "bundle.part-evil", "unexpected"])
def test_rejects_unlisted_files(deployment, extra):
    (deployment["release"] / extra).write_bytes(b"unexpected")
    assert_rejected(deployment, run_deploy(deployment))


@pytest.mark.parametrize("kind", ["chunk", "archive", "archive-name", "archive-duplicate", "empty-chunk"])
def test_rejects_hash_mismatch_or_bad_archive_manifest(deployment, kind):
    release = deployment["release"]
    if kind in ("chunk", "empty-chunk"):
        (release / "bundle.part-0000").write_bytes(b"corrupted" if kind == "chunk" else b"")
    else:
        path = release / "archive.sha256"
        record = path.read_text()
        if kind == "archive":
            record = "0" * 64 + record[64:]
        elif kind == "archive-name":
            record = record.replace(ARCHIVE, "../" + ARCHIVE)
        else:
            record += record
        path.write_text(record)
    assert_rejected(deployment, run_deploy(deployment))


@pytest.mark.parametrize("name,kind,link", [
    ("../escape", "file", ""),
    ("/tmp/deploy-escape", "file", ""),
    (BUNDLE + "/../escape", "file", ""),
    (BUNDLE + "/./nested", "file", ""),
    (BUNDLE + "//nested", "file", ""),
    ("wrong-root/data", "file", ""),
    (BUNDLE + "/data", "file", ""),
    (BUNDLE + "/fifo", "fifo", ""),
    (BUNDLE + "/device", "device", ""),
    (BUNDLE + "/link", "symlink", "../../escape"),
    (BUNDLE + "/link", "symlink", "/etc/passwd"),
    (BUNDLE + "/link", "symlink", "missing"),
    (BUNDLE + "/link", "symlink", "link"),
    (BUNDLE + "/link", "hardlink", "../escape"),
    (BUNDLE + "/link", "hardlink", "/etc/passwd"),
])
def test_unsafe_archive_members_never_reach_installer(deployment, name, kind, link):
    shutil.rmtree(deployment["release"])
    write_release(deployment["release"], [entry(name, kind, link=link)])
    assert_rejected(deployment, run_deploy(deployment))
    assert not (deployment["temp"] / "escape").exists()


def test_rejects_members_below_symlinks_before_extraction(deployment):
    shutil.rmtree(deployment["release"])
    write_release(deployment["release"], [
        entry(BUNDLE + "/target", "directory"),
        entry(BUNDLE + "/link", "symlink", link="target"),
        entry(BUNDLE + "/link/child"),
    ])
    assert_rejected(deployment, run_deploy(deployment))


def test_symlink_parent_traversal_cannot_hide_escape_through_normalization(deployment):
    shutil.rmtree(deployment["release"])
    write_release(deployment["release"], [
        entry(BUNDLE + "/alias", "symlink", link="."),
        entry(BUNDLE + "/escape", "symlink", link="alias/../data"),
    ])
    assert_rejected(deployment, run_deploy(deployment))


def test_safe_internal_symlinks_hardlinks_and_executable_modes_survive(deployment):
    installer = INSTALLER.replace(
        b'exit "${DEPLOY_TEST_STATUS:-0}"',
        b'[[ -L nested/link && "$(< nested/link)" == payload ]]\n'
        b'[[ -f hardlink && "$(< hardlink)" == payload ]]\n'
        b'[[ -x tool ]]\nexit 0',
    )
    shutil.rmtree(deployment["release"])
    write_release(deployment["release"], [
        entry(BUNDLE + "/nested", "directory", mode=0o750),
        entry(BUNDLE + "/nested/link", "symlink", link="../data"),
        entry(BUNDLE + "/hardlink", "hardlink", link=BUNDLE + "/data"),
        entry(BUNDLE + "/tool", mode=0o755),
    ], installer=entry(BUNDLE + "/install-offline.sh", data=installer, mode=0o755))
    result = run_deploy(deployment)
    assert result.returncode == 0, result.stderr
    assert not list(deployment["temp"].iterdir())


@pytest.mark.parametrize("kind", ["not-executable", "symlink", "setuid"])
def test_installer_must_be_real_executable_without_special_permissions(deployment, kind):
    shutil.rmtree(deployment["release"])
    installer = entry(BUNDLE + "/install-offline.sh", data=INSTALLER,
                      mode=0o644 if kind == "not-executable" else 0o4755)
    if kind == "symlink":
        installer = entry(BUNDLE + "/install-offline.sh", "symlink", link="data")
    write_release(deployment["release"], installer=installer)
    assert_rejected(deployment, run_deploy(deployment))


def test_nonroot_cannot_bypass_root_requirement(tmp_path):
    if os.geteuid() == 0:
        pytest.skip("run this case as an ordinary user")
    bash = shutil.which("bash")
    result = subprocess.run(
        [bash, str(DEPLOY)], capture_output=True, text=True,
        env={**os.environ, "GC_ANALYZER_SKIP_ROOT_CHECK": "1", "GC_ANALYZER_RELEASE_DIR": str(tmp_path)},
    )
    assert result.returncode != 0
    assert "root" in result.stderr.lower(), result.stderr
