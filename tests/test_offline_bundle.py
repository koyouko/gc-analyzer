import hashlib
import itertools
import os
import shutil
import subprocess
import stat
import shlex
import tarfile
from pathlib import Path, PurePosixPath

import pytest


ROOT = Path(__file__).resolve().parents[1]
COPY_APPLICATION_SCRIPT = ROOT / "offline/copy-application.sh"
BUNDLE_BUILDER_SCRIPT = ROOT / "offline/build-bundle.sh"
INVENTORY_SCRIPT = ROOT / "offline/generate-inventory.py"
OFFLINE_INSTALLER_SCRIPT = ROOT / "offline/install-offline.sh"
OFFLINE_VERIFIER_SCRIPT = ROOT / "offline/verify-offline.sh"
FORBIDDEN_RELEASE_NAMES = {
    ".env",
    ".venv",
    "users.json",
    ".session_secret",
    "node_modules",
    ".next",
    "__pycache__",
}


def manifest_lines(relative_path):
    return (ROOT / relative_path).read_text().splitlines()


def release_path_is_allowed(path):
    path_parts = set(PurePosixPath(path).parts)
    return FORBIDDEN_RELEASE_NAMES.isdisjoint(path_parts) and not path.endswith(
        (".db", ".pyc")
    )


def git_tracked_release_files(allowlist_entry):
    # Task 4 must copy this tracked-file expansion, never raw directory trees.
    result = subprocess.run(
        ["git", "ls-files", "--", allowlist_entry],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.splitlines()


def staged_file_paths(destination):
    return {
        path.relative_to(destination).as_posix()
        for path in destination.rglob("*")
        if path.is_file() or path.is_symlink()
    }


def run_git(repository, *arguments):
    return subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )


def commit_fixture_repository(repository):
    run_git(repository, "init", "--quiet")
    run_git(repository, "add", ".")
    run_git(
        repository,
        "-c",
        "user.name=GC Analyzer Tests",
        "-c",
        "user.email=tests@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "fixture",
    )


def git_head_file_bytes(repository, tracked_path):
    return subprocess.run(
        ["git", "-C", str(repository), "show", f"HEAD:{tracked_path}"],
        check=True,
        capture_output=True,
    ).stdout


def git_head_file_mode(repository, tracked_path):
    tree_entry = run_git(
        repository, "ls-tree", "HEAD", "--", tracked_path
    ).stdout.strip()
    assert tree_entry, f"HEAD has no tree entry for {tracked_path}"
    return tree_entry.split(maxsplit=1)[0]


@pytest.mark.parametrize(
    "tracked_path",
    [
        "gcanalyzer/.env",
        "gcanalyzer/.venv/bin/python",
        "gcanalyzer/users.json",
        "gcanalyzer/.session_secret",
        "web/node_modules/package/index.js",
        "web/.next/server/app.js",
        "gcanalyzer/__pycache__/analyzer.cpython-312.pyc",
        "seed/history.db",
    ],
)
def test_release_policy_rejects_forbidden_nested_paths(tracked_path):
    assert not release_path_is_allowed(tracked_path)


def test_offline_requirements_are_exact_and_include_ml():
    lines = [
        line.strip()
        for line in (ROOT / "requirements-offline.txt").read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    assert lines == [
        "fastapi==0.141.1",
        "uvicorn==0.52.1",
        "paramiko==5.0.0",
        "PyYAML==6.0.3",
        "scikit-learn==1.9.0",
    ]


def test_rhel8_package_roots_are_exact_and_ordered():
    assert manifest_lines("offline/rhel8-packages.txt") == [
        "python3.12",
        "python3.12-pip",
        "ca-certificates",
        "curl",
        "tar",
        "gzip",
        "shadow-utils",
        "util-linux",
        "libstdc++",
        "bash",
        "coreutils",
        "findutils",
        "grep",
        "sed",
        "gawk",
        "dnf",
        "systemd",
    ]


def test_application_release_allowlist_is_exact_and_ordered():
    assert manifest_lines("offline/app-files.txt") == [
        "frontend",
        "gcanalyzer",
        "seed",
        "web/app",
        "web/components",
        "web/lib",
        "web/next.config.js",
        "web/package.json",
        "web/package-lock.json",
        "web/tsconfig.json",
        "requirements.txt",
        "requirements-offline.txt",
        "manage-app.sh",
        "README.md",
        "architecture_and_user_guide.html",
        "prometheus.example.json",
        "offline/README.md",
    ]


def test_release_allowlist_expands_to_nonempty_safe_git_tracked_files_only():
    for allowlist_entry in manifest_lines("offline/app-files.txt"):
        tracked_files = git_tracked_release_files(allowlist_entry)

        assert tracked_files, f"allowlist entry matches no tracked files: {allowlist_entry}"
        for tracked_path in tracked_files:
            assert release_path_is_allowed(tracked_path), (
                f"forbidden tracked release path: {tracked_path}"
            )


def test_copy_application_stages_exact_tracked_allowlist_without_ignored_cache(
    tmp_path,
):
    assert COPY_APPLICATION_SCRIPT.is_file(), "tracked-only copy helper is missing"
    destination = tmp_path / "application"
    cache_directory = ROOT / "gcanalyzer" / "__pycache__"
    cache_directory_existed = cache_directory.exists()
    cache_directory.mkdir(exist_ok=True)
    ignored_cache = cache_directory / f"copy-policy-{tmp_path.name}.pyc"
    ignored_cache.write_bytes(b"ignored build artifact")

    try:
        subprocess.run(
            [str(COPY_APPLICATION_SCRIPT), str(ROOT), str(destination)],
            check=True,
        )

        expected = set()
        for entry in manifest_lines("offline/app-files.txt"):
            expected.update(git_tracked_release_files(entry))

        actual = staged_file_paths(destination)
        assert actual == expected
        assert ignored_cache.relative_to(ROOT).as_posix() not in actual
        assert not any("__pycache__" in PurePosixPath(path).parts for path in actual)
        for tracked_path in expected:
            staged_path = destination / tracked_path
            assert not staged_path.is_symlink()
            assert staged_path.read_bytes() == git_head_file_bytes(ROOT, tracked_path)
            head_mode = git_head_file_mode(ROOT, tracked_path)
            assert head_mode in {"100644", "100755"}
            assert bool(staged_path.stat().st_mode & stat.S_IXUSR) == (
                head_mode == "100755"
            )
    finally:
        ignored_cache.unlink(missing_ok=True)
        if not cache_directory_existed:
            cache_directory.rmdir()


def test_copy_application_rejects_nonempty_destination(tmp_path):
    assert COPY_APPLICATION_SCRIPT.is_file(), "tracked-only copy helper is missing"
    destination = tmp_path / "application"
    destination.mkdir()
    marker = destination / "stale.txt"
    marker.write_text("keep me")

    result = subprocess.run(
        [str(COPY_APPLICATION_SCRIPT), str(ROOT), str(destination)],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "destination must be empty" in result.stderr
    assert marker.read_text() == "keep me"


def test_copy_application_rejects_forbidden_tracked_path_before_copying(tmp_path):
    assert COPY_APPLICATION_SCRIPT.is_file(), "tracked-only copy helper is missing"
    source = tmp_path / "source"
    destination = tmp_path / "application"
    (source / "offline").mkdir(parents=True)
    (source / "payload" / "__pycache__").mkdir(parents=True)
    (source / "offline" / "app-files.txt").write_text("payload\n")
    (source / "payload" / "safe.txt").write_text("safe\n")
    (source / "payload" / "__pycache__" / "unsafe.pyc").write_bytes(b"unsafe")
    commit_fixture_repository(source)

    result = subprocess.run(
        [str(COPY_APPLICATION_SCRIPT), str(source), str(destination)],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "forbidden tracked path" in result.stderr
    assert not destination.exists()


def test_copy_application_rejects_worktree_manifest_drift(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "application"
    (source / "offline").mkdir(parents=True)
    (source / "payload").mkdir()
    (source / "alternate").mkdir()
    manifest = source / "offline" / "app-files.txt"
    manifest.write_text("payload\n")
    (source / "payload" / "from-head.txt").write_text("HEAD payload\n")
    (source / "alternate" / "worktree-only-choice.txt").write_text("alternate\n")
    commit_fixture_repository(source)
    manifest.write_text("alternate\n")

    result = subprocess.run(
        [str(COPY_APPLICATION_SCRIPT), str(source), str(destination)],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "allowlist differs from HEAD" in result.stderr
    assert not destination.exists()


def test_copy_application_rejects_committed_external_symlink_before_copying(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "application"
    external_target = tmp_path / "outside-release.txt"
    external_target.write_text("must not be packaged\n")
    (source / "offline").mkdir(parents=True)
    (source / "payload").mkdir()
    (source / "offline" / "app-files.txt").write_text("payload\n")
    (source / "payload" / "safe.txt").write_text("safe\n")
    (source / "payload" / "external-link").symlink_to(external_target)
    commit_fixture_repository(source)

    result = subprocess.run(
        [str(COPY_APPLICATION_SCRIPT), str(source), str(destination)],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "unsupported Git mode 120000" in result.stderr
    assert not destination.exists()


def builder_source():
    assert BUNDLE_BUILDER_SCRIPT.is_file(), "offline bundle builder is missing"
    return BUNDLE_BUILDER_SCRIPT.read_text()


def builder_function_body(function_name, next_function_name):
    source = builder_source()
    return source.split(f"{function_name}() {{", 1)[1].split(
        f"\n{next_function_name}() {{", 1
    )[0]


def builder_root_assignments(work_root, dist_root):
    return f"""
WORK_ROOT={shlex.quote(str(work_root))}
DIST_ROOT={shlex.quote(str(dist_root))}
STAGE_DIR="$WORK_ROOT/$STAGE_NAME"
SOURCE_SNAPSHOT_DIR="$WORK_ROOT/source-snapshot"
SOURCE_TEST_DIR="$WORK_ROOT/source-test"
NPM_WORK_DIR="$WORK_ROOT/npm-work"
NODE_DOWNLOAD_DIR="$WORK_ROOT/node-download"
ARCHIVE_PATH="$DIST_ROOT/$ARCHIVE_NAME"
"""


MANAGED_WORK_CHILDREN = [
    "STAGE_DIR",
    "SOURCE_SNAPSHOT_DIR",
    "SOURCE_TEST_DIR",
    "NPM_WORK_DIR",
    "NODE_DOWNLOAD_DIR",
]


def managed_path_values(work_root, dist_root):
    return {
        "STAGE_DIR": work_root / "gc-analyzer-offline",
        "SOURCE_SNAPSHOT_DIR": work_root / "source-snapshot",
        "SOURCE_TEST_DIR": work_root / "source-test",
        "NPM_WORK_DIR": work_root / "npm-work",
        "NODE_DOWNLOAD_DIR": work_root / "node-download",
        "ARCHIVE_PATH": dist_root / "gc-analyzer-rhel8.10-x86_64-offline.tar.gz",
    }


def managed_path_assignments(work_root, dist_root, paths):
    assignments = [
        f"WORK_ROOT={shlex.quote(str(work_root))}",
        f"DIST_ROOT={shlex.quote(str(dist_root))}",
    ]
    assignments.extend(
        f"{name}={shlex.quote(str(paths[name]))}"
        for name in [*MANAGED_WORK_CHILDREN, "ARCHIVE_PATH"]
    )
    return "\n".join(assignments)


def run_managed_layout_guard(tmp_path, assignments, protected_file):
    marker_call = tmp_path / "marker-called"
    docker_call = tmp_path / "docker-called"
    removal_call = tmp_path / "removal-called"
    clone_call = tmp_path / "clone-called"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
{assignments}
RESOLVED_UBI_IMAGE_ID=sha256:frozen-ubi
initialize_managed_root() {{ touch {shlex.quote(str(marker_call))}; }}
assert_managed_root_marker() {{ touch {shlex.quote(str(marker_call))}; }}
assert_source_snapshot() {{ :; }}
docker() {{ touch {shlex.quote(str(docker_call))}; }}
rm() {{ touch {shlex.quote(str(removal_call))}; command rm "$@"; }}
git() {{ touch {shlex.quote(str(clone_call))}; }}
repair_build_ownership
prepare_stage
create_source_snapshot
"""
    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)
    return result, marker_call, docker_call, removal_call, clone_call, protected_file


def test_bundle_builder_has_required_shell_contract_and_functions():
    source = builder_source()

    assert source.startswith("#!/usr/bin/env bash\n")
    assert "set -euo pipefail" in source
    assert "umask 022" in source
    for function_name in [
        "preflight",
        "test_source",
        "prepare_stage",
        "download_rpms",
        "download_node_runtime",
        "download_python_wheels",
        "populate_npm_cache",
        "copy_application",
        "write_version_manifest",
        "write_checksums",
        "run_clean_room",
        "create_archive",
    ]:
        assert f"{function_name}()" in source
    assert '[[ "${BASH_SOURCE[0]}" == "$0" ]]' in source


def test_bundle_builder_targets_exact_rhel_platform_and_resolvers():
    source = builder_source()

    assert 'TARGET_RHEL_VERSION="8.10"' in source
    assert 'TARGET_ARCH="x86_64"' in source
    assert 'CONTAINER_PLATFORM="linux/amd64"' in source
    assert 'UBI_IMAGE_TAG="registry.access.redhat.com/ubi8/ubi:8.10"' in source
    assert 'docker run --rm --platform "$CONTAINER_PLATFORM"' in source
    assert 'dnf download --resolve --alldeps' in source
    assert '"$RPM_ROOTS_FILE"' in source
    assert 'python3.12 -m pip download --only-binary=:all:' in source
    assert '--dest /bundle/python-wheels' in source
    assert '-r /src/requirements-offline.txt' in source


def test_bundle_builder_verifies_exact_official_node_runtime():
    source = builder_source()

    assert 'NODE_VERSION="22.22.3"' in source
    assert 'NODE_ARCHIVE="node-v${NODE_VERSION}-linux-x64.tar.xz"' in source
    assert 'https://nodejs.org/dist/v${NODE_VERSION}' in source
    assert "SHASUMS256.txt" in source
    assert "sha256sum --check" in source
    assert 'node --version' in source
    assert 'npm --version' in source


def test_bundle_builder_runs_source_gates_and_populates_linux_npm_cache():
    source = builder_source()

    assert "pytest" in source
    assert "compileall" in source
    assert "npm ci" in source
    assert "npm run typecheck" in source
    assert "npm run audit:prod" in source
    assert "npm run build" in source
    assert "npm audit --omit=dev --audit-level=high" in source
    assert "package-lock.json" in source
    assert "/bundle/npm-cache" in source
    assert "web/node_modules" not in source


def test_source_gate_uses_exact_linux_node_and_committed_isolated_web_copy():
    source = builder_source()
    body = builder_function_body("test_source", "prepare_stage")

    assert 'NODE_TEST_IMAGE_TAG="node:22.22.3-bookworm-slim"' in source
    assert 'git -C "$SOURCE_SNAPSHOT_DIR" archive --format=tar' in body
    assert '"$SOURCE_COMMIT" -- web' in body
    assert '"$SOURCE_TEST_DIR/web:/workspace"' in body
    assert 'docker run --rm --platform "$CONTAINER_PLATFORM"' in body
    assert '"$RESOLVED_NODE_TEST_IMAGE_ID"' in body
    assert 'test "$(node --version)" = "v$1"' in body
    assert 'case "$(npm --version)" in "$2".*)' in body
    assert "npm ci" in body
    assert "npm run typecheck" in body
    assert "npm run audit:prod" in body
    assert "npm run build" in body
    assert 'cd "$SOURCE_ROOT/web"' not in body
    assert '"$SOURCE_ROOT/web:/workspace"' not in body


def test_source_gate_compileall_includes_backend_seed_and_tests():
    body = builder_function_body("test_source", "prepare_stage")

    assert 'python3.12 -m compileall -q gcanalyzer seed tests' in body
    assert "require_command npm" not in builder_function_body("preflight", "test_source")


def create_source_gate_fixture(tmp_path):
    source = tmp_path / "source"
    (source / "web").mkdir(parents=True)
    (source / "gcanalyzer").mkdir()
    (source / "seed").mkdir()
    (source / "tests").mkdir()
    (source / "tests" / "test_fixture.py").write_text("# Source gate fixture\n")
    (source / "frontend").mkdir()
    (source / "frontend" / "dashboard.test.cjs").write_text("// Source gate fixture\n")
    (source / ".gitignore").write_text(".venv/\nweb/node_modules/\n")
    (source / "web" / "package.json").write_text('{"scripts": {}}\n')
    (source / "web" / "package-lock.json").write_text(
        '{"name": "fixture", "lockfileVersion": 3, "packages": {}}\n'
    )
    head_marker = source / "web" / "from-head.txt"
    head_marker.write_text("committed\n")
    commit_fixture_repository(source)
    frozen_commit = run_git(source, "rev-parse", "HEAD").stdout.strip()
    head_marker.write_text("working-tree\n")

    repository_cache = source / "web" / "node_modules"
    repository_cache.mkdir()
    repository_sentinel = repository_cache / "must-survive"
    repository_sentinel.write_text("untouched\n")
    return source, frozen_commit, repository_sentinel


def test_source_gate_behavior_uses_head_copy_without_touching_repository(tmp_path):
    source, frozen_commit, repository_sentinel = create_source_gate_fixture(tmp_path)
    work_root = tmp_path / "work"
    source_test_dir = work_root / "source-test"
    docker_log = tmp_path / "docker.log"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
WORK_ROOT={shlex.quote(str(work_root))}
SOURCE_TEST_DIR={shlex.quote(str(source_test_dir))}
SOURCE_SNAPSHOT_DIR={shlex.quote(str(source))}
SOURCE_COMMIT={shlex.quote(frozen_commit)}
RESOLVED_UBI_IMAGE_ID=sha256:ubi-image-id
RESOLVED_NODE_TEST_IMAGE_ID=sha256:node-image-id
assert_managed_output_roots() {{ :; }}
assert_source_snapshot() {{ :; }}
docker() {{
    printf '%s\\n' "$@" >> {shlex.quote(str(docker_log))}
    case " $* " in
        *sha256:node-image-id*)
            printf 'exported=%s\\n' "$(cat "$SOURCE_TEST_DIR/web/from-head.txt")" \
                >> {shlex.quote(str(docker_log))}
            ;;
    esac
}}
npm() {{ return 99; }}
test_source
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    assert repository_sentinel.read_text() == "untouched\n"
    assert not source_test_dir.exists()
    docker_arguments = docker_log.read_text()
    assert "linux/amd64" in docker_arguments
    assert "sha256:node-image-id" in docker_arguments
    assert f"{source_test_dir}/web:/workspace" in docker_arguments
    assert f"{source}/web:/workspace" not in docker_arguments
    assert "exported=committed" in docker_arguments
    assert "sha256:ubi-image-id" in docker_arguments
    assert "sha256:node-image-id" in docker_arguments


def test_source_gate_cleans_disposable_copy_when_container_fails(tmp_path):
    source, frozen_commit, repository_sentinel = create_source_gate_fixture(tmp_path)
    work_root = tmp_path / "work"
    source_test_dir = work_root / "source-test"
    docker_log = tmp_path / "docker-failed"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
WORK_ROOT={shlex.quote(str(work_root))}
SOURCE_TEST_DIR={shlex.quote(str(source_test_dir))}
SOURCE_SNAPSHOT_DIR={shlex.quote(str(source))}
SOURCE_COMMIT={shlex.quote(frozen_commit)}
RESOLVED_UBI_IMAGE_ID=sha256:ubi-image-id
RESOLVED_NODE_TEST_IMAGE_ID=sha256:node-image-id
assert_managed_output_roots() {{ :; }}
assert_source_snapshot() {{ :; }}
docker() {{
    case " $* " in
        *sha256:node-image-id*)
            touch {shlex.quote(str(docker_log))}
            return 37
            ;;
    esac
}}
npm() {{ return 99; }}
test_source
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode == 37
    assert docker_log.is_file()
    assert not source_test_dir.exists()
    assert repository_sentinel.read_text() == "untouched\n"


def test_bundle_builder_uses_approved_copy_manifest_and_clean_room_steps():
    source = builder_source()

    assert '"$COPY_APPLICATION_SCRIPT" "$SOURCE_SNAPSHOT_DIR" "$STAGE_DIR/app" "$SOURCE_COMMIT"' in source
    assert '"$SOURCE_COMMIT:offline/install-offline.sh"' in source
    assert '"$SOURCE_COMMIT:offline/verify-offline.sh"' in source
    assert "VERSIONS.txt" in source
    assert "MANIFEST.sha256" in source
    assert "sha256sum" in source
    assert '"$CLEAN_ROOM_SCRIPT" "$STAGE_DIR"' in source
    assert 'ARCHIVE_NAME="gc-analyzer-rhel8.10-x86_64-offline.tar.gz"' in source
    assert '"$ARCHIVE_PATH.sha256"' in source


def test_bundle_builder_main_orders_clean_room_strictly_before_archive():
    source = builder_source()
    main_body = source.split("main() {", 1)[1].split("\n}", 1)[0]

    expected_order = [
        "preflight",
        "test_source",
        "prepare_stage",
        "download_rpms",
        "download_node_runtime",
        "download_python_wheels",
        "populate_npm_cache",
        "copy_application",
        "write_version_manifest",
        "write_checksums",
        "run_clean_room",
        "create_archive",
    ]
    calls = [line.strip() for line in main_body.splitlines() if line.strip()]
    assert calls == expected_order


def test_bundle_builder_does_not_archive_when_clean_room_fails(tmp_path):
    marker = tmp_path / "archive-created"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
preflight() {{ :; }}
test_source() {{ :; }}
prepare_stage() {{ :; }}
download_rpms() {{ :; }}
download_node_runtime() {{ :; }}
download_python_wheels() {{ :; }}
populate_npm_cache() {{ :; }}
copy_application() {{ :; }}
write_version_manifest() {{ :; }}
write_checksums() {{ :; }}
run_clean_room() {{ return 42; }}
create_archive() {{ touch {marker!s}; }}
main
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode == 42
    assert not marker.exists()


def test_bundle_builder_places_ubi_image_before_container_command():
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
RESOLVED_UBI_IMAGE_ID=sha256:frozen-ubi
docker() {{ printf '%s\n' "$@"; }}
run_ubi -v /host:/container -- bash -c 'printf ignored'
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "run",
        "--rm",
        "--platform",
        "linux/amd64",
        "-v",
        "/host:/container",
        "sha256:frozen-ubi",
        "bash",
        "-c",
        "printf ignored",
    ]


def test_bundle_builder_rejects_lexical_path_escape(tmp_path):
    work_root = tmp_path / "work"
    escaped_stage = work_root / ".." / "outside"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
require_descendant \
    {shlex.quote(str(escaped_stage))} \
    {shlex.quote(str(work_root))} \
    stage
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode != 0
    assert "stage must be beneath" in result.stderr


def test_bundle_builder_has_valid_bash_syntax():
    result = subprocess.run(
        ["bash", "-n", str(BUNDLE_BUILDER_SCRIPT)],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_python_gate_is_exact_ubi_810_amd64_python312_with_pinned_tests():
    source = builder_source()
    body = builder_function_body("test_source", "prepare_stage")

    assert 'PYTEST_VERSION="9.1.1"' in source
    assert '"$RESOLVED_UBI_IMAGE_ID"' in body
    assert '--platform "$CONTAINER_PLATFORM"' in body
    assert 'test "$VERSION_ID" = "$1"' in body
    assert '_ "$TARGET_RHEL_VERSION" "$PYTEST_VERSION"' in body
    assert 'test "$(uname -m)" = "x86_64"' in body
    assert 'sys.version_info[:2] == (3, 12)' in body
    assert "dnf install -y python3.12 python3.12-pip git tar gzip" in body
    assert "python3.12 -m pip install" in body
    assert '-r /source/requirements-offline.txt "pytest==$2"' in body
    assert "python3.12 -m pytest -q" in body
    assert "python_bin" not in body
    assert ".venv/bin/python" not in body


def test_builder_freezes_clean_source_commit_and_uses_standalone_snapshot():
    source = builder_source()
    preflight = builder_function_body("preflight", "test_source")

    assert 'STARTUP_SOURCE_COMMIT=$(git -C "$ORIGINAL_SOURCE_ROOT" rev-parse HEAD)' in source
    assert 'SOURCE_COMMIT=${GC_ANALYZER_SOURCE_COMMIT:-"$STARTUP_SOURCE_COMMIT"}' in source
    assert 'git -C "$ORIGINAL_SOURCE_ROOT" diff --quiet "$SOURCE_COMMIT" --' in preflight
    assert 'git -C "$ORIGINAL_SOURCE_ROOT" diff --cached --quiet "$SOURCE_COMMIT" --' in preflight
    assert 'git -C "$ORIGINAL_SOURCE_ROOT" rev-parse HEAD' in preflight
    assert "create_source_snapshot" in preflight
    assert 'git clone --no-local --no-checkout' in source
    assert 'checkout --detach "$SOURCE_COMMIT"' in source
    assert 'rev-parse HEAD' in builder_function_body(
        "assert_source_snapshot", "resolve_image"
    )


def test_later_bundle_inputs_are_read_only_from_frozen_snapshot():
    source = builder_source()

    assert 'RPM_ROOTS_FILE="$SOURCE_SNAPSHOT_DIR/offline/rhel8-packages.txt"' in source
    assert 'COPY_APPLICATION_SCRIPT="$SOURCE_SNAPSHOT_DIR/offline/copy-application.sh"' in source
    assert '"$COPY_APPLICATION_SCRIPT" "$SOURCE_SNAPSHOT_DIR" "$STAGE_DIR/app" "$SOURCE_COMMIT"' in source
    assert 'show "$SOURCE_COMMIT:web/package.json"' in source
    assert 'show "$SOURCE_COMMIT:web/package-lock.json"' in source
    assert 'show "$SOURCE_COMMIT:requirements-offline.txt"' in source
    assert '"$SOURCE_COMMIT:offline/install-offline.sh"' in source
    assert '"$SOURCE_COMMIT:offline/verify-offline.sh"' in source
    assert 'printf \'source_commit=%s\\n\' "$SOURCE_COMMIT"' in source
    for function_name, next_name in [
        ("test_source", "prepare_stage"),
        ("download_rpms", "download_node_runtime"),
        ("download_python_wheels", "populate_npm_cache"),
        ("populate_npm_cache", "copy_application"),
        ("copy_application", "write_version_manifest"),
        ("write_version_manifest", "write_checksums"),
        ("run_clean_room", "create_archive"),
        ("create_archive", "main"),
    ]:
        assert '"$ORIGINAL_SOURCE_ROOT' not in builder_function_body(
            function_name, next_name
        )


def test_copy_application_can_package_explicit_commit_after_head_moves(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "application"
    (source / "offline").mkdir(parents=True)
    (source / "payload").mkdir()
    (source / "offline" / "app-files.txt").write_text("payload\n")
    payload = source / "payload" / "version.txt"
    payload.write_text("frozen\n")
    commit_fixture_repository(source)
    frozen_commit = run_git(source, "rev-parse", "HEAD").stdout.strip()
    payload.write_text("later\n")
    run_git(source, "add", "payload/version.txt")
    run_git(
        source,
        "-c",
        "user.name=GC Analyzer Tests",
        "-c",
        "user.email=tests@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "later",
    )

    subprocess.run(
        [
            str(COPY_APPLICATION_SCRIPT),
            str(source),
            str(destination),
            frozen_commit,
        ],
        check=True,
    )

    assert (destination / "payload/version.txt").read_text() == "frozen\n"


def test_resolver_images_are_pulled_validated_and_frozen_by_id():
    source = builder_source()
    preflight = builder_function_body("preflight", "test_source")
    run_ubi_body = builder_function_body("run_ubi", "repair_build_ownership")

    assert 'UBI_IMAGE_TAG="registry.access.redhat.com/ubi8/ubi:8.10"' in source
    assert 'NODE_TEST_IMAGE_TAG="node:22.22.3-bookworm-slim"' in source
    assert 'docker pull --platform "$CONTAINER_PLATFORM" "$UBI_IMAGE_TAG"' in preflight
    assert 'docker pull --platform "$CONTAINER_PLATFORM" "$NODE_TEST_IMAGE_TAG"' in preflight
    assert "resolve_image" in preflight
    assert "RepoDigests" in source
    assert '"$RESOLVED_UBI_IMAGE_ID"' in run_ubi_body
    assert '"$UBI_IMAGE_TAG"' not in run_ubi_body
    assert 'test "$(uname -m)" = "x86_64"' in preflight
    assert 'test "$(node --version)" = "v$1"' in preflight
    assert 'case "$(npm --version)" in "$2".*)' in preflight


def test_writable_containers_propagate_host_identity_and_repair_ownership():
    source = builder_source()
    test_source_body = builder_function_body("test_source", "prepare_stage")
    npm_body = builder_function_body("populate_npm_cache", "copy_application")
    prepare_body = builder_function_body("prepare_stage", "download_rpms")

    assert 'HOST_UID=$(id -u)' in source
    assert 'HOST_GID=$(id -g)' in source
    for body in (test_source_body, npm_body):
        assert '--user "$HOST_UID:$HOST_GID"' in body
        assert '-e HOME=/tmp/' in body
    assert "repair_build_ownership" in prepare_body
    assert source.count("trap repair_container_ownership EXIT") >= 4
    assert 'chown -R "$HOST_UID:$HOST_GID"' in source


def test_prepare_stage_repairs_ownership_before_removing_stale_output(tmp_path):
    work_root = tmp_path / "work"
    dist_root = tmp_path / "dist"
    stage = work_root / "gc-analyzer-offline"
    stale = stage / "root-owned/stale.txt"
    stale.parent.mkdir(parents=True)
    stale.write_text("stale\n")
    dist_root.mkdir()
    marker_name = ".gc-analyzer-bundle-root"
    marker_magic = "GC_ANALYZER_BUNDLE_MANAGED_ROOT_V1"
    (work_root / marker_name).write_text(marker_magic)
    (dist_root / marker_name).write_text(marker_magic)
    repair_marker = tmp_path / "ownership-repaired"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
{builder_root_assignments(work_root, dist_root)}
repair_build_ownership() {{ touch {shlex.quote(str(repair_marker))}; }}
prepare_stage
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    assert repair_marker.is_file()
    assert not stale.exists()
    assert (stage / "app").is_dir()


def test_ownership_repair_passes_numeric_host_uid_and_gid(tmp_path):
    docker_log = tmp_path / "docker-ownership.log"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
{builder_root_assignments(tmp_path / 'work', tmp_path / 'dist')}
HOST_UID=1234
HOST_GID=5678
RESOLVED_UBI_IMAGE_ID=sha256:frozen-ubi
docker() {{ printf '%s\\n' "$@" > {shlex.quote(str(docker_log))}; }}
repair_build_ownership
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    docker_arguments = docker_log.read_text().splitlines()
    assert "HOST_UID=1234" in docker_arguments
    assert "HOST_GID=5678" in docker_arguments
    assert "sha256:frozen-ubi" in docker_arguments
    assert 'chown -R "$HOST_UID:$HOST_GID" /work /dist' in docker_arguments


@pytest.mark.parametrize(
    "unsafe_layout",
    [
        "work_root",
        "dist_root",
        "empty_work_root",
        "empty_dist_root",
        "unmarked_nonempty",
        "symlink_root",
        "wrong_marker",
        "symlink_marker",
        "equal_roots",
        "nested_roots",
    ],
)
def test_managed_output_roots_reject_unsafe_layout_before_docker_or_removal(
    tmp_path, unsafe_layout
):
    work_root = tmp_path / "work"
    dist_root = tmp_path / "dist"
    protected = tmp_path / "protected.txt"
    protected.write_text("must remain\n")
    if unsafe_layout == "work_root":
        work_root = Path("/")
    elif unsafe_layout == "dist_root":
        dist_root = Path("/")
    elif unsafe_layout == "empty_work_root":
        work_root = ""
    elif unsafe_layout == "empty_dist_root":
        dist_root = ""
    elif unsafe_layout == "unmarked_nonempty":
        work_root.mkdir()
        (work_root / "existing.txt").write_text("do not chown or remove\n")
    elif unsafe_layout == "symlink_root":
        real_root = tmp_path / "real-work"
        real_root.mkdir()
        work_root.symlink_to(real_root, target_is_directory=True)
    elif unsafe_layout == "wrong_marker":
        work_root.mkdir()
        (work_root / ".gc-analyzer-bundle-root").write_text("wrong")
    elif unsafe_layout == "symlink_marker":
        work_root.mkdir()
        marker_target = tmp_path / "marker-target"
        marker_target.write_text("GC_ANALYZER_BUNDLE_MANAGED_ROOT_V1")
        (work_root / ".gc-analyzer-bundle-root").symlink_to(marker_target)
    elif unsafe_layout == "equal_roots":
        dist_root = work_root
    else:
        dist_root = work_root / "nested-dist"
    docker_marker = tmp_path / "docker-called"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
{builder_root_assignments(work_root, dist_root)}
RESOLVED_UBI_IMAGE_ID=sha256:frozen-ubi
docker() {{ touch {shlex.quote(str(docker_marker))}; }}
repair_build_ownership
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode != 0
    assert not docker_marker.exists()
    assert protected.read_text() == "must remain\n"
    if unsafe_layout == "unmarked_nonempty":
        assert (work_root / "existing.txt").read_text() == "do not chown or remove\n"


def test_managed_output_roots_reject_unsafe_descendant_before_docker(tmp_path):
    work_root = tmp_path / "work"
    dist_root = tmp_path / "dist"
    docker_marker = tmp_path / "docker-called"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
{builder_root_assignments(work_root, dist_root)}
STAGE_DIR={shlex.quote(str(tmp_path / 'outside-stage'))}
RESOLVED_UBI_IMAGE_ID=sha256:frozen-ubi
docker() {{ touch {shlex.quote(str(docker_marker))}; }}
repair_build_ownership
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode != 0
    assert not docker_marker.exists()
    assert not work_root.exists()
    assert not dist_root.exists()


@pytest.mark.parametrize("child_name", MANAGED_WORK_CHILDREN)
def test_managed_work_children_must_be_strict_descendants_before_side_effects(
    tmp_path, child_name
):
    work_root = tmp_path / "work"
    dist_root = tmp_path / "dist"
    work_root.mkdir()
    protected = work_root / "protected.txt"
    protected.write_text("must survive\n")
    paths = managed_path_values(work_root, dist_root)
    paths[child_name] = work_root
    outcome = run_managed_layout_guard(
        tmp_path,
        managed_path_assignments(work_root, dist_root, paths),
        protected,
    )
    result, marker_call, docker_call, removal_call, clone_call, protected = outcome

    assert result.returncode != 0
    assert protected.read_text() == "must survive\n"
    assert not marker_call.exists()
    assert not docker_call.exists()
    assert not removal_call.exists()
    assert not clone_call.exists()


@pytest.mark.parametrize(
    ("first_child", "second_child", "relationship"),
    [
        (*pair, relationship)
        for pair, relationship in itertools.product(
            itertools.combinations(MANAGED_WORK_CHILDREN, 2),
            ["equal", "first_parent", "second_parent"],
        )
    ],
)
def test_managed_work_children_must_be_pairwise_disjoint_before_side_effects(
    tmp_path, first_child, second_child, relationship
):
    work_root = tmp_path / "work"
    dist_root = tmp_path / "dist"
    work_root.mkdir()
    protected = work_root / "protected.txt"
    protected.write_text("must survive\n")
    paths = managed_path_values(work_root, dist_root)
    if relationship == "equal":
        paths[second_child] = paths[first_child]
    elif relationship == "first_parent":
        paths[second_child] = paths[first_child] / "nested"
    else:
        paths[first_child] = paths[second_child] / "nested"
    outcome = run_managed_layout_guard(
        tmp_path,
        managed_path_assignments(work_root, dist_root, paths),
        protected,
    )
    result, marker_call, docker_call, removal_call, clone_call, protected = outcome

    assert result.returncode != 0
    assert protected.read_text() == "must survive\n"
    assert not marker_call.exists()
    assert not docker_call.exists()
    assert not removal_call.exists()
    assert not clone_call.exists()


@pytest.mark.parametrize("archive_layout", ["dist_root", "work_root", "stage"])
def test_archive_path_must_be_strict_and_isolated_before_side_effects(
    tmp_path, archive_layout
):
    work_root = tmp_path / "work"
    dist_root = tmp_path / "dist"
    work_root.mkdir()
    protected = work_root / "protected.txt"
    protected.write_text("must survive\n")
    paths = managed_path_values(work_root, dist_root)
    if archive_layout == "dist_root":
        paths["ARCHIVE_PATH"] = dist_root
    elif archive_layout == "work_root":
        paths["ARCHIVE_PATH"] = work_root
    else:
        paths["ARCHIVE_PATH"] = paths["STAGE_DIR"]
    outcome = run_managed_layout_guard(
        tmp_path,
        managed_path_assignments(work_root, dist_root, paths),
        protected,
    )
    result, marker_call, docker_call, removal_call, clone_call, protected = outcome

    assert result.returncode != 0
    assert protected.read_text() == "must survive\n"
    assert not marker_call.exists()
    assert not docker_call.exists()
    assert not removal_call.exists()
    assert not clone_call.exists()


@pytest.mark.parametrize(
    ("root_name", "trailing_slashes"),
    [
        ("work", "/"),
        ("work", "///"),
        ("dist", "/"),
        ("dist", "///"),
    ],
)
def test_managed_output_roots_reject_symlink_aliases_with_trailing_slashes(
    tmp_path, root_name, trailing_slashes
):
    work_root = tmp_path / "work"
    dist_root = tmp_path / "dist"
    symlink_target = tmp_path / f"{root_name}-target"
    symlink_target.mkdir()
    symlink_alias = tmp_path / f"{root_name}-alias"
    symlink_alias.symlink_to(symlink_target, target_is_directory=True)
    if root_name == "work":
        work_root = f"{symlink_alias}{trailing_slashes}"
    else:
        dist_root = f"{symlink_alias}{trailing_slashes}"
    docker_marker = tmp_path / "docker-called"
    removal_marker = tmp_path / "removal-called"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
{builder_root_assignments(work_root, dist_root)}
RESOLVED_UBI_IMAGE_ID=sha256:frozen-ubi
docker() {{ touch {shlex.quote(str(docker_marker))}; }}
rm() {{ touch {shlex.quote(str(removal_marker))}; command rm "$@"; }}
repair_build_ownership
prepare_stage
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode != 0
    assert not (symlink_target / ".gc-analyzer-bundle-root").exists()
    assert not docker_marker.exists()
    assert not removal_marker.exists()


def test_managed_output_roots_reject_trailing_separators_before_side_effects(tmp_path):
    work_root = tmp_path / "work"
    dist_root = tmp_path / "dist"
    marker_call = tmp_path / "marker-called"
    docker_call = tmp_path / "docker-called"
    removal_call = tmp_path / "removal-called"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
{builder_root_assignments(f'{work_root}///', f'{dist_root}//')}
RESOLVED_UBI_IMAGE_ID=sha256:frozen-ubi
initialize_managed_root() {{ touch {shlex.quote(str(marker_call))}; }}
assert_managed_root_marker() {{ touch {shlex.quote(str(marker_call))}; }}
docker() {{ touch {shlex.quote(str(docker_call))}; }}
rm() {{ touch {shlex.quote(str(removal_call))}; }}
repair_build_ownership
prepare_stage
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode != 0
    assert "WORK_ROOT must be lexically normalized" in result.stderr
    assert not marker_call.exists()
    assert not docker_call.exists()
    assert not removal_call.exists()
    assert not work_root.exists()
    assert not dist_root.exists()


@pytest.mark.parametrize(
    "alias_kind",
    [
        "symlink_dot",
        "symlink_dot_slash",
        "symlinked_parent",
        "dot",
        "dotdot",
        "duplicate_separator",
        "descendant_dot",
        "descendant_dotdot",
        "descendant_symlinked_parent",
    ],
)
def test_managed_output_paths_reject_noncanonical_aliases_before_side_effects(
    tmp_path, alias_kind
):
    work_root = tmp_path / "work"
    dist_root = tmp_path / "dist"
    descendant_override = ""
    if alias_kind in {"symlink_dot", "symlink_dot_slash"}:
        target = tmp_path / "root-target"
        target.mkdir()
        alias = tmp_path / "root-alias"
        alias.symlink_to(target, target_is_directory=True)
        suffix = "/." if alias_kind == "symlink_dot" else "/./"
        work_root = f"{alias}{suffix}"
    elif alias_kind == "symlinked_parent":
        target = tmp_path / "parent-target"
        target.mkdir()
        alias = tmp_path / "parent-alias"
        alias.symlink_to(target, target_is_directory=True)
        work_root = alias / "work"
    elif alias_kind == "dot":
        work_root = f"{tmp_path}/./work"
    elif alias_kind == "dotdot":
        holder = tmp_path / "holder"
        holder.mkdir()
        work_root = f"{holder}/../work"
    elif alias_kind == "duplicate_separator":
        work_root = f"{tmp_path}//work"
    elif alias_kind == "descendant_dot":
        descendant_override = 'STAGE_DIR="$WORK_ROOT/./$STAGE_NAME"'
    elif alias_kind == "descendant_dotdot":
        descendant_override = 'SOURCE_TEST_DIR="$WORK_ROOT/scratch/../source-test"'
    else:
        work_root.mkdir()
        real_parent = work_root / "real-parent"
        real_parent.mkdir()
        (work_root / "parent-alias").symlink_to(
            real_parent, target_is_directory=True
        )
        descendant_override = (
            'STAGE_DIR="$WORK_ROOT/parent-alias/$STAGE_NAME"'
        )
    marker_call = tmp_path / "marker-called"
    docker_call = tmp_path / "docker-called"
    removal_call = tmp_path / "removal-called"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
{builder_root_assignments(work_root, dist_root)}
{descendant_override}
RESOLVED_UBI_IMAGE_ID=sha256:frozen-ubi
initialize_managed_root() {{ touch {shlex.quote(str(marker_call))}; }}
assert_managed_root_marker() {{ touch {shlex.quote(str(marker_call))}; }}
docker() {{ touch {shlex.quote(str(docker_call))}; }}
rm() {{ touch {shlex.quote(str(removal_call))}; }}
repair_build_ownership
prepare_stage
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode != 0
    assert not marker_call.exists()
    assert not docker_call.exists()
    assert not removal_call.exists()


def test_default_managed_output_paths_are_canonical():
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
validate_managed_output_paths
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr


def test_managed_output_roots_initialize_empty_roots_and_reuse_marker(tmp_path):
    work_root = tmp_path / "work"
    dist_root = tmp_path / "dist"
    work_root.mkdir()
    dist_root.mkdir()
    docker_log = tmp_path / "docker.log"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
{builder_root_assignments(work_root, dist_root)}
RESOLVED_UBI_IMAGE_ID=sha256:frozen-ubi
docker() {{ printf 'called\\n' >> {shlex.quote(str(docker_log))}; }}
repair_build_ownership
repair_build_ownership
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    marker_name = ".gc-analyzer-bundle-root"
    expected_magic = "GC_ANALYZER_BUNDLE_MANAGED_ROOT_V1"
    for root in (work_root, dist_root):
        marker = root / marker_name
        assert marker.is_file()
        assert not marker.is_symlink()
        assert marker.read_text() == expected_magic
    assert docker_log.read_text().splitlines() == ["called", "called"]


def test_managed_root_markers_are_outside_stage_and_archive_payload(tmp_path):
    work_root = tmp_path / "work"
    dist_root = tmp_path / "dist"
    docker_log = tmp_path / "docker.log"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
{builder_root_assignments(work_root, dist_root)}
RESOLVED_UBI_IMAGE_ID=sha256:frozen-ubi
docker() {{ printf 'called\\n' >> {shlex.quote(str(docker_log))}; }}
repair_build_ownership
prepare_stage
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    assert (work_root / ".gc-analyzer-bundle-root").is_file()
    assert (dist_root / ".gc-analyzer-bundle-root").is_file()
    assert not (work_root / "gc-analyzer-offline/.gc-analyzer-bundle-root").exists()


def run_inventory(stage):
    return subprocess.run(
        [str(INVENTORY_SCRIPT), str(stage)],
        capture_output=True,
        text=True,
    )


def verify_inventory(stage):
    return subprocess.run(
        [str(INVENTORY_SCRIPT), "--verify", str(stage)],
        capture_output=True,
        text=True,
    )


def inventory_contents(stage):
    return {
        name: (stage / name).read_text()
        for name in ("MANIFEST.paths", "MANIFEST.symlinks", "MANIFEST.sha256")
    }


def test_inventory_tracks_files_modes_safe_symlinks_and_added_entries(tmp_path):
    stage = tmp_path / "gc-analyzer-offline"
    binary = stage / "node-runtime/bin/node"
    target = stage / "node-runtime/lib/node_modules/next/dist/bin/next"
    link = stage / "node-runtime/lib/node_modules/.bin/next"
    binary.parent.mkdir(parents=True)
    target.parent.mkdir(parents=True)
    link.parent.mkdir(parents=True)
    binary.write_text("node-v1\n")
    binary.chmod(0o755)
    target.write_text("next-v1\n")
    link.symlink_to("../next/dist/bin/next")

    first = run_inventory(stage)
    assert first.returncode == 0, first.stderr
    baseline = inventory_contents(stage)
    assert "file\t0755\tnode-runtime/bin/node\n" in baseline["MANIFEST.paths"]
    assert "symlink\t0777\tnode-runtime/lib/node_modules/.bin/next\n" in baseline[
        "MANIFEST.paths"
    ]
    assert (
        "node-runtime/lib/node_modules/.bin/next\t../next/dist/bin/next\n"
        in baseline["MANIFEST.symlinks"]
    )
    assert "MANIFEST.paths" in baseline["MANIFEST.sha256"]
    assert "MANIFEST.symlinks" in baseline["MANIFEST.sha256"]

    binary.write_text("node-v2\n")
    assert run_inventory(stage).returncode == 0
    changed_file = inventory_contents(stage)
    assert changed_file["MANIFEST.sha256"] != baseline["MANIFEST.sha256"]

    second_target = target.with_name("next-alt")
    second_target.write_text("next-v1\n")
    link.unlink()
    link.symlink_to("../next/dist/bin/next-alt")
    assert run_inventory(stage).returncode == 0
    changed_link = inventory_contents(stage)
    assert changed_link["MANIFEST.symlinks"] != changed_file["MANIFEST.symlinks"]

    added = stage / "app/added.txt"
    added.parent.mkdir()
    added.write_text("added\n")
    assert run_inventory(stage).returncode == 0
    changed_entries = inventory_contents(stage)
    assert changed_entries["MANIFEST.paths"] != changed_link["MANIFEST.paths"]
    assert "app/added.txt" in changed_entries["MANIFEST.paths"]


@pytest.mark.parametrize("unsafe_kind", ["absolute", "escape", "broken", "tab"])
def test_inventory_rejects_unsafe_or_ambiguous_entries(tmp_path, unsafe_kind):
    stage = tmp_path / "gc-analyzer-offline"
    stage.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("outside\n")
    if unsafe_kind == "absolute":
        (stage / "link").symlink_to(outside)
    elif unsafe_kind == "escape":
        (stage / "link").symlink_to("../outside")
    elif unsafe_kind == "broken":
        (stage / "link").symlink_to("missing")
    else:
        (stage / "bad\tname").write_text("ambiguous\n")

    result = run_inventory(stage)

    assert result.returncode != 0


def test_inventory_rejects_unsupported_entry_type(tmp_path):
    stage = tmp_path / "gc-analyzer-offline"
    stage.mkdir()
    fifo = stage / "pipe"
    os.mkfifo(fifo)

    result = run_inventory(stage)

    assert result.returncode != 0
    assert "unsupported entry type" in result.stderr


def test_inventory_verify_accepts_untouched_stage_without_mutating_manifests(tmp_path):
    stage = tmp_path / "gc-analyzer-offline"
    payload = stage / "app/gcanalyzer/app.py"
    payload.parent.mkdir(parents=True)
    payload.write_text("print('healthy')\n")
    payload.chmod(0o640)
    assert run_inventory(stage).returncode == 0
    before = {
        name: ((stage / name).read_bytes(), (stage / name).stat().st_mtime_ns)
        for name in ("MANIFEST.paths", "MANIFEST.symlinks", "MANIFEST.sha256")
    }

    result = verify_inventory(stage)

    assert result.returncode == 0, result.stderr
    after = {
        name: ((stage / name).read_bytes(), (stage / name).stat().st_mtime_ns)
        for name in before
    }
    assert after == before


@pytest.mark.parametrize(
    "tamper",
    ["changed_file", "changed_symlink", "added_path", "missing_path", "mode_change"],
)
def test_inventory_verify_rejects_tampered_stage(tmp_path, tamper):
    stage = tmp_path / "gc-analyzer-offline"
    payload = stage / "app/payload.txt"
    target = stage / "node-runtime/lib/node.js"
    alternate = stage / "node-runtime/lib/node-alt.js"
    link = stage / "node-runtime/bin/node"
    payload.parent.mkdir(parents=True)
    target.parent.mkdir(parents=True)
    link.parent.mkdir(parents=True)
    payload.write_text("original\n")
    target.write_text("node\n")
    alternate.write_text("alternate\n")
    link.symlink_to("../lib/node.js")
    assert run_inventory(stage).returncode == 0

    if tamper == "changed_file":
        payload.write_text("changed\n")
    elif tamper == "changed_symlink":
        link.unlink()
        link.symlink_to("../lib/node-alt.js")
    elif tamper == "added_path":
        (stage / "app/added.txt").write_text("added\n")
    elif tamper == "missing_path":
        payload.unlink()
    else:
        payload.chmod(0o600)

    result = verify_inventory(stage)

    assert result.returncode != 0
    assert "verification failed" in result.stderr.lower()


def installer_source():
    assert OFFLINE_INSTALLER_SCRIPT.is_file(), "offline installer is missing"
    return OFFLINE_INSTALLER_SCRIPT.read_text()


def verifier_source():
    assert OFFLINE_VERIFIER_SCRIPT.is_file(), "offline verifier is missing"
    return OFFLINE_VERIFIER_SCRIPT.read_text()


def installer_function_body(function_name, next_function_name):
    source = installer_source()
    return source.split(f"{function_name}() {{", 1)[1].split(
        f"\n{next_function_name}() {{", 1
    )[0]


def rewrite_bundle_manifests(stage):
    symlinks = []
    for path in sorted(stage.rglob("*"), key=lambda item: os.fsencode(item.relative_to(stage))):
        if path.is_symlink():
            symlinks.append(
                f"{path.relative_to(stage).as_posix()}\t{os.readlink(path)}"
            )
    (stage / "MANIFEST.symlinks").write_text(
        "".join(f"{line}\n" for line in symlinks)
    )

    regular_paths = []
    for line in (stage / "MANIFEST.paths").read_text().splitlines():
        entry_type, _mode, relative = line.split("\t")
        if entry_type == "file":
            regular_paths.append(relative)
    regular_paths.extend(["MANIFEST.paths", "MANIFEST.symlinks"])
    regular_paths.sort(key=os.fsencode)
    (stage / "MANIFEST.sha256").write_text(
        "".join(
            f"{hashlib.sha256((stage / relative).read_bytes()).hexdigest()}  {relative}\n"
            for relative in regular_paths
        )
    )


def create_installer_bundle_fixture(tmp_path, symlink_case):
    stage = tmp_path / "gc-analyzer-offline"
    for directory in [
        "app",
        "rpms",
        "node-runtime/bin",
        "node-runtime/lib",
        "python-wheels",
        "npm-cache",
    ]:
        (stage / directory).mkdir(parents=True, exist_ok=True)
    for relative in [
        "app/payload.txt",
        "rpms/package.rpm",
        "python-wheels/package.whl",
        "npm-cache/cache-entry",
        "inventory.py",
        "verify-offline.sh",
        "node-runtime/lib/npm.js",
        "node-runtime/lib/npm-alt.js",
    ]:
        (stage / relative).write_text(f"fixture: {relative}\n")

    primary = stage / "node-runtime/bin/npm"
    primary.symlink_to("../lib/npm.js")
    chain = None
    if symlink_case in {"chain_escape", "loop"}:
        primary.unlink()
        primary.symlink_to("npm-chain")
        chain = stage / "node-runtime/bin/npm-chain"
        chain.symlink_to("../lib/npm.js")

    assert run_inventory(stage).returncode == 0
    outside = tmp_path / "outside-target"
    outside.write_text("outside\n")
    if symlink_case == "external_escape":
        primary.unlink()
        primary.symlink_to(os.path.relpath(outside, primary.parent))
    elif symlink_case == "absolute":
        primary.unlink()
        primary.symlink_to(outside)
    elif symlink_case == "broken":
        primary.unlink()
        primary.symlink_to("../lib/missing.js")
    elif symlink_case == "literal_mismatch":
        primary.unlink()
        primary.symlink_to("../lib/npm-alt.js")
    elif symlink_case == "chain_escape":
        chain.unlink()
        chain.symlink_to(os.path.relpath(outside, chain.parent))
    elif symlink_case == "loop":
        chain.unlink()
        chain.symlink_to("npm")
    if symlink_case not in {"safe_internal", "literal_mismatch"}:
        rewrite_bundle_manifests(stage)
    return stage


def run_shell_bundle_verification(tmp_path, stage):
    marker = tmp_path / "dnf-called"
    tools = tmp_path / "tools"
    tools.mkdir()
    for name, command in [
        ("readlink", shutil.which("greadlink") or shutil.which("readlink")),
        ("stat", shutil.which("gstat") or shutil.which("stat")),
    ]:
        assert command, f"required test command is missing: {name}"
        wrapper = tools / name
        wrapper.write_text(
            f'#!/usr/bin/env bash\nexec {shlex.quote(command)} "$@"\n'
        )
        wrapper.chmod(0o755)
    command = f"""
source {shlex.quote(str(OFFLINE_INSTALLER_SCRIPT))}
BUNDLE_ROOT={shlex.quote(str(stage))}
install_local_rpms() {{ touch {shlex.quote(str(marker))}; }}
verify_bundle_before_install
install_local_rpms
"""
    environment = os.environ.copy()
    environment["PATH"] = f"{tools}:{environment['PATH']}"
    result = subprocess.run(
        ["bash", "-c", command],
        capture_output=True,
        text=True,
        env=environment,
    )
    return result, marker


def test_offline_installer_has_strict_preflight_and_stage_aware_errors():
    source = installer_source()

    assert source.startswith("#!/usr/bin/env bash\n")
    assert "set -euo pipefail" in source
    assert "trap 'on_error" in source
    assert '[[ "$EUID" -eq 0 ]]' in source
    assert '. /etc/os-release' in source
    assert '[[ "$ID" == "rhel" ]]' in source
    assert '[[ "$VERSION_ID" == "8.10" ]]' in source
    assert '[[ "$(uname -m)" == "x86_64" ]]' in source
    assert "df -Pk" in source
    assert "MIN_FREE_KB" in source
    assert '[[ ${1:-} == "--container-test" ]]' in source
    assert "unexpected argument" in source


def test_offline_installer_verifies_bundle_before_any_rpm_mutation():
    source = installer_source()
    main = source.split("main() {", 1)[1]

    assert "sha256sum -c MANIFEST.sha256" in source
    assert "verify_shell_inventory" in source
    assert main.index("verify_bundle_before_install") < main.index("install_local_rpms")
    assert main.index("install_local_rpms") < main.index("verify_python_inventory")
    assert '"$BUNDLE_ROOT/inventory.py" --verify "$BUNDLE_ROOT"' in source


def test_offline_installer_all_package_managers_are_local_only():
    source = installer_source()

    assert "--disablerepo=*" in source
    assert "--disableplugin=*" in source
    assert 'rpms=("$BUNDLE_ROOT"/rpms/*.rpm)' in source
    assert 'dnf "${dnf_options[@]}" install "${rpms[@]}"' in source
    assert "pip install --no-index" in source
    assert '--find-links="$BUNDLE_ROOT/python-wheels"' in source
    assert "--only-binary=:all:" in source
    assert '-r "$APP_ROOT/requirements-offline.txt"' in source
    assert "pip check" in source
    assert "pip install --upgrade" not in source
    assert '"$NODE_ROOT/bin/node" "$npm_cli" ci --offline' in source
    assert '--cache "$BUNDLE_ROOT/npm-cache"' in source
    assert "--no-audit" in source


def test_offline_frontend_commands_run_from_installed_web_directory_only(tmp_path):
    source = installer_source()
    body = installer_function_body("install_frontend", "apply_permissions")

    assert 'cd "$APP_ROOT/web"' in body
    assert body.index('cd "$APP_ROOT/web"') < body.index('"$npm_cli" ci --offline')
    assert body.index('cd "$APP_ROOT/web"') < body.index('"$npm_cli" run build')
    assert 'cd "$BUNDLE_ROOT"' not in body
    assert '--prefix "$BUNDLE_ROOT' not in body

    app_root = tmp_path / "opt/gc-analyzer"
    web_root = app_root / "web"
    node_root = app_root / "runtime/node"
    bundle_root = tmp_path / "bundle"
    web_root.mkdir(parents=True)
    (node_root / "bin").mkdir(parents=True)
    (node_root / "lib/node_modules/npm/bin").mkdir(parents=True)
    (bundle_root / "npm-cache").mkdir(parents=True)
    node = node_root / "bin/node"
    node.write_text('#!/usr/bin/env bash\nprintf "v22.22.3\\n"\n')
    node.chmod(0o755)
    (node_root / "lib/node_modules/npm/bin/npm-cli.js").write_text("fixture\n")
    cwd_log = tmp_path / "npm-cwds"
    command = f"""
source {shlex.quote(str(OFFLINE_INSTALLER_SCRIPT))}
APP_ROOT={shlex.quote(str(app_root))}
NODE_ROOT={shlex.quote(str(node_root))}
BUNDLE_ROOT={shlex.quote(str(bundle_root))}
STATE_ROOT={shlex.quote(str(tmp_path / 'state'))}
SERVICE_USER=gc-analyzer
runuser() {{
    printf '%s\n' "$PWD" >> {shlex.quote(str(cwd_log))}
    case " $* " in
        *' run build '*) mkdir -p "$APP_ROOT/web/.next"; printf id > "$APP_ROOT/web/.next/BUILD_ID" ;;
    esac
}}
chown() {{ :; }}
install_frontend
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    assert cwd_log.read_text().splitlines() == [str(web_root), str(web_root)]
    assert str(bundle_root) not in cwd_log.read_text()


def test_offline_installer_preserves_state_and_uses_exact_runtime_paths():
    source = installer_source()

    for value in [
        'APP_ROOT="/opt/gc-analyzer"',
        'STATE_ROOT="/var/lib/gc-analyzer"',
        'CONFIG_ROOT="/etc/gc-analyzer"',
        'NODE_ROOT="$APP_ROOT/runtime/node"',
        'PYTHON_ROOT="$APP_ROOT/.venv"',
        'GC_USERS_FILE="$CONFIG_ROOT/users.json"',
        'GC_DB="$STATE_ROOT/gc_history.db"',
    ]:
        assert value in source
    assert "groupadd --system gc-analyzer" in source
    assert "useradd --system" in source
    assert "--shell /sbin/nologin" in source
    assert 'python3.12 -m venv "$PYTHON_ROOT"' in source
    assert '"$NODE_ROOT/bin/node"' in source
    assert "migrate_legacy_state" in source
    assert "ensure_persistent_link" in source
    assert "generate_session_secret" in source
    assert "initialize_users_file" in source
    assert "auth.load_users()" in source
    for database_pattern in ["*.db", "*.sqlite", "*.sqlite3", "*.db-wal", "*.db-shm"]:
        assert database_pattern in source
    assert "eval " not in source
    for forbidden in [
        'rm -rf "$APP_ROOT"',
        'rm -f "$STATE_ROOT',
        'rm -f "$CONFIG_ROOT',
        'rm -rf "$CONFIG_ROOT',
        'rm -rf "$STATE_ROOT',
    ]:
        assert forbidden not in source


def test_offline_config_permissions_allow_service_traversal_without_broad_writes():
    source = installer_source()
    state_body = installer_function_body(
        "prepare_persistent_state", "install_python_environment"
    )
    main = source.split("main() {", 1)[1]

    assert 'chown root:"$SERVICE_GROUP" "$CONFIG_ROOT"' in state_body
    assert 'chmod 0750 "$CONFIG_ROOT"' in state_body
    assert 'chown -R "$SERVICE_USER:$SERVICE_GROUP" "$CLUSTERS_ROOT"' in state_body
    assert 'chmod 0750 "$CLUSTERS_ROOT"' in state_body
    assert 'find "$CLUSTERS_ROOT" -type f -exec chmod 0640' in state_body
    assert 'chown "$SERVICE_USER:$SERVICE_GROUP" "$GC_USERS_FILE"' in source
    assert 'chmod 0600 "$GC_USERS_FILE"' in source
    assert 'chown root:"$SERVICE_GROUP" "$ENV_FILE" "$SESSION_SECRET_FILE"' in state_body
    assert 'chmod 0640 "$ENV_FILE" "$SESSION_SECRET_FILE"' in state_body
    assert '"$CONFIG_ROOT/config.json"' in state_body
    assert main.index("prepare_persistent_state") < main.index("verify_installation")


@pytest.mark.parametrize(
    ("symlink_case", "expected_success"),
    [
        ("safe_internal", True),
        ("external_escape", False),
        ("absolute", False),
        ("broken", False),
        ("chain_escape", False),
        ("loop", False),
        ("literal_mismatch", False),
    ],
)
def test_shell_bundle_verification_resolves_symlinks_before_rpm_install(
    tmp_path, symlink_case, expected_success
):
    stage = create_installer_bundle_fixture(tmp_path, symlink_case)

    result, dnf_marker = run_shell_bundle_verification(tmp_path, stage)

    if expected_success:
        assert result.returncode == 0, result.stderr
        assert dnf_marker.is_file()
    else:
        assert result.returncode != 0
        assert not dnf_marker.exists()
        assert "symlink" in result.stderr.lower()


def test_offline_installer_has_systemd_control_and_container_test_branches():
    source = installer_source()

    assert "gc-analyzer-backend.service" in source
    assert "gc-analyzer-frontend.service" in source
    for command in ["start", "stop", "restart", "status", "logs", "verify"]:
        assert f"{command})" in source
    assert "systemctl daemon-reload" in source
    assert "systemctl enable" in source
    assert "systemctl start" in source
    assert 'if [[ "$CONTAINER_TEST" == "1" ]]' in source
    assert '"$BUNDLE_ROOT/verify-offline.sh"' in source
    assert "systemctl is-active --quiet" in source
    assert "trap restore_after_verify EXIT" in source


def test_offline_verifier_checks_exact_runtimes_and_built_dependencies():
    source = verifier_source()

    assert "set -euo pipefail" in source
    assert "sys.version_info[:2] == (3, 12)" in source
    for module in ["fastapi", "uvicorn", "paramiko", "yaml", "sklearn", "sqlite3"]:
        assert module in source
    assert 'EXPECTED_NODE_VERSION="v22.22.3"' in source
    assert 'EXPECTED_NPM_MAJOR="10"' in source
    assert '"$NODE_BIN" --version' in source
    assert '"$NODE_BIN" "$NPM_CLI" ls --offline' in source
    assert '.next/BUILD_ID' in source


def test_offline_verifier_starts_both_services_with_isolated_state_and_checks_http():
    source = verifier_source()

    assert 'GC_SCHED_ENABLED="0"' in source
    assert "GC_DB=" in source
    assert "GC_USERS_FILE=" in source
    assert "GC_SESSION_SECRET=" in source
    assert "BACKEND_URL=" in source
    assert "/api/health" in source
    assert 'FRONTEND_URL="http://${FRONTEND_HOST}:${FRONTEND_PORT}/"' in source
    assert 'FRONTEND_API_URL="http://${FRONTEND_HOST}:${FRONTEND_PORT}/api/health"' in source
    assert 'wait_for_http_200 "$FRONTEND_API_URL"' in source
    assert "wait_for_http_200" in source
    assert "assert_port_available" in source
    assert "kill -TERM" in source
    assert "kill -KILL" in source
    assert "trap cleanup EXIT" in source
    assert "tail -n" in source
    assert "runuser -u gc-analyzer" in source


@pytest.mark.parametrize(
    "script",
    [OFFLINE_INSTALLER_SCRIPT, OFFLINE_VERIFIER_SCRIPT, COPY_APPLICATION_SCRIPT, BUNDLE_BUILDER_SCRIPT],
)
def test_all_offline_shell_scripts_have_valid_bash_syntax(script):
    assert script.is_file(), f"offline shell script is missing: {script.name}"
    result = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_builder_writes_complete_inventory_before_clean_room():
    source = builder_source()
    body = builder_function_body("write_checksums", "run_clean_room")

    assert 'generate-inventory.py" "$STAGE_DIR"' in body
    assert "MANIFEST.paths" in source
    assert "MANIFEST.symlinks" in source
    main = source.split("main() {", 1)[1].split('\n}\n\nif [[', 1)[0]
    assert main.index("write_checksums") < main.index("run_clean_room")


def test_archive_contains_gc_analyzer_offline_as_top_level(tmp_path):
    source = builder_source()
    create_body = builder_function_body("create_archive", "main")

    assert 'STAGE_NAME="gc-analyzer-offline"' in source
    assert 'STAGE_DIR=${GC_ANALYZER_BUNDLE_STAGE_DIR:-"$WORK_ROOT/$STAGE_NAME"}' in source
    assert '-C /work -czf "/dist/$ARCHIVE_NAME" "$STAGE_NAME"' in create_body

    work = tmp_path / "work"
    stage = work / "gc-analyzer-offline"
    (stage / "app").mkdir(parents=True)
    (stage / "app/file.txt").write_text("payload\n")
    archive = tmp_path / "bundle.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(stage, arcname="gc-analyzer-offline")
    with tarfile.open(archive, "r:gz") as tar:
        names = tar.getnames()

    assert names[0] == "gc-analyzer-offline"
    assert "gc-analyzer-offline/app/file.txt" in names


def test_versions_records_resolvers_and_sorted_artifact_hashes():
    source = builder_source()
    body = builder_function_body("write_version_manifest", "write_checksums")

    for field in [
        "resolver_ubi_tag",
        "resolver_ubi_id",
        "resolver_ubi_digest",
        "resolver_node_test_tag",
        "resolver_node_test_id",
        "resolver_node_test_digest",
        "rpm_artifact",
        "python_wheel",
    ]:
        assert field in body
    assert "sha256" in body
    assert "sorted" in body


def test_version_manifest_behavior_records_sorted_hashed_artifacts(tmp_path):
    stage = tmp_path / "gc-analyzer-offline"
    rpms = stage / "rpms"
    wheels = stage / "python-wheels"
    rpms.mkdir(parents=True)
    wheels.mkdir()
    artifacts = {
        rpms / "z-package.rpm": b"rpm-z",
        rpms / "a-package.rpm": b"rpm-a",
        wheels / "z_package.whl": b"wheel-z",
        wheels / "a_package.whl": b"wheel-a",
    }
    for artifact, content in artifacts.items():
        artifact.write_bytes(content)
    source_commit = run_git(ROOT, "rev-parse", "HEAD").stdout.strip()
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
SOURCE_SNAPSHOT_DIR={shlex.quote(str(ROOT))}
SOURCE_COMMIT={shlex.quote(source_commit)}
STAGE_DIR={shlex.quote(str(stage))}
RESOLVED_UBI_IMAGE_ID=sha256:ubi-id
RESOLVED_UBI_IMAGE_DIGEST=registry/ubi@sha256:ubi-digest
RESOLVED_NODE_TEST_IMAGE_ID=sha256:node-id
RESOLVED_NODE_TEST_IMAGE_DIGEST=node@sha256:node-digest
RESOLVED_NPM_VERSION=10.9.4
assert_source_snapshot() {{ :; }}
write_version_manifest
"""

    result = subprocess.run(["bash", "-c", command], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    lines = (stage / "VERSIONS.txt").read_text().splitlines()
    rpm_lines = [line for line in lines if line.startswith("rpm_artifact=")]
    wheel_lines = [line for line in lines if line.startswith("python_wheel=")]
    assert rpm_lines == sorted(rpm_lines)
    assert wheel_lines == sorted(wheel_lines)
    for artifact, content in artifacts.items():
        label = "rpm_artifact" if artifact.suffix == ".rpm" else "python_wheel"
        expected = (
            f"{label}={artifact.name} "
            f"sha256={hashlib.sha256(content).hexdigest()}"
        )
        assert expected in lines
    assert f"source_commit={source_commit}" in lines
    assert "resolver_ubi_id=sha256:ubi-id" in lines
    assert "resolver_node_test_id=sha256:node-id" in lines
