import subprocess
import stat
import shlex
from pathlib import Path, PurePosixPath

import pytest


ROOT = Path(__file__).resolve().parents[1]
COPY_APPLICATION_SCRIPT = ROOT / "offline/copy-application.sh"
BUNDLE_BUILDER_SCRIPT = ROOT / "offline/build-bundle.sh"
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


def test_bundle_builder_has_required_shell_contract_and_functions():
    source = builder_source()

    assert source.startswith("#!/usr/bin/env bash\n")
    assert "set -euo pipefail" in source
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
    assert 'UBI_IMAGE="registry.access.redhat.com/ubi8/ubi:8.10"' in source
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

    assert 'NODE_TEST_IMAGE="node:22.22.3-bookworm-slim"' in source
    assert 'git -C "$SOURCE_ROOT" archive --format=tar HEAD -- web' in body
    assert '"$SOURCE_TEST_DIR/web:/workspace"' in body
    assert 'docker run --rm --platform "$CONTAINER_PLATFORM"' in body
    assert '"$NODE_TEST_IMAGE"' in body
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

    assert 'compileall -q gcanalyzer seed tests' in body
    assert "require_command npm" not in builder_function_body("preflight", "test_source")


def create_source_gate_fixture(tmp_path):
    source = tmp_path / "source"
    (source / "web").mkdir(parents=True)
    (source / "gcanalyzer").mkdir()
    (source / "seed").mkdir()
    (source / "tests").mkdir()
    (source / ".gitignore").write_text(".venv/\nweb/node_modules/\n")
    (source / "web" / "package.json").write_text('{"scripts": {}}\n')
    (source / "web" / "package-lock.json").write_text(
        '{"name": "fixture", "lockfileVersion": 3, "packages": {}}\n'
    )
    head_marker = source / "web" / "from-head.txt"
    head_marker.write_text("committed\n")
    commit_fixture_repository(source)
    head_marker.write_text("working-tree\n")

    python_log = tmp_path / "python.log"
    python_stub = source / ".venv" / "bin" / "python"
    python_stub.parent.mkdir(parents=True)
    python_stub.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$*\" >> \"$PYTHON_LOG\"\n"
    )
    python_stub.chmod(0o755)

    repository_cache = source / "web" / "node_modules"
    repository_cache.mkdir()
    repository_sentinel = repository_cache / "must-survive"
    repository_sentinel.write_text("untouched\n")
    return source, python_log, repository_sentinel


def test_source_gate_behavior_uses_head_copy_without_touching_repository(tmp_path):
    source, python_log, repository_sentinel = create_source_gate_fixture(tmp_path)
    work_root = tmp_path / "work"
    source_test_dir = work_root / "source-test"
    docker_log = tmp_path / "docker.log"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
SOURCE_ROOT={shlex.quote(str(source))}
WORK_ROOT={shlex.quote(str(work_root))}
SOURCE_TEST_DIR={shlex.quote(str(source_test_dir))}
export PYTHON_LOG={shlex.quote(str(python_log))}
docker() {{
    printf '%s\\n' "$@" > {shlex.quote(str(docker_log))}
    printf 'exported=%s\\n' "$(cat "$SOURCE_TEST_DIR/web/from-head.txt")" \
        >> {shlex.quote(str(docker_log))}
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
    assert "node:22.22.3-bookworm-slim" in docker_arguments
    assert f"{source_test_dir}/web:/workspace" in docker_arguments
    assert f"{source}/web:/workspace" not in docker_arguments
    assert "exported=committed" in docker_arguments
    python_commands = python_log.read_text()
    assert "-m pytest -q" in python_commands
    assert "-m compileall -q gcanalyzer seed tests" in python_commands


def test_source_gate_cleans_disposable_copy_when_container_fails(tmp_path):
    source, _, repository_sentinel = create_source_gate_fixture(tmp_path)
    work_root = tmp_path / "work"
    source_test_dir = work_root / "source-test"
    docker_log = tmp_path / "docker-failed"
    command = f"""
source {shlex.quote(str(BUNDLE_BUILDER_SCRIPT))}
SOURCE_ROOT={shlex.quote(str(source))}
WORK_ROOT={shlex.quote(str(work_root))}
SOURCE_TEST_DIR={shlex.quote(str(source_test_dir))}
export PYTHON_LOG={shlex.quote(str(tmp_path / 'python-failed.log'))}
docker() {{ touch {shlex.quote(str(docker_log))}; return 37; }}
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

    assert '"$COPY_APPLICATION_SCRIPT" "$SOURCE_ROOT" "$STAGE_DIR/app"' in source
    assert '"$INSTALLER_SCRIPT"' in source
    assert '"$VERIFIER_SCRIPT"' in source
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
        "registry.access.redhat.com/ubi8/ubi:8.10",
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
