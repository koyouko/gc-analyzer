import subprocess
from pathlib import Path, PurePosixPath

import pytest


ROOT = Path(__file__).resolve().parents[1]
COPY_APPLICATION_SCRIPT = ROOT / "offline/copy-application.sh"
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
    run_git(source, "init", "--quiet")
    run_git(source, "add", "offline/app-files.txt", "payload")
    run_git(
        source,
        "-c",
        "user.name=GC Analyzer Tests",
        "-c",
        "user.email=tests@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "fixture",
    )

    result = subprocess.run(
        [str(COPY_APPLICATION_SCRIPT), str(source), str(destination)],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "forbidden tracked path" in result.stderr
    assert not destination.exists()
