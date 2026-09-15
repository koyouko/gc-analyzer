"""Behavioral tests for the release gate, without a Docker daemon or RPM installs."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import textwrap

import pytest


ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "offline/test-clean-room.sh"
IMAGE_ID = "sha256:" + "a" * 64
OTHER_IMAGE_ID = "sha256:" + "b" * 64


def executable(path, source):
    path.write_text(textwrap.dedent(source).lstrip())
    path.chmod(0o755)


@pytest.fixture
def room(tmp_path):
    tmp_path = tmp_path.resolve()
    binaries = tmp_path / "bin"
    binaries.mkdir()
    stage = tmp_path / "staged bundle"
    stage.mkdir()
    container = tmp_path / "container"
    container.mkdir()
    (container / "os-release").write_text('ID="rhel"\nVERSION_ID="8.10"\n')
    (container / "proc/1").mkdir(parents=True)
    (container / "proc/1/cmdline").write_bytes(b"/sbin/docker-init\0")
    (container / "proc/net").mkdir()
    for protocol in ("tcp", "tcp6"):
        (container / "proc/net" / protocol).write_text(
            "  sl  local_address rem_address   st\n"
        )
    executable(
        stage / "install-offline.sh",
        r"""
        #!/bin/bash
        set -euo pipefail
        [[ "$#" == 1 && "$1" == --container-test ]]
        [[ "$(pwd -P)" != "$FAKE_STAGE" ]]
        [[ "${BASH_SOURCE[0]}" == "$FAKE_ROOT/bundle/install-offline.sh" ]]
        count=0
        [[ ! -f "$FAKE_ROOT/count" ]] || read -r count < "$FAKE_ROOT/count"
        count=$((count + 1))
        printf '%s\n' "$count" > "$FAKE_ROOT/count"
        printf 'install:%s\n' "$count" >> "$FAKE_ROOT/events"
        if [[ "$count" == "${FAKE_INSTALL_FAIL_AT:-0}" ]]; then
            exit 42
        fi
        if [[ "$count" == 1 ]]; then
            printf 'preserve me\n' > "$FAKE_ROOT/persistent-state"
        else
            [[ "$(< "$FAKE_ROOT/persistent-state")" == 'preserve me' ]]
        fi
        "$FAKE_ROOT/bundle/verify-offline.sh"
        if [[ "$count" == "${FAKE_LEAK_AT:-0}" ]]; then
            mkdir -p "$FAKE_ROOT/proc/123"
            printf '%s\0' "$FAKE_LEAK_COMMAND" > "$FAKE_ROOT/proc/123/cmdline"
        fi
        """,
    )
    executable(
        stage / "verify-offline.sh",
        r"""
        #!/bin/bash
        set -euo pipefail
        read -r count < "$FAKE_ROOT/count"
        printf 'verify:%s\n' "$count" >> "$FAKE_ROOT/events"
        if [[ "$count" == "${FAKE_VERIFY_FAIL_AT:-0}" ]]; then
            exit 43
        fi
        """,
    )
    executable(
        binaries / "uname",
        """
        #!/bin/bash
        printf '%s\\n' "${FAKE_ARCH:-x86_64}"
        """,
    )
    executable(
        binaries / "docker",
        f"#!{Path(sys.executable).resolve()}\n"
        + textwrap.dedent(
            r"""
            import json
            import os
            from pathlib import Path
            import subprocess
            import sys

            args = sys.argv[1:]
            body = sys.stdin.read() if args and args[0] == "run" else ""
            with open(os.environ["FAKE_DOCKER_LOG"], "a") as log:
                log.write(json.dumps({"args": args, "body": body}) + "\n")
            if args[:2] == ["image", "inspect"]:
                if os.environ.get("FAKE_INSPECT_STATUS"):
                    sys.exit(int(os.environ["FAKE_INSPECT_STATUS"]))
                print(os.environ["FAKE_INSPECT_RESULT"])
                sys.exit(0)
            if not args or args[0] != "run":
                sys.exit("unexpected Docker operation")
            if os.environ.get("FAKE_RUN_STATUS"):
                sys.exit(int(os.environ["FAKE_RUN_STATUS"]))
            if os.environ.get("FAKE_EXECUTE") != "1":
                sys.exit(0)
            image_index = args.index(os.environ["FAKE_IMAGE_ID"])
            command = args[image_index + 1:]
            # Only filesystem roots are substituted; the actual container shell
            # program is executed, so its conditionals and exit statuses are real.
            root = os.environ["FAKE_ROOT"]
            for source, target in (
                ("$EUID", "${FAKE_EUID:-0}"),
                ("/etc/os-release", root + "/os-release"),
                ("/incoming", os.environ["FAKE_STAGE"]),
                ("/bundle", root + "/bundle"),
                ("/proc", root + "/proc"),
            ):
                body = body.replace(source, target)
            result = subprocess.run(
                [os.environ["FAKE_BASH"], *command], input=body, text=True,
                cwd=root, env=os.environ.copy(),
            )
            sys.exit(result.returncode)
            """
        ),
    )
    bash = shutil.which("bash")
    assert bash, "Bash is required to test the clean-room harness"
    version = subprocess.check_output(
        [bash, "-c", "printf '%s' \"${BASH_VERSINFO[0]}\""], text=True
    )
    if int(version) < 4:
        pytest.skip("behavioral harness tests require Bash 4 or newer")
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("GC_", "RESOLVED_", "FAKE_"))
        and key not in ("BASH_ENV", "ENV", "CDPATH")
    }
    environment.update(
        PATH=f"{binaries}:{os.environ['PATH']}",
        FAKE_DOCKER_LOG=str(tmp_path / "docker.jsonl"),
        FAKE_IMAGE_ID=IMAGE_ID,
        FAKE_INSPECT_RESULT=f"{IMAGE_ID} linux/amd64",
        FAKE_ROOT=str(container),
        FAKE_STAGE=str(stage),
        FAKE_BASH=bash,
    )
    return {
        "root": tmp_path,
        "stage": stage,
        "container": container,
        "env": environment,
        "bash": bash,
    }


def run_harness(room, *arguments, **environment):
    return subprocess.run(
        [room["bash"], str(HARNESS), *map(str, arguments)],
        cwd=room["root"],
        env={**room["env"], **environment},
        capture_output=True,
        text=True,
        timeout=15,
    )


def calls(room):
    path = Path(room["env"]["FAKE_DOCKER_LOG"])
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def events(room):
    path = room["container"] / "events"
    return path.read_text().splitlines() if path.exists() else []


def test_runs_only_frozen_image_with_offline_read_only_input(room):
    result = run_harness(room, room["stage"], IMAGE_ID)
    assert result.returncode == 0, result.stderr
    inspect, run = calls(room)
    assert inspect["args"] == [
        "image", "inspect", "--format", "{{.Id}} {{.Os}}/{{.Architecture}}", IMAGE_ID,
    ]
    args = run["args"]
    assert args[0] == "run"
    assert args[args.index("--platform") + 1] == "linux/amd64"
    assert args[args.index("--network") + 1] == "none"
    assert args[args.index("--pull") + 1] == "never"
    assert "--rm" in args
    assert args[args.index("--user") + 1] == "0:0"
    assert args[args.index("--entrypoint") + 1] == "/bin/bash"
    assert args.count("--mount") == 1
    assert args[args.index("--mount") + 1] == (
        f"type=bind,source={room['stage']},target=/incoming,readonly"
    )
    assert not {"-v", "--volume", "--privileged", "-p", "--publish", "--env-file"}.intersection(args)
    assert args[args.index(IMAGE_ID) + 1:] == ["-euo", "pipefail", "-s"]
    assert str(room["stage"]) not in run["body"]


def test_accepts_frozen_image_from_builder_environment(room):
    result = run_harness(room, room["stage"], GC_ANALYZER_CLEAN_ROOM_IMAGE=IMAGE_ID)
    assert result.returncode == 0, result.stderr
    assert IMAGE_ID in calls(room)[-1]["args"]


@pytest.mark.parametrize("arguments", [[], ["{stage}"], ["{stage}", IMAGE_ID, "extra"]])
def test_rejects_missing_or_extra_arguments_before_docker(room, arguments):
    arguments = [str(room["stage"]) if value == "{stage}" else value for value in arguments]
    result = run_harness(room, *arguments)
    assert result.returncode != 0
    assert "ERROR" in result.stderr
    assert not calls(room)


@pytest.mark.parametrize("image", ["", "ubi:8.10", "sha256:abc", "a" * 64,
                                  "sha256:" + "g" * 64, IMAGE_ID + "\n", "$(touch injected)"])
def test_rejects_unfrozen_or_malformed_images_before_docker(room, image):
    result = run_harness(room, room["stage"], image)
    assert result.returncode != 0
    assert "image" in result.stderr.lower()
    assert not calls(room)
    assert not (room["root"] / "injected").exists()


@pytest.mark.parametrize("kind", ["relative", "missing", "file", "root", "symlink",
                                 "symlink-parent", "dot", "parent", "trailing-slash",
                                 "comma", "quote", "newline", "tab"])
def test_rejects_unsafe_stage_paths_before_docker(room, kind):
    stage = room["stage"]
    if kind == "relative":
        stage = stage.name
    elif kind == "missing":
        stage = room["root"] / "absent"
    elif kind == "file":
        stage = stage / "install-offline.sh"
    elif kind == "root":
        stage = "/"
    elif kind == "symlink":
        link = room["root"] / "stage-link"
        link.symlink_to(stage, target_is_directory=True)
        stage = link
    elif kind == "symlink-parent":
        link = room["root"] / "parent-link"
        link.symlink_to(room["root"], target_is_directory=True)
        stage = link / stage.name
    elif kind == "dot":
        stage = f"{stage}/."
    elif kind == "parent":
        stage = f"{stage}/../{stage.name}"
    elif kind == "trailing-slash":
        stage = f"{stage}/"
    else:
        suffix = {"comma": ",readonly=false", "quote": '"', "newline": "\n", "tab": "\t"}[kind]
        destination = room["root"] / ("stage" + suffix)
        stage.rename(destination)
        stage = destination
    result = run_harness(room, stage, IMAGE_ID)
    assert result.returncode != 0
    assert "stage" in result.stderr.lower()
    assert not calls(room)


@pytest.mark.parametrize("name", ["install-offline.sh", "verify-offline.sh"])
@pytest.mark.parametrize("kind", ["missing", "symlink", "not-executable", "directory"])
def test_requires_real_executable_entrypoints_before_docker(room, name, kind):
    path = room["stage"] / name
    if kind == "not-executable":
        path.chmod(0o644)
    else:
        path.unlink()
        if kind == "symlink":
            target = room["root"] / "external-script"
            executable(target, "#!/bin/bash\nexit 0\n")
            path.symlink_to(target)
        elif kind == "directory":
            path.mkdir()
    result = run_harness(room, room["stage"], IMAGE_ID)
    assert result.returncode != 0
    assert name in result.stderr
    assert not calls(room)


def test_shell_metacharacters_in_path_remain_literal(room):
    destination = room["root"] / "stage ' ; $(touch injected) & space"
    room["stage"].rename(destination)
    result = run_harness(room, destination, IMAGE_ID)
    assert result.returncode == 0, result.stderr
    assert f"type=bind,source={destination},target=/incoming,readonly" in calls(room)[-1]["args"]
    assert not (room["root"] / "injected").exists()


@pytest.mark.parametrize("metadata", ["", f"{IMAGE_ID} linux/arm64", f"{IMAGE_ID} windows/amd64",
                                     f"{OTHER_IMAGE_ID} linux/amd64"])
def test_rejects_missing_wrong_platform_or_mismatched_local_image(room, metadata):
    result = run_harness(room, room["stage"], IMAGE_ID, FAKE_INSPECT_RESULT=metadata)
    assert result.returncode != 0
    assert len(calls(room)) == 1


def test_image_inspection_failure_never_starts_or_pulls_container(room):
    result = run_harness(room, room["stage"], IMAGE_ID, FAKE_INSPECT_STATUS="17")
    assert result.returncode != 0
    assert len(calls(room)) == 1


@pytest.mark.parametrize("status", [1, 42, 125, 137])
def test_propagates_docker_or_container_failure(room, status):
    result = run_harness(room, room["stage"], IMAGE_ID, FAKE_RUN_STATUS=str(status))
    assert result.returncode == status


def test_installs_and_verifies_twice_in_same_container_without_host_writes(room):
    original = {path.name: path.read_bytes() for path in room["stage"].iterdir()}
    result = run_harness(room, room["stage"], IMAGE_ID, FAKE_EXECUTE="1")
    assert result.returncode == 0, result.stderr
    assert events(room) == ["install:1", "verify:1", "install:2", "verify:2"]
    assert len(calls(room)) == 2
    assert (room["container"] / "bundle/install-offline.sh").is_file()
    assert {path.name: path.read_bytes() for path in room["stage"].iterdir()} == original


@pytest.mark.parametrize("release", ['ID="rocky"\nVERSION_ID="8.10"\n',
                                   'ID="rhel"\nVERSION_ID="8.9"\n', '',
                                   'ID="rhel"\nVERSION_ID="8.10"\ntouch injected\n'])
def test_requires_actual_rhel_810_before_install(room, release):
    (room["container"] / "os-release").write_text(release)
    result = run_harness(room, room["stage"], IMAGE_ID, FAKE_EXECUTE="1")
    if 'touch injected' in release:
        assert not (room["container"] / "injected").exists()
    else:
        assert result.returncode != 0
        assert not events(room)


def test_requires_actual_x86_64_before_install(room):
    result = run_harness(room, room["stage"], IMAGE_ID, FAKE_EXECUTE="1", FAKE_ARCH="aarch64")
    assert result.returncode != 0
    assert not events(room)


@pytest.mark.parametrize("failure", ["INSTALL", "VERIFY"])
@pytest.mark.parametrize("attempt", [1, 2])
def test_actual_install_or_verifier_failure_stops_gate(room, failure, attempt):
    result = run_harness(
        room, room["stage"], IMAGE_ID, FAKE_EXECUTE="1",
        **{f"FAKE_{failure}_FAIL_AT": str(attempt)},
    )
    assert result.returncode == (42 if failure == "INSTALL" else 43), result.stderr
    expected = ["install:1", "verify:1", "install:2", "verify:2"]
    length = (attempt - 1) * 2 + (1 if failure == "INSTALL" else 2)
    assert events(room) == expected[:length]


@pytest.mark.parametrize("command", ["gcanalyzer.app", "/opt/gc-analyzer/.venv/bin/python",
                                    "next-server (v15.5.12)"])
@pytest.mark.parametrize("attempt", [1, 2])
def test_leftover_application_process_fails_gate_after_each_install(room, command, attempt):
    result = run_harness(
        room, room["stage"], IMAGE_ID, FAKE_EXECUTE="1",
        FAKE_LEAK_AT=str(attempt), FAKE_LEAK_COMMAND=command,
    )
    assert result.returncode != 0
    assert "process" in result.stderr.lower()
    assert len(events(room)) == attempt * 2


def test_copy_failure_prevents_install(room):
    executable(room["root"] / "bin/cp", "#!/bin/bash\nexit 31\n")
    result = run_harness(room, room["stage"], IMAGE_ID, FAKE_EXECUTE="1")
    assert result.returncode == 31
    assert not events(room)
