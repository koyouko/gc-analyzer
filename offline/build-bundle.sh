#!/usr/bin/env bash
set -euo pipefail

TARGET_RHEL_VERSION="8.10"
TARGET_ARCH="x86_64"
CONTAINER_PLATFORM="linux/amd64"
UBI_IMAGE="registry.access.redhat.com/ubi8/ubi:8.10"
NODE_VERSION="22.22.3"
NPM_MAJOR_VERSION="10"
NODE_TEST_IMAGE="node:22.22.3-bookworm-slim"
NODE_ARCHIVE="node-v${NODE_VERSION}-linux-x64.tar.xz"
ARCHIVE_NAME="gc-analyzer-rhel8.10-x86_64-offline.tar.gz"

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SOURCE_ROOT=$(git -C "$SCRIPT_DIR/.." rev-parse --show-toplevel)
WORK_ROOT=${GC_ANALYZER_BUNDLE_WORK_ROOT:-"$SCRIPT_DIR/work"}
DIST_ROOT=${GC_ANALYZER_BUNDLE_DIST_ROOT:-"$SCRIPT_DIR/dist"}
STAGE_DIR=${GC_ANALYZER_BUNDLE_STAGE_DIR:-"$WORK_ROOT/$ARCHIVE_NAME.stage"}
SOURCE_TEST_DIR=${GC_ANALYZER_SOURCE_TEST_DIR:-"$WORK_ROOT/source-test"}
NPM_WORK_DIR="$WORK_ROOT/npm-work"
NODE_DOWNLOAD_DIR="$WORK_ROOT/node-download"
RPM_ROOTS_FILE="$SOURCE_ROOT/offline/rhel8-packages.txt"
COPY_APPLICATION_SCRIPT=${GC_ANALYZER_COPY_APPLICATION_SCRIPT:-"$SOURCE_ROOT/offline/copy-application.sh"}
INSTALLER_SCRIPT=${GC_ANALYZER_INSTALLER_SCRIPT:-"$SOURCE_ROOT/offline/install-offline.sh"}
VERIFIER_SCRIPT=${GC_ANALYZER_VERIFIER_SCRIPT:-"$SOURCE_ROOT/offline/verify-offline.sh"}
CLEAN_ROOM_SCRIPT=${GC_ANALYZER_CLEAN_ROOM_SCRIPT:-"$SOURCE_ROOT/offline/test-clean-room.sh"}
ARCHIVE_PATH="$DIST_ROOT/$ARCHIVE_NAME"
RESOLVED_NPM_VERSION=""

fail() {
    printf 'ERROR: %s\n' "$1" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "required command is unavailable: $1"
}

require_nonempty_directory() {
    local directory=$1
    local label=$2
    [[ -d "$directory" ]] || fail "$label directory is missing: $directory"
    [[ -n "$(find "$directory" -mindepth 1 -print -quit)" ]] \
        || fail "$label directory is empty: $directory"
}

require_descendant() {
    local path=${1%/}
    local parent=${2%/}
    local label=$3
    local normalized_path
    local normalized_parent
    [[ -n "$path" && -n "$parent" && "$path" != "/" && "$parent" != "/" ]] \
        || fail "unsafe $label path"
    normalized_path=$(python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$path")
    normalized_parent=$(python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$parent")
    case "$normalized_path/" in
        "$normalized_parent"/*)
            ;;
        *)
            fail "$label must be beneath $parent: $path"
            ;;
    esac
}

run_ubi() {
    local docker_options=()
    while (($# > 0)) && [[ "$1" != "--" ]]; do
        (($# >= 2)) || fail "Docker option is missing a value: $1"
        docker_options+=("$1" "$2")
        shift 2
    done
    [[ ${1:-} == "--" ]] || fail "run_ubi requires -- before the container command"
    shift
    (($# > 0)) || fail "run_ubi requires a container command"

    docker run --rm --platform "$CONTAINER_PLATFORM" \
        "${docker_options[@]}" "$UBI_IMAGE" "$@"
}

preflight() {
    ((BASH_VERSINFO[0] >= 4)) || fail "Bash 4 or newer is required"
    require_command curl
    require_command docker
    require_command git
    require_command python3
    require_command tar

    [[ -s "$RPM_ROOTS_FILE" ]] || fail "RPM root manifest is missing or empty: $RPM_ROOTS_FILE"
    [[ -s "$SOURCE_ROOT/requirements-offline.txt" ]] \
        || fail "offline Python requirements are missing"
    [[ -s "$SOURCE_ROOT/web/package-lock.json" ]] \
        || fail "committed frontend package lock is missing"
    [[ -x "$COPY_APPLICATION_SCRIPT" ]] \
        || fail "application copy helper is missing or not executable: $COPY_APPLICATION_SCRIPT"
    [[ -x "$INSTALLER_SCRIPT" ]] \
        || fail "offline installer is missing or not executable: $INSTALLER_SCRIPT"
    [[ -x "$VERIFIER_SCRIPT" ]] \
        || fail "offline verifier is missing or not executable: $VERIFIER_SCRIPT"
    [[ -x "$CLEAN_ROOM_SCRIPT" ]] \
        || fail "clean-room test is missing or not executable: $CLEAN_ROOM_SCRIPT"

    git -C "$SOURCE_ROOT" diff --quiet HEAD -- \
        || fail "Git worktree has modified tracked files"
    git -C "$SOURCE_ROOT" diff --cached --quiet HEAD -- \
        || fail "Git index has staged changes"
    [[ -z "$(git -C "$SOURCE_ROOT" ls-files --others --exclude-standard)" ]] \
        || fail "Git worktree has untracked files"

    docker info >/dev/null
}

test_source() {
    local python_bin=python3
    if [[ -x "$SOURCE_ROOT/.venv/bin/python" ]]; then
        python_bin="$SOURCE_ROOT/.venv/bin/python"
    fi

    require_descendant "$SOURCE_TEST_DIR" "$WORK_ROOT" "source test"
    mkdir -p "$WORK_ROOT"
    rm -rf "$SOURCE_TEST_DIR"
    mkdir -p "$SOURCE_TEST_DIR"

    (
        cleanup_source_test() {
            local status=$?
            trap - EXIT
            rm -rf -- "$SOURCE_TEST_DIR"
            exit "$status"
        }
        trap cleanup_source_test EXIT

        cd "$SOURCE_ROOT"
        "$python_bin" -m pytest -q
        "$python_bin" -m compileall -q gcanalyzer seed tests

        git -C "$SOURCE_ROOT" archive --format=tar HEAD -- web \
            | tar -xf - -C "$SOURCE_TEST_DIR"

        docker run --rm --platform "$CONTAINER_PLATFORM" \
            -v "$SOURCE_TEST_DIR/web:/workspace" \
            -w /workspace \
            "$NODE_TEST_IMAGE" \
            sh -eu -c '
                test "$(node --version)" = "v$1"
                case "$(npm --version)" in "$2".*) ;; *) exit 1 ;; esac
                npm ci
                npm run typecheck
                npm run audit:prod
                npm run build
            ' _ "$NODE_VERSION" "$NPM_MAJOR_VERSION"
    )
}

prepare_stage() {
    require_descendant "$STAGE_DIR" "$WORK_ROOT" "stage"
    require_descendant "$NPM_WORK_DIR" "$WORK_ROOT" "npm work"
    require_descendant "$NODE_DOWNLOAD_DIR" "$WORK_ROOT" "Node download"
    require_descendant "$ARCHIVE_PATH" "$DIST_ROOT" "archive"

    mkdir -p "$WORK_ROOT" "$DIST_ROOT"
    rm -rf "$STAGE_DIR" "$NPM_WORK_DIR" "$NODE_DOWNLOAD_DIR"
    rm -f "$ARCHIVE_PATH" "$ARCHIVE_PATH.sha256"
    mkdir -p \
        "$STAGE_DIR/app" \
        "$STAGE_DIR/rpms" \
        "$STAGE_DIR/node-runtime" \
        "$STAGE_DIR/python-wheels" \
        "$STAGE_DIR/npm-cache" \
        "$NPM_WORK_DIR" \
        "$NODE_DOWNLOAD_DIR"
}

download_rpms() {
    run_ubi \
        -v "$SOURCE_ROOT:/src:ro" \
        -v "$STAGE_DIR:/bundle" \
        -- \
        bash -euo pipefail -c '
            dnf install -y dnf-plugins-core >/dev/null
            mapfile -t packages < /src/offline/rhel8-packages.txt
            ((${#packages[@]} > 0))
            dnf download --resolve --alldeps \
                --destdir /bundle/rpms "${packages[@]}"
            find /bundle/rpms -maxdepth 1 -type f -name "*.rpm" -print -quit \
                | grep -q .
        '
    require_nonempty_directory "$STAGE_DIR/rpms" "RPM closure"
}

download_node_runtime() {
    local node_base_url="https://nodejs.org/dist/v${NODE_VERSION}"
    curl --fail --location --retry 3 \
        --output "$NODE_DOWNLOAD_DIR/$NODE_ARCHIVE" \
        "$node_base_url/$NODE_ARCHIVE"
    curl --fail --location --retry 3 \
        --output "$NODE_DOWNLOAD_DIR/SHASUMS256.txt" \
        "$node_base_url/SHASUMS256.txt"

    run_ubi \
        -v "$NODE_DOWNLOAD_DIR:/downloads:ro" \
        -v "$STAGE_DIR/node-runtime:/runtime" \
        -- \
        bash -euo pipefail -c '
            dnf install -y tar xz >/dev/null
            cd /downloads
            awk -v archive="$1" '\''$2 == archive {print}'\'' SHASUMS256.txt \
                > /tmp/node-checksum
            test "$(wc -l < /tmp/node-checksum)" -eq 1
            sha256sum --check /tmp/node-checksum
            tar -xJf "$1" -C /runtime --strip-components=1
        ' _ "$NODE_ARCHIVE"

    RESOLVED_NPM_VERSION=$(run_ubi \
        -v "$STAGE_DIR/node-runtime:/runtime:ro" \
        -- \
        bash -euo pipefail -c '
            test "$(/runtime/bin/node --version)" = "v$1"
            export PATH="/runtime/bin:$PATH"
            npm_version=$(npm --version)
            case "$npm_version" in
                "$2".*) ;;
                *) printf "unexpected npm version: %s\n" "$npm_version" >&2; exit 1 ;;
            esac
            printf "%s" "$npm_version"
        ' _ "$NODE_VERSION" "$NPM_MAJOR_VERSION")
    require_nonempty_directory "$STAGE_DIR/node-runtime" "Node runtime"
}

download_python_wheels() {
    run_ubi \
        -v "$SOURCE_ROOT:/src:ro" \
        -v "$STAGE_DIR:/bundle" \
        -- \
        bash -euo pipefail -c '
            dnf install -y python3.12 python3.12-pip >/dev/null
            python3.12 -m pip download --only-binary=:all: \
                --dest /bundle/python-wheels \
                -r /src/requirements-offline.txt
            find /bundle/python-wheels -maxdepth 1 -type f -name "*.whl" -print -quit \
                | grep -q .
        '
    require_nonempty_directory "$STAGE_DIR/python-wheels" "Python wheelhouse"
}

populate_npm_cache() {
    git -C "$SOURCE_ROOT" cat-file -e HEAD:web/package.json
    git -C "$SOURCE_ROOT" cat-file -e HEAD:web/package-lock.json
    git -C "$SOURCE_ROOT" show HEAD:web/package.json > "$NPM_WORK_DIR/package.json"
    git -C "$SOURCE_ROOT" show HEAD:web/package-lock.json > "$NPM_WORK_DIR/package-lock.json"

    run_ubi \
        -v "$STAGE_DIR/node-runtime:/runtime:ro" \
        -v "$NPM_WORK_DIR:/workspace" \
        -v "$STAGE_DIR/npm-cache:/bundle/npm-cache" \
        -w /workspace \
        -- \
        bash -euo pipefail -c '
            export PATH="/runtime/bin:$PATH"
            test "$(node --version)" = "v$1"
            case "$(npm --version)" in "$2".*) ;; *) exit 1 ;; esac
            npm ci --cache /bundle/npm-cache
            npm audit --omit=dev --audit-level=high --cache /bundle/npm-cache
        ' _ "$NODE_VERSION" "$NPM_MAJOR_VERSION"
    require_nonempty_directory "$STAGE_DIR/npm-cache" "npm cache"
}

copy_application() {
    [[ -x "$INSTALLER_SCRIPT" ]] || fail "offline installer is unavailable"
    [[ -x "$VERIFIER_SCRIPT" ]] || fail "offline verifier is unavailable"

    rmdir "$STAGE_DIR/app"
    "$COPY_APPLICATION_SCRIPT" "$SOURCE_ROOT" "$STAGE_DIR/app"
    install -m 0755 "$INSTALLER_SCRIPT" "$STAGE_DIR/install-offline.sh"
    install -m 0755 "$VERIFIER_SCRIPT" "$STAGE_DIR/verify-offline.sh"
}

write_version_manifest() {
    local source_commit
    local frontend_versions
    source_commit=$(git -C "$SOURCE_ROOT" rev-parse HEAD)
    frontend_versions=$(python3 - "$SOURCE_ROOT/web/package.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as package_file:
    package = json.load(package_file)

for dependency in ("next", "react", "react-dom", "chart.js", "react-chartjs-2"):
    print(f"frontend_{dependency}={package['dependencies'][dependency]}")
PY
)

    {
        printf 'target_rhel=%s\n' "$TARGET_RHEL_VERSION"
        printf 'target_arch=%s\n' "$TARGET_ARCH"
        printf 'container_platform=%s\n' "$CONTAINER_PLATFORM"
        printf 'resolver_image=%s\n' "$UBI_IMAGE"
        printf 'python=%s\n' "3.12"
        printf 'node=%s\n' "$NODE_VERSION"
        printf 'npm=%s\n' "$RESOLVED_NPM_VERSION"
        printf '%s\n' "$frontend_versions"
        while IFS= read -r dependency || [[ -n "$dependency" ]]; do
            printf 'backend_dependency=%s\n' "$dependency"
        done < "$SOURCE_ROOT/requirements-offline.txt"
        printf 'source_commit=%s\n' "$source_commit"
    } > "$STAGE_DIR/VERSIONS.txt"
}

write_checksums() {
    rm -f "$STAGE_DIR/MANIFEST.sha256"
    run_ubi \
        -v "$STAGE_DIR:/bundle" \
        -w /bundle \
        -- \
        bash -euo pipefail -c '
            dnf install -y coreutils findutils >/dev/null
            find . -type f ! -path ./MANIFEST.sha256 -print0 \
                | LC_ALL=C sort -z \
                | xargs -0 sha256sum > MANIFEST.sha256
            test -s MANIFEST.sha256
        '
}

run_clean_room() {
    "$CLEAN_ROOM_SCRIPT" "$STAGE_DIR"
}

create_archive() {
    local source_date_epoch
    source_date_epoch=$(git -C "$SOURCE_ROOT" show -s --format=%ct HEAD)

    run_ubi \
        -v "$STAGE_DIR:/stage:ro" \
        -v "$DIST_ROOT:/dist" \
        -e "SOURCE_DATE_EPOCH=$source_date_epoch" \
        -e "ARCHIVE_NAME=$ARCHIVE_NAME" \
        -- \
        bash -euo pipefail -c '
            dnf install -y gzip tar >/dev/null
            GZIP=-n tar --sort=name --mtime="@$SOURCE_DATE_EPOCH" \
                --owner=0 --group=0 --numeric-owner \
                -C /stage -czf "/dist/$ARCHIVE_NAME" .
            cd /dist
            sha256sum "$ARCHIVE_NAME" > "$ARCHIVE_NAME.sha256"
        '
    [[ -s "$ARCHIVE_PATH" ]] || fail "offline archive was not created"
    [[ -s "$ARCHIVE_PATH.sha256" ]] || fail "archive checksum was not created"
}

main() {
    preflight
    test_source
    prepare_stage
    download_rpms
    download_node_runtime
    download_python_wheels
    populate_npm_cache
    copy_application
    write_version_manifest
    write_checksums
    run_clean_room
    create_archive
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
