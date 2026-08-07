#!/usr/bin/env bash
set -euo pipefail
umask 022

TARGET_RHEL_VERSION="8.10"
TARGET_ARCH="x86_64"
CONTAINER_PLATFORM="linux/amd64"
UBI_IMAGE_TAG="registry.access.redhat.com/ubi8/ubi:8.10"
NODE_VERSION="22.22.3"
NPM_MAJOR_VERSION="10"
PYTEST_VERSION="9.1.1"
NODE_TEST_IMAGE_TAG="node:22.22.3-bookworm-slim"
NODE_ARCHIVE="node-v${NODE_VERSION}-linux-x64.tar.xz"
STAGE_NAME="gc-analyzer-offline"
ARCHIVE_NAME="gc-analyzer-rhel8.10-x86_64-offline.tar.gz"
MANAGED_ROOT_MARKER=".gc-analyzer-bundle-root"
MANAGED_ROOT_MAGIC="GC_ANALYZER_BUNDLE_MANAGED_ROOT_V1"

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ORIGINAL_SOURCE_ROOT=$(git -C "$SCRIPT_DIR/.." rev-parse --show-toplevel)
STARTUP_SOURCE_COMMIT=$(git -C "$ORIGINAL_SOURCE_ROOT" rev-parse HEAD)
SOURCE_COMMIT=${GC_ANALYZER_SOURCE_COMMIT:-"$STARTUP_SOURCE_COMMIT"}
WORK_ROOT=${GC_ANALYZER_BUNDLE_WORK_ROOT:-"$SCRIPT_DIR/work"}
DIST_ROOT=${GC_ANALYZER_BUNDLE_DIST_ROOT:-"$SCRIPT_DIR/dist"}
STAGE_DIR=${GC_ANALYZER_BUNDLE_STAGE_DIR:-"$WORK_ROOT/$STAGE_NAME"}
SOURCE_SNAPSHOT_DIR=${GC_ANALYZER_SOURCE_SNAPSHOT_DIR:-"$WORK_ROOT/source-snapshot"}
SOURCE_TEST_DIR=${GC_ANALYZER_SOURCE_TEST_DIR:-"$WORK_ROOT/source-test"}
NPM_WORK_DIR="$WORK_ROOT/npm-work"
NODE_DOWNLOAD_DIR="$WORK_ROOT/node-download"
RPM_ROOTS_FILE="$SOURCE_SNAPSHOT_DIR/offline/rhel8-packages.txt"
COPY_APPLICATION_SCRIPT="$SOURCE_SNAPSHOT_DIR/offline/copy-application.sh"
INSTALLER_SCRIPT="$SOURCE_SNAPSHOT_DIR/offline/install-offline.sh"
VERIFIER_SCRIPT="$SOURCE_SNAPSHOT_DIR/offline/verify-offline.sh"
CLEAN_ROOM_SCRIPT="$SOURCE_SNAPSHOT_DIR/offline/test-clean-room.sh"
INVENTORY_SCRIPT="$SOURCE_SNAPSHOT_DIR/offline/generate-inventory.py"
ARCHIVE_PATH="$DIST_ROOT/$ARCHIVE_NAME"
HOST_UID=$(id -u)
HOST_GID=$(id -g)

RESOLVED_UBI_IMAGE_ID=""
RESOLVED_UBI_IMAGE_DIGEST=""
RESOLVED_NODE_TEST_IMAGE_ID=""
RESOLVED_NODE_TEST_IMAGE_DIGEST=""
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

canonical_path() {
    python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$1"
}

strip_trailing_separators() {
    local path=$1
    while [[ "$path" != "/" && "$path" == */ ]]; do
        path=${path%/}
    done
    printf '%s' "$path"
}

normalize_managed_output_roots() {
    WORK_ROOT=$(strip_trailing_separators "$WORK_ROOT")
    DIST_ROOT=$(strip_trailing_separators "$DIST_ROOT")
}

validate_managed_root() {
    local root=$1
    local label=$2
    [[ -n "$root" ]] || fail "$label must not be empty"
    [[ "$root" == /* ]] || fail "$label must be an absolute path: $root"
    [[ "$root" != "/" ]] || fail "$label must not be /"
    [[ ! -L "$root" ]] || fail "$label must not be a symlink: $root"
    if [[ -e "$root" ]]; then
        [[ -d "$root" ]] || fail "$label must be a directory: $root"
    fi
}

validate_managed_descendant() {
    local path=$1
    local root=$2
    local label=$3
    require_descendant "$path" "$root" "$label"
    [[ ! -L "$path" ]] || fail "$label must not be a symlink: $path"
}

validate_managed_output_paths() {
    local work_canonical
    local dist_canonical
    normalize_managed_output_roots
    validate_managed_root "$WORK_ROOT" "WORK_ROOT"
    validate_managed_root "$DIST_ROOT" "DIST_ROOT"
    work_canonical=$(canonical_path "$WORK_ROOT")
    dist_canonical=$(canonical_path "$DIST_ROOT")
    [[ "$work_canonical" != "$dist_canonical" ]] \
        || fail "WORK_ROOT and DIST_ROOT must be different directories"
    case "$work_canonical/" in
        "$dist_canonical"/*)
            fail "WORK_ROOT and DIST_ROOT must not overlap"
            ;;
    esac
    case "$dist_canonical/" in
        "$work_canonical"/*)
            fail "WORK_ROOT and DIST_ROOT must not overlap"
            ;;
    esac

    validate_managed_descendant "$STAGE_DIR" "$WORK_ROOT" "stage"
    validate_managed_descendant \
        "$SOURCE_SNAPSHOT_DIR" "$WORK_ROOT" "source snapshot"
    validate_managed_descendant "$SOURCE_TEST_DIR" "$WORK_ROOT" "source test"
    validate_managed_descendant "$NPM_WORK_DIR" "$WORK_ROOT" "npm work"
    validate_managed_descendant \
        "$NODE_DOWNLOAD_DIR" "$WORK_ROOT" "Node download"
    validate_managed_descendant "$ARCHIVE_PATH" "$DIST_ROOT" "archive"
}

assert_managed_root_marker() {
    local root=$1
    local label=$2
    local marker="$root/$MANAGED_ROOT_MARKER"
    local marker_size
    [[ -f "$marker" && ! -L "$marker" ]] \
        || fail "$label is not a marked GC Analyzer bundle directory: $root"
    marker_size=$(wc -c < "$marker" | tr -d ' ')
    [[ "$marker_size" == "${#MANAGED_ROOT_MAGIC}" ]] \
        || fail "$label marker has invalid content: $marker"
    [[ "$(cat "$marker")" == "$MANAGED_ROOT_MAGIC" ]] \
        || fail "$label marker has invalid content: $marker"
}

initialize_managed_root() {
    local root=$1
    local label=$2
    local marker="$root/$MANAGED_ROOT_MARKER"
    if [[ ! -e "$root" ]]; then
        mkdir -p "$root"
    fi
    [[ -d "$root" && ! -L "$root" ]] \
        || fail "$label must be a regular directory: $root"
    if [[ -z "$(find "$root" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
        printf '%s' "$MANAGED_ROOT_MAGIC" > "$marker"
    fi
    assert_managed_root_marker "$root" "$label"
}

initialize_managed_output_roots() {
    validate_managed_output_paths
    initialize_managed_root "$WORK_ROOT" "WORK_ROOT"
    initialize_managed_root "$DIST_ROOT" "DIST_ROOT"
    validate_managed_output_paths
}

assert_managed_output_roots() {
    validate_managed_output_paths
    assert_managed_root_marker "$WORK_ROOT" "WORK_ROOT"
    assert_managed_root_marker "$DIST_ROOT" "DIST_ROOT"
}

assert_source_snapshot() {
    [[ -d "$SOURCE_SNAPSHOT_DIR/.git" ]] \
        || fail "standalone source snapshot is missing: $SOURCE_SNAPSHOT_DIR"
    [[ "$(git -C "$SOURCE_SNAPSHOT_DIR" rev-parse HEAD)" == "$SOURCE_COMMIT" ]] \
        || fail "source snapshot HEAD changed"
    git -C "$SOURCE_SNAPSHOT_DIR" diff --quiet "$SOURCE_COMMIT" -- \
        || fail "source snapshot worktree changed"
    git -C "$SOURCE_SNAPSHOT_DIR" diff --cached --quiet "$SOURCE_COMMIT" -- \
        || fail "source snapshot index changed"
    [[ -z "$(git -C "$SOURCE_SNAPSHOT_DIR" ls-files --others --exclude-standard)" ]] \
        || fail "source snapshot has untracked files"
}

resolve_image() {
    local image_tag=$1
    local id_variable=$2
    local digest_variable=$3
    local image_id
    local image_digest
    local image_platform

    image_id=$(docker image inspect --format '{{.Id}}' "$image_tag")
    image_digest=$(docker image inspect --format '{{index .RepoDigests 0}}' "$image_tag")
    image_platform=$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image_id")
    [[ "$image_id" == sha256:* ]] || fail "resolver image has no immutable ID: $image_tag"
    [[ "$image_digest" == *@sha256:* ]] \
        || fail "resolver image has no repository digest: $image_tag"
    [[ "$image_platform" == "$CONTAINER_PLATFORM" ]] \
        || fail "resolver image platform mismatch: $image_tag is $image_platform"
    printf -v "$id_variable" '%s' "$image_id"
    printf -v "$digest_variable" '%s' "$image_digest"
}

run_ubi() {
    local docker_options=()
    [[ -n "$RESOLVED_UBI_IMAGE_ID" ]] || fail "UBI resolver image is not frozen"
    while (($# > 0)) && [[ "$1" != "--" ]]; do
        (($# >= 2)) || fail "Docker option is missing a value: $1"
        docker_options+=("$1" "$2")
        shift 2
    done
    [[ ${1:-} == "--" ]] || fail "run_ubi requires -- before the container command"
    shift
    (($# > 0)) || fail "run_ubi requires a container command"

    docker run --rm --platform "$CONTAINER_PLATFORM" \
        "${docker_options[@]}" "$RESOLVED_UBI_IMAGE_ID" "$@"
}

repair_build_ownership() {
    initialize_managed_output_roots
    [[ -n "$RESOLVED_UBI_IMAGE_ID" ]] || fail "UBI resolver image is not frozen"
    docker run --rm --platform "$CONTAINER_PLATFORM" \
        -e "HOST_UID=$HOST_UID" \
        -e "HOST_GID=$HOST_GID" \
        -v "$WORK_ROOT:/work" \
        -v "$DIST_ROOT:/dist" \
        "$RESOLVED_UBI_IMAGE_ID" \
        bash -euo pipefail -c \
            'chown -R "$HOST_UID:$HOST_GID" /work /dist'
}

create_source_snapshot() {
    assert_managed_output_roots
    rm -rf "$SOURCE_SNAPSHOT_DIR"
    git clone --no-local --no-checkout \
        "$ORIGINAL_SOURCE_ROOT" "$SOURCE_SNAPSHOT_DIR"
    git -C "$SOURCE_SNAPSHOT_DIR" checkout --detach "$SOURCE_COMMIT"
    assert_source_snapshot
}

preflight() {
    local current_head
    ((BASH_VERSINFO[0] >= 4)) || fail "Bash 4 or newer is required"
    require_command curl
    require_command docker
    require_command git
    require_command id
    require_command python3
    require_command tar
    initialize_managed_output_roots

    current_head=$(git -C "$ORIGINAL_SOURCE_ROOT" rev-parse HEAD)
    [[ "$current_head" == "$SOURCE_COMMIT" ]] \
        || fail "original repository HEAD changed after builder startup"
    git -C "$ORIGINAL_SOURCE_ROOT" diff --quiet "$SOURCE_COMMIT" -- \
        || fail "original Git worktree has modified tracked files"
    git -C "$ORIGINAL_SOURCE_ROOT" diff --cached --quiet "$SOURCE_COMMIT" -- \
        || fail "original Git index has staged changes"
    [[ -z "$(git -C "$ORIGINAL_SOURCE_ROOT" ls-files --others --exclude-standard)" ]] \
        || fail "original Git worktree has untracked files"

    docker info >/dev/null
    docker pull --platform "$CONTAINER_PLATFORM" "$UBI_IMAGE_TAG" >/dev/null
    docker pull --platform "$CONTAINER_PLATFORM" "$NODE_TEST_IMAGE_TAG" >/dev/null
    resolve_image "$UBI_IMAGE_TAG" \
        RESOLVED_UBI_IMAGE_ID RESOLVED_UBI_IMAGE_DIGEST
    resolve_image "$NODE_TEST_IMAGE_TAG" \
        RESOLVED_NODE_TEST_IMAGE_ID RESOLVED_NODE_TEST_IMAGE_DIGEST

    docker run --rm --platform "$CONTAINER_PLATFORM" \
        "$RESOLVED_UBI_IMAGE_ID" bash -euo pipefail -c '
            . /etc/os-release
            test "$VERSION_ID" = "8.10"
            test "$(uname -m)" = "x86_64"
        '
    docker run --rm --platform "$CONTAINER_PLATFORM" \
        "$RESOLVED_NODE_TEST_IMAGE_ID" sh -eu -c '
            test "$(uname -m)" = "x86_64"
            test "$(node --version)" = "v$1"
            case "$(npm --version)" in "$2".*) ;; *) exit 1 ;; esac
        ' _ "$NODE_VERSION" "$NPM_MAJOR_VERSION"

    repair_build_ownership
    create_source_snapshot
    [[ -s "$RPM_ROOTS_FILE" ]] || fail "RPM root manifest is missing or empty"
    git -C "$SOURCE_SNAPSHOT_DIR" cat-file -e \
        "$SOURCE_COMMIT:requirements-offline.txt"
    git -C "$SOURCE_SNAPSHOT_DIR" cat-file -e \
        "$SOURCE_COMMIT:web/package-lock.json"
    for required_path in \
        offline/copy-application.sh \
        offline/generate-inventory.py \
        offline/install-offline.sh \
        offline/verify-offline.sh \
        offline/test-clean-room.sh; do
        git -C "$SOURCE_SNAPSHOT_DIR" cat-file -e \
            "$SOURCE_COMMIT:$required_path" \
            || fail "required committed build input is missing: $required_path"
    done
}

test_source() {
    assert_managed_output_roots
    assert_source_snapshot
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

        docker run --rm --platform "$CONTAINER_PLATFORM" \
            -v "$SOURCE_SNAPSHOT_DIR:/snapshot:ro" \
            "$RESOLVED_UBI_IMAGE_ID" \
            bash -euo pipefail -c '
                . /etc/os-release
                test "$VERSION_ID" = "$1"
                test "$(uname -m)" = "x86_64"
                dnf install -y python3.12 python3.12-pip git tar gzip >/dev/null
                python3.12 -c "import sys; assert sys.version_info[:2] == (3, 12)"
                cp -a /snapshot /source
                python3.12 -m pip install --only-binary=:all: \
                    -r /source/requirements-offline.txt "pytest==$2"
                cd /source
                python3.12 -m pytest -q
                python3.12 -m compileall -q gcanalyzer seed tests
            ' _ "$TARGET_RHEL_VERSION" "$PYTEST_VERSION"

        git -C "$SOURCE_SNAPSHOT_DIR" archive --format=tar \
            "$SOURCE_COMMIT" -- web \
            | tar -xf - -C "$SOURCE_TEST_DIR"
        docker run --rm --platform "$CONTAINER_PLATFORM" \
            --user "$HOST_UID:$HOST_GID" \
            -e HOME=/tmp/node-home \
            -v "$SOURCE_TEST_DIR/web:/workspace" \
            -w /workspace \
            "$RESOLVED_NODE_TEST_IMAGE_ID" \
            sh -eu -c '
                mkdir -p "$HOME"
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
    assert_managed_output_roots
    [[ "$(basename "$STAGE_DIR")" == "$STAGE_NAME" ]] \
        || fail "stage directory must be named $STAGE_NAME"

    repair_build_ownership
    rm -rf "$STAGE_DIR" "$NPM_WORK_DIR" "$NODE_DOWNLOAD_DIR" "$SOURCE_TEST_DIR"
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
    assert_managed_output_roots
    assert_source_snapshot
    run_ubi \
        -e "HOST_UID=$HOST_UID" \
        -e "HOST_GID=$HOST_GID" \
        -v "$SOURCE_SNAPSHOT_DIR:/src:ro" \
        -v "$STAGE_DIR:/bundle" \
        -- \
        bash -euo pipefail -c '
            repair_container_ownership() {
                status=$?
                trap - EXIT
                chown -R "$HOST_UID:$HOST_GID" /bundle
                exit "$status"
            }
            trap repair_container_ownership EXIT
            umask 022
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
    assert_managed_output_roots
    curl --fail --location --retry 3 \
        --output "$NODE_DOWNLOAD_DIR/$NODE_ARCHIVE" \
        "$node_base_url/$NODE_ARCHIVE"
    curl --fail --location --retry 3 \
        --output "$NODE_DOWNLOAD_DIR/SHASUMS256.txt" \
        "$node_base_url/SHASUMS256.txt"

    run_ubi \
        -e "HOST_UID=$HOST_UID" \
        -e "HOST_GID=$HOST_GID" \
        -v "$NODE_DOWNLOAD_DIR:/downloads:ro" \
        -v "$STAGE_DIR/node-runtime:/runtime" \
        -- \
        bash -euo pipefail -c '
            repair_container_ownership() {
                status=$?
                trap - EXIT
                chown -R "$HOST_UID:$HOST_GID" /runtime
                exit "$status"
            }
            trap repair_container_ownership EXIT
            umask 022
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
    assert_managed_output_roots
    assert_source_snapshot
    run_ubi \
        -e "HOST_UID=$HOST_UID" \
        -e "HOST_GID=$HOST_GID" \
        -v "$SOURCE_SNAPSHOT_DIR:/src:ro" \
        -v "$STAGE_DIR:/bundle" \
        -- \
        bash -euo pipefail -c '
            repair_container_ownership() {
                status=$?
                trap - EXIT
                chown -R "$HOST_UID:$HOST_GID" /bundle
                exit "$status"
            }
            trap repair_container_ownership EXIT
            umask 022
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
    assert_managed_output_roots
    assert_source_snapshot
    git -C "$SOURCE_SNAPSHOT_DIR" show "$SOURCE_COMMIT:web/package.json" \
        > "$NPM_WORK_DIR/package.json"
    git -C "$SOURCE_SNAPSHOT_DIR" show "$SOURCE_COMMIT:web/package-lock.json" \
        > "$NPM_WORK_DIR/package-lock.json"

    run_ubi \
        --user "$HOST_UID:$HOST_GID" \
        -e HOME=/tmp/npm-home \
        -v "$STAGE_DIR/node-runtime:/runtime:ro" \
        -v "$NPM_WORK_DIR:/workspace" \
        -v "$STAGE_DIR/npm-cache:/bundle/npm-cache" \
        -w /workspace \
        -- \
        bash -euo pipefail -c '
            mkdir -p "$HOME"
            umask 022
            export PATH="/runtime/bin:$PATH"
            test "$(node --version)" = "v$1"
            case "$(npm --version)" in "$2".*) ;; *) exit 1 ;; esac
            npm ci --cache /bundle/npm-cache
            npm audit --omit=dev --audit-level=high --cache /bundle/npm-cache
        ' _ "$NODE_VERSION" "$NPM_MAJOR_VERSION"
    require_nonempty_directory "$STAGE_DIR/npm-cache" "npm cache"
}

copy_application() {
    assert_managed_output_roots
    assert_source_snapshot
    rmdir "$STAGE_DIR/app"
    "$COPY_APPLICATION_SCRIPT" "$SOURCE_SNAPSHOT_DIR" "$STAGE_DIR/app" "$SOURCE_COMMIT"
    git -C "$SOURCE_SNAPSHOT_DIR" show \
        "$SOURCE_COMMIT:offline/install-offline.sh" > "$STAGE_DIR/install-offline.sh"
    git -C "$SOURCE_SNAPSHOT_DIR" show \
        "$SOURCE_COMMIT:offline/verify-offline.sh" > "$STAGE_DIR/verify-offline.sh"
    git -C "$SOURCE_SNAPSHOT_DIR" show \
        "$SOURCE_COMMIT:offline/generate-inventory.py" > "$STAGE_DIR/inventory.py"
    chmod 0755 \
        "$STAGE_DIR/install-offline.sh" \
        "$STAGE_DIR/verify-offline.sh" \
        "$STAGE_DIR/inventory.py"
}

write_version_manifest() {
    local frontend_versions
    local backend_dependencies
    local artifact_inventory
    assert_source_snapshot

    frontend_versions=$(git -C "$SOURCE_SNAPSHOT_DIR" \
        show "$SOURCE_COMMIT:web/package.json" \
        | python3 -c '
import json
import sys
package = json.load(sys.stdin)
dependencies = package["dependencies"]
for dependency in ("next", "react", "react-dom", "chart.js", "react-chartjs-2"):
    print(f"frontend_{dependency}={dependencies[dependency]}")
')
    backend_dependencies=$(git -C "$SOURCE_SNAPSHOT_DIR" \
        show "$SOURCE_COMMIT:requirements-offline.txt")
    artifact_inventory=$(python3 - "$STAGE_DIR/rpms" "$STAGE_DIR/python-wheels" <<'PY'
import hashlib
import sys
from pathlib import Path

def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

for label, directory in (("rpm_artifact", Path(sys.argv[1])), ("python_wheel", Path(sys.argv[2]))):
    for artifact in sorted(directory.iterdir(), key=lambda path: path.name.encode()):
        if not artifact.is_file() or artifact.is_symlink():
            raise SystemExit(f"unexpected non-file artifact: {artifact}")
        if "\n" in artifact.name or "\r" in artifact.name or "\t" in artifact.name:
            raise SystemExit(f"ambiguous artifact filename: {artifact.name!r}")
        print(f"{label}={artifact.name} sha256={sha256_file(artifact)}")
PY
)

    {
        printf 'target_rhel=%s\n' "$TARGET_RHEL_VERSION"
        printf 'target_arch=%s\n' "$TARGET_ARCH"
        printf 'container_platform=%s\n' "$CONTAINER_PLATFORM"
        printf 'resolver_ubi_tag=%s\n' "$UBI_IMAGE_TAG"
        printf 'resolver_ubi_id=%s\n' "$RESOLVED_UBI_IMAGE_ID"
        printf 'resolver_ubi_digest=%s\n' "$RESOLVED_UBI_IMAGE_DIGEST"
        printf 'resolver_node_test_tag=%s\n' "$NODE_TEST_IMAGE_TAG"
        printf 'resolver_node_test_id=%s\n' "$RESOLVED_NODE_TEST_IMAGE_ID"
        printf 'resolver_node_test_digest=%s\n' "$RESOLVED_NODE_TEST_IMAGE_DIGEST"
        printf 'python=%s\n' "3.12"
        printf 'pytest=%s\n' "$PYTEST_VERSION"
        printf 'node=%s\n' "$NODE_VERSION"
        printf 'npm=%s\n' "$RESOLVED_NPM_VERSION"
        printf '%s\n' "$frontend_versions"
        while IFS= read -r dependency || [[ -n "$dependency" ]]; do
            printf 'backend_dependency=%s\n' "$dependency"
        done <<< "$backend_dependencies"
        printf '%s\n' "$artifact_inventory"
        printf 'source_commit=%s\n' "$SOURCE_COMMIT"
    } > "$STAGE_DIR/VERSIONS.txt"
}

write_checksums() {
    assert_managed_output_roots
    assert_source_snapshot
    "$SOURCE_SNAPSHOT_DIR/offline/generate-inventory.py" "$STAGE_DIR"
    [[ -s "$STAGE_DIR/MANIFEST.paths" ]] || fail "path inventory is empty"
    [[ -f "$STAGE_DIR/MANIFEST.symlinks" ]] || fail "symlink inventory is missing"
    [[ -s "$STAGE_DIR/MANIFEST.sha256" ]] || fail "checksum inventory is empty"
}

run_clean_room() {
    assert_managed_output_roots
    assert_source_snapshot
    "$CLEAN_ROOM_SCRIPT" "$STAGE_DIR"
}

create_archive() {
    local source_date_epoch
    assert_managed_output_roots
    assert_source_snapshot
    source_date_epoch=$(git -C "$SOURCE_SNAPSHOT_DIR" \
        show -s --format=%ct "$SOURCE_COMMIT")

    run_ubi \
        -e "HOST_UID=$HOST_UID" \
        -e "HOST_GID=$HOST_GID" \
        -v "$WORK_ROOT:/work:ro" \
        -v "$DIST_ROOT:/dist" \
        -e "SOURCE_DATE_EPOCH=$source_date_epoch" \
        -e "ARCHIVE_NAME=$ARCHIVE_NAME" \
        -e "STAGE_NAME=$STAGE_NAME" \
        -- \
        bash -euo pipefail -c '
            repair_container_ownership() {
                status=$?
                trap - EXIT
                chown -R "$HOST_UID:$HOST_GID" /dist
                exit "$status"
            }
            trap repair_container_ownership EXIT
            umask 022
            dnf install -y gzip tar >/dev/null
            GZIP=-n tar --sort=name --mtime="@$SOURCE_DATE_EPOCH" \
                --owner=0 --group=0 --numeric-owner \
                -C /work -czf "/dist/$ARCHIVE_NAME" "$STAGE_NAME"
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
