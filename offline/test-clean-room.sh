#!/usr/bin/env bash
set -euo pipefail
umask 022

# Usage: test-clean-room.sh /absolute/staged-bundle [sha256:IMAGE_ID]
# The builder may instead supply GC_ANALYZER_CLEAN_ROOM_IMAGE. Tags are
# deliberately not resolved or pulled here.
fail() {
    printf 'ERROR [clean-room]: %s\n' "$1" >&2
    exit 1
}

((BASH_VERSINFO[0] >= 4)) || fail "Bash 4 or newer is required"
(($# >= 1 && $# <= 2)) \
    || fail "usage: test-clean-room.sh /absolute/staged-bundle [sha256:IMAGE_ID]"

STAGE_DIR=$1
IMAGE_ID=${2-${GC_ANALYZER_CLEAN_ROOM_IMAGE:-}}
[[ "$IMAGE_ID" =~ ^sha256:[0-9a-f]{64}$ ]] \
    || fail "a frozen UBI image ID (sha256 plus 64 hex digits) is required"

[[ "$STAGE_DIR" == /* && "$STAGE_DIR" != / ]] \
    || fail "stage must be an absolute path other than /"
[[ -d "$STAGE_DIR" && ! -L "$STAGE_DIR" ]] \
    || fail "stage must be an existing, real nonsymlink directory"
# Docker --mount parses CSV. Reject its delimiters, not ordinary shell text.
[[ "$STAGE_DIR" != *,* && "$STAGE_DIR" != *\"* && ! "$STAGE_DIR" =~ [[:cntrl:]] ]] \
    || fail "stage path contains ambiguous mount characters"
CANONICAL_STAGE=$(cd -P -- "$STAGE_DIR" && pwd -P)
[[ "$CANONICAL_STAGE" == "$STAGE_DIR" ]] \
    || fail "stage path must be normalized and contain no symlink components"
for entrypoint in install-offline.sh verify-offline.sh; do
    [[ -f "$STAGE_DIR/$entrypoint" && -x "$STAGE_DIR/$entrypoint" \
        && ! -L "$STAGE_DIR/$entrypoint" ]] \
        || fail "stage $entrypoint must be a real executable file"
done

command -v docker >/dev/null 2>&1 || fail "required command is unavailable: docker"
IMAGE_METADATA=$(docker image inspect \
    --format '{{.Id}} {{.Os}}/{{.Architecture}}' "$IMAGE_ID") \
    || fail "frozen UBI image is not available locally: $IMAGE_ID"
[[ "$IMAGE_METADATA" == "$IMAGE_ID linux/amd64" ]] \
    || fail "local image ID or platform does not match the frozen linux/amd64 image"

# No writable host mounts, published ports, inherited shell program, or mutable
# image references. The writable /bundle lives only in this disposable container.
exec docker run --rm --init -i --pull never --platform linux/amd64 \
    --network none --user 0:0 --entrypoint /bin/bash \
    --mount "type=bind,source=$STAGE_DIR,target=/incoming,readonly" \
    "$IMAGE_ID" -euo pipefail -s <<'CLEAN_ROOM'
fail() {
    printf 'ERROR [clean-room container]: %s\n' "$1" >&2
    exit 1
}

((BASH_VERSINFO[0] >= 4)) || fail "Bash 4 or newer is required"
[[ "$EUID" -eq 0 ]] || fail "container must run as root"
[[ -r /etc/os-release ]] || fail "/etc/os-release is unavailable"
os_id=""
os_version=""
# Read identity as data, without executing os-release as shell code.
while IFS='=' read -r key value || [[ -n "$key" ]]; do
    case "$key" in
        ID)
            case "$value" in rhel|'"rhel"'|"'rhel'") os_id=rhel ;;
                *) fail "operating system must be RHEL" ;;
            esac
            ;;
        VERSION_ID)
            case "$value" in 8.10|'"8.10"'|"'8.10'") os_version=8.10 ;;
                *) fail "RHEL version must be exactly 8.10" ;;
            esac
            ;;
    esac
done < /etc/os-release
[[ "$os_id" == rhel && "$os_version" == 8.10 ]] \
    || fail "container must identify as RHEL 8.10"
[[ "$(uname -m)" == x86_64 ]] || fail "architecture must be exactly x86_64"

assert_no_application_processes() {
    local command_line argument
    [[ -r /proc/1/cmdline ]] || fail "process inspection is unavailable"
    for command_line in /proc/[0-9]*/cmdline; do
        # A process may exit between glob expansion and opening its cmdline.
        [[ -e "$command_line" ]] || continue
        [[ -r "$command_line" ]] || fail "cannot inspect process: $command_line"
        while IFS= read -r -d '' argument; do
            case "$argument" in
                gcanalyzer.app|uvicorn|/opt/gc-analyzer/*|next-server*)
                    fail "application process leaked after verification: $command_line ($argument)"
                    ;;
            esac
        done < "$command_line"
    done
}

[[ ! -e /bundle && ! -L /bundle ]] || fail "/bundle already exists in clean-room image"
cp -a -- "/incoming" "/bundle"
assert_no_application_processes
printf 'Clean-room: first offline installation and verification\n'
"/bundle/install-offline.sh" --container-test
assert_no_application_processes
printf 'Clean-room: repeat offline installation and verification\n'
"/bundle/install-offline.sh" --container-test
assert_no_application_processes
printf 'Clean-room passed: both offline installations verified without application process leaks.\n'
CLEAN_ROOM
