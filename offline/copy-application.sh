#!/usr/bin/env bash
set -euo pipefail

fail() {
    printf 'ERROR: %s\n' "$1" >&2
    exit 1
}

if [[ $# -ne 2 ]]; then
    fail "usage: $0 SOURCE_REPOSITORY DESTINATION_DIRECTORY"
fi

source_repository=$1
destination=$2
manifest_path=offline/app-files.txt
manifest="$source_repository/$manifest_path"

[[ -d "$source_repository" ]] || fail "source repository does not exist: $source_repository"
[[ -f "$manifest" ]] || fail "application allowlist does not exist: $manifest"
git -C "$source_repository" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
    || fail "source is not a Git repository: $source_repository"
git -C "$source_repository" cat-file -e "HEAD:$manifest_path" \
    || fail "application allowlist is not present in HEAD: $manifest_path"
git -C "$source_repository" diff --quiet HEAD -- "$manifest_path" \
    || fail "worktree application allowlist differs from HEAD: $manifest_path"
head_manifest=$(git -C "$source_repository" show "HEAD:$manifest_path")

destination_exists=false
if [[ -e "$destination" || -L "$destination" ]]; then
    [[ -d "$destination" && ! -L "$destination" ]] \
        || fail "destination must be an empty directory: $destination"
    [[ -z "$(find "$destination" -mindepth 1 -print -quit)" ]] \
        || fail "destination must be empty: $destination"
    destination_exists=true
fi

tracked_files=()
entry_count=0
tracked_file_count=0

while IFS= read -r entry || [[ -n "$entry" ]]; do
    [[ -n "$entry" ]] || fail "application allowlist contains an empty entry"
    entry_count=$((entry_count + 1))

    index_output=$(git -C "$source_repository" ls-files -- "$entry")
    [[ -n "$index_output" ]] \
        || fail "allowlist entry matches no tracked files: $entry"
    head_output=$(git -C "$source_repository" ls-tree -r --name-only HEAD -- "$entry")
    [[ -n "$head_output" ]] \
        || fail "allowlist entry matches no files in HEAD: $entry"
    [[ "$index_output" == "$head_output" ]] \
        || fail "tracked expansion differs from HEAD for allowlist entry: $entry"

    while IFS= read -r tracked_path || [[ -n "$tracked_path" ]]; do
        case "/$tracked_path/" in
            */.env/*|*/.venv/*|*/users.json/*|*/.session_secret/*|*/node_modules/*|*/.next/*|*/__pycache__/*)
                fail "forbidden tracked path: $tracked_path"
                ;;
        esac
        case "$tracked_path" in
            *.db|*.pyc)
                fail "forbidden tracked path: $tracked_path"
                ;;
        esac

        tree_entry=$(git -C "$source_repository" ls-tree HEAD -- "$tracked_path")
        [[ -n "$tree_entry" ]] \
            || fail "tracked path is not present in HEAD: $tracked_path"
        entry_metadata=${tree_entry%%$'\t'*}
        git_mode=${entry_metadata%% *}
        metadata_without_mode=${entry_metadata#* }
        object_type=${metadata_without_mode%% *}
        case "$git_mode:$object_type" in
            100644:blob|100755:blob)
                ;;
            *)
                fail "unsupported Git mode $git_mode for tracked path: $tracked_path"
                ;;
        esac

        tracked_files[$tracked_file_count]=$tracked_path
        tracked_file_count=$((tracked_file_count + 1))
    done <<< "$head_output"
done <<< "$head_manifest"

[[ $entry_count -gt 0 ]] || fail "application allowlist is empty: $manifest"

if [[ "$destination_exists" == false ]]; then
    mkdir -p "$destination"
fi

git -C "$source_repository" archive --format=tar HEAD -- "${tracked_files[@]}" \
    | tar -xf - -C "$destination"
