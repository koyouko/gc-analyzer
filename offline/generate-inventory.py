#!/usr/bin/env python3
import hashlib
import os
import posixpath
import stat
import sys
from pathlib import Path


MANIFEST_PATHS = "MANIFEST.paths"
MANIFEST_SYMLINKS = "MANIFEST.symlinks"
MANIFEST_SHA256 = "MANIFEST.sha256"
MANIFEST_NAMES = {MANIFEST_PATHS, MANIFEST_SYMLINKS, MANIFEST_SHA256}


def fail(message):
    raise ValueError(message)


def validate_text(value, label):
    if "\n" in value or "\r" in value or "\t" in value:
        fail(f"{label} contains a newline, carriage return, or tab: {value!r}")


def is_within(path, root):
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def validate_symlink(root, relative_path, link_path):
    target = os.readlink(link_path)
    validate_text(target, f"symlink target for {relative_path}")
    if posixpath.isabs(target):
        fail(f"absolute symlink target is forbidden: {relative_path} -> {target}")

    lexical_target = posixpath.normpath(
        posixpath.join(posixpath.dirname(relative_path), target)
    )
    if lexical_target == ".." or lexical_target.startswith("../"):
        fail(f"escaping symlink target is forbidden: {relative_path} -> {target}")

    try:
        resolved_target = link_path.resolve(strict=True)
    except (FileNotFoundError, RuntimeError, OSError) as error:
        fail(f"broken symlink is forbidden: {relative_path} -> {target}: {error}")
    if not is_within(resolved_target, root):
        fail(f"escaping symlink target is forbidden: {relative_path} -> {target}")
    return target


def scan_entries(root):
    entries = []
    symlinks = []

    def visit(directory, relative_directory=""):
        children = sorted(os.scandir(directory), key=lambda entry: os.fsencode(entry.name))
        for child in children:
            relative_path = (
                f"{relative_directory}/{child.name}"
                if relative_directory
                else child.name
            )
            validate_text(relative_path, "payload path")
            if relative_path in MANIFEST_NAMES:
                continue

            metadata = child.stat(follow_symlinks=False)
            mode = f"{stat.S_IMODE(metadata.st_mode):04o}"
            child_path = Path(child.path)
            if stat.S_ISREG(metadata.st_mode):
                entries.append((relative_path, "file", mode))
            elif stat.S_ISDIR(metadata.st_mode):
                entries.append((relative_path, "directory", mode))
                visit(child.path, relative_path)
            elif stat.S_ISLNK(metadata.st_mode):
                target = validate_symlink(root, relative_path, child_path)
                entries.append((relative_path, "symlink", "0777"))
                symlinks.append((relative_path, target))
            else:
                fail(f"unsupported entry type: {relative_path}")

    visit(root)
    entries.sort(key=lambda item: os.fsencode(item[0]))
    symlinks.sort(key=lambda item: os.fsencode(item[0]))
    return entries, symlinks


def write_text(path, lines):
    with path.open("w", encoding="utf-8", newline="\n") as output:
        for line in lines:
            output.write(line)
            output.write("\n")


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def generate(root_argument):
    root = Path(root_argument)
    if not root.is_dir() or root.is_symlink():
        fail(f"stage must be a real directory: {root}")
    root = root.resolve(strict=True)

    for manifest_name in MANIFEST_NAMES:
        manifest = root / manifest_name
        if manifest.is_dir() and not manifest.is_symlink():
            fail(f"manifest path must not be a directory: {manifest_name}")
        if manifest.exists() or manifest.is_symlink():
            manifest.unlink()

    entries, symlinks = scan_entries(root)
    write_text(
        root / MANIFEST_PATHS,
        (f"{entry_type}\t{mode}\t{path}" for path, entry_type, mode in entries),
    )
    write_text(
        root / MANIFEST_SYMLINKS,
        (f"{path}\t{target}" for path, target in symlinks),
    )

    regular_files = [path for path, entry_type, _ in entries if entry_type == "file"]
    regular_files.extend([MANIFEST_PATHS, MANIFEST_SYMLINKS])
    regular_files.sort(key=os.fsencode)
    write_text(
        root / MANIFEST_SHA256,
        (f"{sha256_file(root / path)}  {path}" for path in regular_files),
    )


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} STAGE_DIRECTORY", file=sys.stderr)
        return 2
    try:
        generate(sys.argv[1])
    except (OSError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
