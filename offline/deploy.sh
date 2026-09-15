#!/usr/bin/env bash
set -euo pipefail
umask 077

fail() {
    printf 'ERROR [offline deploy]: %s\n' "$1" >&2
    exit 1
}

((BASH_VERSINFO[0] >= 4)) || fail "Bash 4 or newer is required"
[[ "$EUID" -eq 0 ]] || fail "deployment must run as root"
(($# == 0)) || fail "deploy.sh accepts no arguments"
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
RELEASE_DIR=${GC_ANALYZER_RELEASE_DIR-"$SCRIPT_DIR/releases/rhel8.10-x86_64"}

# RHEL 8's DNF already provides Python 3.6 with hashlib, gzip and tarfile.
# A preinstalled Python 3 is also usable; no packages are downloaded here.
if [[ -x /usr/libexec/platform-python ]]; then
    PYTHON=/usr/libexec/platform-python
else
    PYTHON=$(command -v python3) || fail "platform-python or Python 3.6+ is required"
fi
exec "$PYTHON" -I -S - "$RELEASE_DIR" <<'PY'
import hashlib
import os
import posixpath
import re
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile


ARCHIVE = "gc-analyzer-rhel8.10-x86_64-offline.tar.gz"
BUNDLE = "gc-analyzer-offline"
CHUNK_LIMIT = 40 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def open_regular(path):
    # O_NONBLOCK also prevents a swapped-in FIFO from hanging verification.
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise ValueError("not a regular nonsymlink file: " + path)
    return os.fdopen(fd, "rb")


def read_manifest(release, name):
    with open_regular(os.path.join(release, name)) as stream:
        data = stream.read(1024 * 1024 + 1)
    require(0 < len(data) <= 1024 * 1024, "empty or oversized manifest: " + name)
    return data


def verify_manifests(release):
    require(os.path.isabs(release) and release != "/", "release path must be absolute and not /")
    require(os.path.normpath(release) == release, "release path must be normalized")
    require(os.path.realpath(release) == release and os.path.isdir(release),
            "release path must be a real directory without symlink components")
    records = read_manifest(release, "parts.sha256").splitlines(keepends=True)
    parts = []
    for number, record in enumerate(records):
        match = re.fullmatch(rb"([0-9a-f]{64})  (bundle\.part-[0-9]{4})\n?", record)
        require(match is not None, "malformed parts.sha256 record")
        digest, name = (value.decode("ascii") for value in match.groups())
        require(name == "bundle.part-{:04d}".format(number),
                "parts must be unique, contiguous and ordered from bundle.part-0000")
        parts.append((name, digest))
    expected = {name for name, _ in parts} | {"parts.sha256", "archive.sha256"}
    require(set(os.listdir(release)) == expected, "release has missing or unexpected files")
    archive_record = read_manifest(release, "archive.sha256")
    match = re.fullmatch(rb"([0-9a-f]{64})  " + re.escape(ARCHIVE.encode("ascii")) + rb"\n?",
                         archive_record)
    require(match is not None, "malformed archive.sha256 record")
    return parts, match.group(1).decode("ascii")


def reassemble(release, parts, expected_digest, destination):
    archive_digest = hashlib.sha256()
    with open(destination, "xb") as output:
        for name, expected in parts:
            digest = hashlib.sha256()
            size = 0
            with open_regular(os.path.join(release, name)) as source:
                while True:
                    block = source.read(1024 * 1024)
                    if not block:
                        break
                    size += len(block)
                    require(size <= CHUNK_LIMIT, "part exceeds 40 MiB: " + name)
                    digest.update(block)
                    archive_digest.update(block)
                    output.write(block)
            require(size > 0, "empty part: " + name)
            require(digest.hexdigest() == expected, "SHA256 mismatch for part: " + name)
    require(archive_digest.hexdigest() == expected_digest, "reassembled archive SHA256 mismatch")


def safe_text(value):
    return bool(value) and "\\" not in value and not any(ord(char) < 32 or ord(char) == 127 for char in value)


def inside_bundle(name):
    return name == BUNDLE or name.startswith(BUNDLE + "/")


def validate_archive(archive):
    members = {}
    for member in archive.getmembers():
        name = member.name
        if member.isdir() and name.endswith("/"):
            name = name[:-1]
        require(safe_text(name) and posixpath.normpath(name) == name and inside_bundle(name),
                "unsafe archive path: " + repr(member.name))
        require(name not in members, "duplicate archive path: " + name)
        require(member.isdir() or member.isreg() or member.issym() or member.islnk(),
                "unsupported archive entry type: " + name)
        require(not member.mode & 0o7000 and member.sparse is None,
                "special permissions or sparse archive entry: " + name)
        members[name] = member
    require(BUNDLE in members and members[BUNDLE].isdir(), "archive must contain the bundle directory")
    installer = members.get(BUNDLE + "/install-offline.sh")
    require(installer is not None and installer.isreg() and installer.mode & 0o100,
            "install-offline.sh must be a real executable file")

    links = {}
    for name, member in members.items():
        parent = posixpath.dirname(name)
        while parent:
            require(parent not in members or members[parent].isdir(),
                    "archive entry has a nondirectory ancestor: " + name)
            parent = posixpath.dirname(parent)
        if member.issym() or member.islnk():
            target = member.linkname
            require(safe_text(target) and not posixpath.isabs(target), "unsafe link target: " + name)
            resolved = posixpath.dirname(name) if member.issym() else ""
            # Inspect each prefix before normalizing '..': a symlink prefix
            # would change where the operating system applies that traversal.
            for component in target.split("/"):
                require(resolved not in members or members[resolved].isdir(),
                        "link target traverses a nondirectory: " + name)
                resolved = posixpath.normpath(posixpath.join(resolved, component))
                require(inside_bundle(resolved), "escaping link: " + name)
            target = resolved
            require(target in members, "dangling link: " + name)
            if member.islnk():
                require(members[target].isreg(), "hardlink must target a regular file: " + name)
            links[name] = target
    for name in links:
        seen = set()
        target = name
        while target in links:
            require(target not in seen, "cyclic archive link: " + name)
            seen.add(target)
            target = links[target]
    return members, links


def extract_archive(archive, members, links, temporary):
    # Do not use extractall: Python 3.6 has no extraction filters. Create regular
    # files first and links last; validation forbids all link ancestors.
    for name, member in members.items():
        destination = os.path.join(temporary, name)
        os.makedirs(os.path.dirname(destination), mode=0o700, exist_ok=True)
        if member.isdir():
            os.makedirs(destination, mode=0o700, exist_ok=True)
        elif member.isreg():
            with archive.extractfile(member) as source, open(destination, "xb") as output:
                shutil.copyfileobj(source, output, 1024 * 1024)
            os.chmod(destination, member.mode)
    for name, target in links.items():
        member = members[name]
        destination = os.path.join(temporary, name)
        if member.issym():
            os.symlink(member.linkname, destination)
        else:
            os.link(os.path.join(temporary, target), destination)
    for name in sorted(members, key=lambda value: value.count("/"), reverse=True):
        if members[name].isdir():
            os.chmod(os.path.join(temporary, name), members[name].mode)


def interrupted(number, frame):
    raise SystemExit(128 + number)


def main():
    require(sys.version_info >= (3, 6), "Python 3.6 or newer is required")
    require(os.geteuid() == 0, "deployment must run as root")
    release = sys.argv[1]
    parts, digest = verify_manifests(release)
    with tempfile.TemporaryDirectory(prefix="gc-analyzer-deploy-") as temporary:
        archive_path = os.path.join(temporary, ARCHIVE)
        reassemble(release, parts, digest, archive_path)
        with tarfile.open(archive_path, "r:gz") as archive:
            members, links = validate_archive(archive)
            extract_archive(archive, members, links, temporary)
        bundle = os.path.join(temporary, BUNDLE)
        print("Verified offline archive; starting installer.", flush=True)
        # The installer enforces the exact RHEL 8.10 / x86_64 target before RPMs.
        status = subprocess.call([os.path.join(bundle, "install-offline.sh")], cwd=bundle)
        return status if status >= 0 else 128 - status


for number in (signal.SIGTERM, signal.SIGHUP):
    signal.signal(number, interrupted)
try:
    sys.exit(main())
except (OSError, ValueError, tarfile.TarError, EOFError) as error:
    print("ERROR [offline deploy]: {}".format(error), file=sys.stderr)
    sys.exit(1)
except KeyboardInterrupt:
    sys.exit(130)
PY
