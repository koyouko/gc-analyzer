"""
Manage dashboard users.

    python -m gcanalyzer.users list
    python -m gcanalyzer.users add <name> <admin|readonly>
    python -m gcanalyzer.users passwd <name>
    python -m gcanalyzer.users remove <name>

Passwords are read interactively (getpass) and stored PBKDF2-hashed.
"""

from __future__ import annotations

import argparse
import getpass
import sys

from . import auth


def main() -> None:
    ap = argparse.ArgumentParser(description="Manage GC Analyzer dashboard users")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    p_add = sub.add_parser("add")
    p_add.add_argument("user")
    p_add.add_argument("role", choices=auth.ROLES)
    sub.add_parser("passwd").add_argument("user")
    sub.add_parser("remove").add_argument("user")
    args = ap.parse_args()

    users = auth.load_users()

    if args.cmd == "list":
        for u in users:
            print(f"{u['user']:<18} {u['role']}")
        return

    if args.cmd == "add":
        if any(u["user"] == args.user for u in users):
            sys.exit(f"user '{args.user}' already exists")
        pw = getpass.getpass("password: ")
        if pw != getpass.getpass("confirm:  "):
            sys.exit("passwords do not match")
        users.append({"user": args.user, "role": args.role, "pw_hash": auth.hash_password(pw)})
        auth.save_users(users)
        print(f"added {args.user} ({args.role})")
        return

    if args.cmd == "passwd":
        u = next((u for u in users if u["user"] == args.user), None)
        if not u:
            sys.exit(f"no such user: {args.user}")
        pw = getpass.getpass("new password: ")
        if pw != getpass.getpass("confirm:     "):
            sys.exit("passwords do not match")
        u["pw_hash"] = auth.hash_password(pw)
        auth.save_users(users)
        print(f"updated password for {args.user}")
        return

    if args.cmd == "remove":
        if not any(u["user"] == args.user for u in users):
            sys.exit(f"no such user: {args.user}")
        auth.save_users([u for u in users if u["user"] != args.user])
        print(f"removed {args.user}")


if __name__ == "__main__":
    main()
