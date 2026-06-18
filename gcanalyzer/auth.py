"""
Simple file-based auth for the dashboard.

Two roles: 'admin' (add/remove/modify clusters) and 'readonly' (view only).
Users live in users.json with PBKDF2-hashed passwords (no plaintext on disk).
Sessions are stateless: a cookie holding {user, role, exp} signed with an HMAC
secret, so there is no server-side session store.

Bootstrap: if users.json is missing, a default admin/readonly pair is created
(passwords from GC_ADMIN_PASSWORD / GC_READONLY_PASSWORD if set, else 'admin' /
'readonly' with a loud warning). Manage users with `python -m gcanalyzer.users`.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
USERS_PATH = os.environ.get("GC_USERS_FILE", os.path.join(ROOT, "users.json"))
SECRET_PATH = os.path.join(ROOT, ".session_secret")
SESSION_TTL_S = int(os.environ.get("GC_SESSION_TTL", str(7 * 24 * 3600)))
ROLES = ("admin", "readonly")
_PBKDF2_ROUNDS = 200_000


# --------------------------------------------------------------------------- #
# Password hashing (PBKDF2-HMAC-SHA256, stdlib only)
# --------------------------------------------------------------------------- #
def hash_password(password: str, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), _PBKDF2_ROUNDS)
    return "pbkdf2${}${}${}".format(_PBKDF2_ROUNDS, salt, dk.hex())


def verify_password(password: str, stored: str) -> bool:
    try:
        _scheme, rounds, salt, hexdk = stored.split("$")
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), int(rounds))
    except (ValueError, AttributeError):
        return False
    return hmac.compare_digest(dk.hex(), hexdk)


# --------------------------------------------------------------------------- #
# Users file
# --------------------------------------------------------------------------- #
def _seed_users() -> list[dict]:
    admin_pw = os.environ.get("GC_ADMIN_PASSWORD", "admin")
    ro_pw = os.environ.get("GC_READONLY_PASSWORD", "readonly")
    users = [
        {"user": "admin", "role": "admin", "pw_hash": hash_password(admin_pw)},
        {"user": "readonly", "role": "readonly", "pw_hash": hash_password(ro_pw)},
    ]
    save_users(users)
    if "GC_ADMIN_PASSWORD" not in os.environ:
        print("WARNING: created users.json with DEFAULT password 'admin' for user "
              "'admin'. Change it now: python -m gcanalyzer.users passwd admin")
    return users


def load_users() -> list[dict]:
    if not os.path.exists(USERS_PATH):
        return _seed_users()
    with open(USERS_PATH) as fh:
        return json.load(fh)


def save_users(users: list[dict]) -> None:
    with open(USERS_PATH, "w") as fh:
        json.dump(users, fh, indent=2)
    try:
        os.chmod(USERS_PATH, 0o600)
    except OSError:
        pass


def find_user(name: str) -> dict | None:
    return next((u for u in load_users() if u["user"] == name), None)


def authenticate(name: str, password: str) -> dict | None:
    u = find_user(name)
    if u and verify_password(password, u["pw_hash"]):
        return u
    return None


# --------------------------------------------------------------------------- #
# Stateless signed-cookie sessions
# --------------------------------------------------------------------------- #
def _secret() -> bytes:
    env = os.environ.get("GC_SESSION_SECRET")
    if env:
        return env.encode()
    if os.path.exists(SECRET_PATH):
        with open(SECRET_PATH) as fh:
            return fh.read().strip().encode()
    s = secrets.token_hex(32)
    with open(SECRET_PATH, "w") as fh:
        fh.write(s)
    try:
        os.chmod(SECRET_PATH, 0o600)
    except OSError:
        pass
    return s.encode()


def make_session(user: str, role: str, ttl: int = SESSION_TTL_S) -> str:
    payload = {"u": user, "r": role, "exp": int(time.time()) + ttl}
    raw = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    sig = hmac.new(_secret(), raw.encode(), hashlib.sha256).hexdigest()
    return raw + "." + sig


def read_session(token: str | None) -> dict | None:
    if not token or "." not in token:
        return None
    raw, sig = token.rsplit(".", 1)
    expected = hmac.new(_secret(), raw.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        payload = json.loads(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)))
    except (ValueError, json.JSONDecodeError):
        return None
    if int(payload.get("exp", 0)) < int(time.time()):
        return None
    return {"user": payload["u"], "role": payload["r"]}
