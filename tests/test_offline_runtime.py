import os
from pathlib import Path
import shlex
import shutil
import subprocess


ROOT = Path(__file__).resolve().parents[1]
BASH = shutil.which("bash")


def test_offline_python_install_invokes_pip_install(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "commands.jsonl"
    stub = bin_dir / "python3.12"
    stub.write_text('#!/usr/bin/env bash\nprintf "%s\\0" "$@" >> "$COMMAND_LOG"\nprintf "\\n" >> "$COMMAND_LOG"\n')
    stub.chmod(0o755)
    python_root = tmp_path / "venv"
    (python_root / "bin").mkdir(parents=True)
    (python_root / "bin/python").symlink_to(stub)
    script = f"""
source {shlex.quote(str(ROOT / 'offline/install-offline.sh'))}
PYTHON_ROOT={shlex.quote(str(python_root))}
APP_ROOT={shlex.quote(str(tmp_path / 'app'))}
BUNDLE_ROOT={shlex.quote(str(tmp_path / 'bundle'))}
install_python_environment
"""
    result = subprocess.run([BASH, "-c", script], capture_output=True, text=True,
                            env={**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "COMMAND_LOG": str(log)})
    assert result.returncode == 0, result.stderr
    commands = [line.rstrip("\0").split("\0") for line in log.read_text().splitlines()]
    install = next(cmd for cmd in commands if "--no-index" in cmd)
    assert install[:3] == ["-m", "pip", "install"]
    assert any(arg.startswith("--find-links=") for arg in install)


def test_installed_prometheus_settings_use_writable_persistent_path():
    source = (ROOT / "offline/install-offline.sh").read_text()
    assert "Environment=GC_PROMETHEUS_CONFIG=/var/lib/gc-analyzer/prometheus.json" in source
    assert 'move_if_destination_absent "$APP_ROOT/prometheus.json" "$STATE_ROOT/prometheus.json"' in source


def test_bundle_has_prometheus_example_but_not_private_config():
    entries = (ROOT / "offline/app-files.txt").read_text().splitlines()
    assert "prometheus.example.json" in entries
    assert "prometheus.json" not in entries


def test_source_gate_includes_api_client_and_javascript_tests():
    script = (ROOT / "offline/build-bundle.sh").read_text()
    assert '"httpx==0.28.1"' in script
    assert 'node --test /source/frontend/dashboard.test.cjs /source/tests/*.cjs' in script
    assert 'GC_ANALYZER_CLEAN_ROOM_IMAGE="$RESOLVED_UBI_IMAGE_ID"' in script
    assert 'image inspect --platform "$CONTAINER_PLATFORM"' in script


def test_fresh_offline_install_uses_random_bootstrap_passwords():
    script = (ROOT / "offline/install-offline.sh").read_text()
    assert 'secrets.token_urlsafe(24)' in script
    assert 'bootstrap-credentials.json' in script
    assert 'os.environ["GC_ADMIN_PASSWORD"]' in script
    assert 'os.environ["GC_READONLY_PASSWORD"]' in script


def test_offline_smoke_checks_current_dashboard_and_isolates_config():
    script = (ROOT / "offline/verify-offline.sh").read_text()
    assert 'GC_CONFIG_DIR="$TEMP_ROOT/config"' in script
    assert 'GC_PROMETHEUS_CONFIG="$TEMP_ROOT/prometheus.json"' in script
    assert '/assets/prometheus-settings.js' in script
    assert 'bootstrap-credentials.json' not in script


def test_rpm_closure_is_verified_without_the_resolver_installed_packages():
    script = (ROOT / "offline/build-bundle.sh").read_text()
    assert '--installroot=/rpm-closure' in script
    assert '--network none' in script
    assert 'rpm --root /rpm-closure --initdb' in script
