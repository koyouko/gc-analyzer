import json
import os
from pathlib import Path
import shlex
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "offline/install-offline.sh"


def run_bash(script, *, env=None):
    return subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, **(env or {})},
    )


def test_main_stops_services_before_migrating_live_state():
    source = INSTALLER.read_text()
    main = source.split("main() {", 1)[1]

    assert main.index("capture_service_state") < main.index("stop_existing_services")
    assert main.index("stop_existing_services") < main.index("migrate_legacy_state")
    assert main.index("apply_permissions") < main.index("install_frontend")
    assert main.rindex("apply_permissions") > main.index("install_frontend")


def test_failed_removal_during_rollback_retains_original_backup(tmp_path):
    app = tmp_path / "app"
    app.mkdir()
    (app / "web").mkdir()
    (app / "web/version").write_text("original")
    result = run_bash(f'''
source {shlex.quote(str(INSTALLER))}
APP_ROOT={shlex.quote(str(app))}
ROLLBACK_PARENT={shlex.quote(str(tmp_path))}
SYSTEMD_UNIT_ROOT={shlex.quote(str(tmp_path / "units"))}
CONTAINER_TEST=1
backup_application_payload
mkdir "$APP_ROOT/web"
printf replacement > "$APP_ROOT/web/version"
rm() {{ return 42; }}
rollback_failed_install || true
''')
    assert result.returncode == 0
    backups = list(tmp_path.glob(".gc-analyzer-rollback.*/web/version"))
    assert len(backups) == 1
    assert backups[0].read_text() == "original"
    assert (app / "web/version").read_text() == "replacement"


def test_fresh_install_captures_inactive_services_without_aborting():
    result = run_bash(f'''
source {shlex.quote(str(INSTALLER))}
systemctl() {{ return 3; }}
capture_service_state
printf '%s:%s' "$BACKEND_WAS_ACTIVE" "$FRONTEND_WAS_ACTIVE"
''')
    assert result.returncode == 0, result.stderr
    assert result.stdout == "0:0"


def test_failed_rollback_retains_backup_for_manual_recovery(tmp_path):
    app = tmp_path / "app"
    app.mkdir()
    (app / "web").mkdir()
    (app / "web/version").write_text("original")
    result = run_bash(f'''
source {shlex.quote(str(INSTALLER))}
APP_ROOT={shlex.quote(str(app))}
ROLLBACK_PARENT={shlex.quote(str(tmp_path))}
SYSTEMD_UNIT_ROOT={shlex.quote(str(tmp_path / "units"))}
CONTAINER_TEST=1
backup_application_payload
mv() {{ return 42; }}
rollback_failed_install || true
''')
    assert result.returncode == 0
    backups = list(tmp_path.glob(".gc-analyzer-rollback.*/web/version"))
    assert len(backups) == 1
    assert backups[0].read_text() == "original"


def test_failed_upgrade_restores_payload_symlinks_and_only_previously_active_services(tmp_path):
    app = tmp_path / "app"
    backup_parent = tmp_path / "backups"
    app.mkdir()
    backup_parent.mkdir()
    units = tmp_path / "units"
    units.mkdir()
    (units / "gc-analyzer-backend.service").write_text("old backend\n")
    (units / "gc-analyzer-frontend.service").symlink_to("old-frontend.service")
    (app / "gcanalyzer").mkdir()
    (app / "gcanalyzer/version").write_text("old\n")
    (app / "web").symlink_to("legacy-web")
    service_log = tmp_path / "services.log"
    script = f"""
source {shlex.quote(str(INSTALLER))}
APP_ROOT={shlex.quote(str(app))}
ROLLBACK_PARENT={shlex.quote(str(backup_parent))}
SYSTEMD_UNIT_ROOT={shlex.quote(str(units))}
BACKEND_WAS_ACTIVE=1
FRONTEND_WAS_ACTIVE=0
systemctl() {{ printf '%s\n' "$*" >> {shlex.quote(str(service_log))}; }}
backup_application_payload
rm -rf "$APP_ROOT/gcanalyzer" "$APP_ROOT/web"
mkdir "$APP_ROOT/gcanalyzer" "$APP_ROOT/web"
printf 'new\n' > "$APP_ROOT/gcanalyzer/version"
printf 'new backend\n' > "$SYSTEMD_UNIT_ROOT/gc-analyzer-backend.service"
rm -f "$SYSTEMD_UNIT_ROOT/gc-analyzer-frontend.service"
printf 'new frontend\n' > "$SYSTEMD_UNIT_ROOT/gc-analyzer-frontend.service"
rollback_failed_install
"""

    result = run_bash(script)

    assert result.returncode == 0, result.stderr
    assert (app / "gcanalyzer/version").read_text() == "old\n"
    assert (app / "web").is_symlink()
    assert os.readlink(app / "web") == "legacy-web"
    assert (units / "gc-analyzer-backend.service").read_text() == "old backend\n"
    assert (units / "gc-analyzer-frontend.service").is_symlink()
    assert os.readlink(units / "gc-analyzer-frontend.service") == "old-frontend.service"
    assert service_log.read_text().splitlines() == [
        "stop gc-analyzer-frontend.service gc-analyzer-backend.service",
        "daemon-reload",
        "start gc-analyzer-backend.service",
    ]
    assert not list(backup_parent.iterdir())


def test_failed_fresh_install_does_not_start_services(tmp_path):
    app = tmp_path / "app"
    app.mkdir()
    units = tmp_path / "units"
    units.mkdir()
    service_log = tmp_path / "services.log"
    script = f"""
source {shlex.quote(str(INSTALLER))}
APP_ROOT={shlex.quote(str(app))}
ROLLBACK_PARENT={shlex.quote(str(tmp_path))}
SYSTEMD_UNIT_ROOT={shlex.quote(str(units))}
BACKEND_WAS_ACTIVE=0
FRONTEND_WAS_ACTIVE=0
systemctl() {{ printf '%s\n' "$*" >> {shlex.quote(str(service_log))}; }}
backup_application_payload
mkdir "$APP_ROOT/web"
rollback_failed_install
"""

    result = run_bash(script)

    assert result.returncode == 0, result.stderr
    assert service_log.read_text().splitlines() == [
        "stop gc-analyzer-frontend.service gc-analyzer-backend.service",
        "daemon-reload",
    ]
    assert not (app / "web").exists()


def test_mid_backup_failure_restores_only_paths_already_moved(tmp_path):
    app = tmp_path / "app"
    app.mkdir()
    (app / "frontend").mkdir()
    (app / "frontend/version").write_text("frontend-old\n")
    (app / "gcanalyzer").mkdir()
    (app / "gcanalyzer/version").write_text("backend-old\n")
    units = tmp_path / "units"
    units.mkdir()
    script = f"""
source {shlex.quote(str(INSTALLER))}
APP_ROOT={shlex.quote(str(app))}
ROLLBACK_PARENT={shlex.quote(str(tmp_path))}
SYSTEMD_UNIT_ROOT={shlex.quote(str(units))}
CONTAINER_TEST=1
move_count=0
mv() {{
    move_count=$((move_count + 1))
    if [[ "$move_count" == 2 ]]; then
        return 73
    fi
    command mv "$@"
}}
trap installation_exit EXIT
backup_application_payload
"""

    result = run_bash(script)

    assert result.returncode == 73
    assert (app / "frontend/version").read_text() == "frontend-old\n"
    assert (app / "gcanalyzer/version").read_text() == "backend-old\n"
    assert not list(tmp_path.glob(".gc-analyzer-rollback.*"))


def test_bootstrap_file_contains_only_generated_passwords(tmp_path):
    config = tmp_path / "config"
    config.mkdir()
    users = config / "users.json"
    python_root = tmp_path / "python"
    (python_root / "bin").mkdir(parents=True)
    (python_root / "bin/python").symlink_to(sys.executable)
    app = shlex.quote(str(ROOT))
    script = f"""
source {shlex.quote(str(INSTALLER))}
APP_ROOT={app}
PYTHON_ROOT={shlex.quote(str(python_root))}
GC_USERS_FILE={shlex.quote(str(users))}
chown() {{ :; }}
initialize_users_file
"""

    result = run_bash(script, env={"GC_ADMIN_PASSWORD": "caller-secret"})

    assert result.returncode == 0, result.stderr
    bootstrap = json.loads((config / "bootstrap-credentials.json").read_text())
    assert set(bootstrap) == {"readonly"}
    assert bootstrap["readonly"]
    assert "caller-secret" not in (config / "bootstrap-credentials.json").read_text()
    assert (config / "bootstrap-credentials.json").stat().st_mode & 0o777 == 0o600


def test_caller_supplied_bootstrap_passwords_are_never_written(tmp_path):
    config = tmp_path / "config"
    config.mkdir()
    users = config / "users.json"
    python_root = tmp_path / "python"
    (python_root / "bin").mkdir(parents=True)
    (python_root / "bin/python").symlink_to(sys.executable)
    script = f"""
source {shlex.quote(str(INSTALLER))}
APP_ROOT={shlex.quote(str(ROOT))}
PYTHON_ROOT={shlex.quote(str(python_root))}
GC_USERS_FILE={shlex.quote(str(users))}
chown() {{ :; }}
initialize_users_file
"""

    result = run_bash(
        script,
        env={
            "GC_ADMIN_PASSWORD": "caller-admin",
            "GC_READONLY_PASSWORD": "caller-readonly",
        },
    )

    assert result.returncode == 0, result.stderr
    assert users.is_file()
    assert not (config / "bootstrap-credentials.json").exists()


def test_manifest_comparison_uses_sha256_without_cmp_preflight():
    source = INSTALLER.read_text()
    preflight = source.split("preflight() {", 1)[1].split("\n}", 1)[0]

    assert " cmp " not in f" {preflight} "
    assert "cmp -s" not in source
    assert "sha256sum" in source


def test_both_install_modes_write_and_validate_actual_systemd_units_before_completion():
    source = INSTALLER.read_text()
    main = source.split("main() {", 1)[1]

    write_index = main.index("write_service_files")
    validate_index = main.index("validate_service_files")
    branch_index = main.index('if [[ "$CONTAINER_TEST" == "1" ]]')
    enable_index = main.index("enable_services")
    assert write_index < validate_index < branch_index < enable_index
    assert "systemd-analyze verify" in source
    assert 'systemctl daemon-reload' in source
    assert main.count("write_service_files") == 1


def test_service_validation_checks_generated_backend_and_frontend_units(tmp_path):
    command_log = tmp_path / "commands.log"
    script = f"""
source {shlex.quote(str(INSTALLER))}
SYSTEMD_UNIT_ROOT={shlex.quote(str(tmp_path))}
systemd-analyze() {{ printf '%s\n' "$*" > {shlex.quote(str(command_log))}; }}
validate_service_files
"""

    result = run_bash(script)

    assert result.returncode == 0, result.stderr
    assert command_log.read_text().split() == [
        "verify",
        str(tmp_path / "gc-analyzer-backend.service"),
        str(tmp_path / "gc-analyzer-frontend.service"),
    ]


def test_mutable_config_is_service_owned_without_broadening_other_config_permissions():
    source = INSTALLER.read_text()
    body = source.split("prepare_persistent_state() {", 1)[1].split("\n}", 1)[0]

    assert 'chown root:"$SERVICE_GROUP" "$CONFIG_ROOT"' in body
    assert 'chmod 0750 "$CONFIG_ROOT"' in body
    assert 'chown "$SERVICE_USER:$SERVICE_GROUP" "$CONFIG_ROOT/config.json"' in body
    assert 'chmod 0640 "$CONFIG_ROOT/config.json"' in body
    assert 'chown root:"$SERVICE_GROUP" "$ENV_FILE" "$SESSION_SECRET_FILE"' in body
    assert 'chmod 0640 "$ENV_FILE" "$SESSION_SECRET_FILE"' in body
