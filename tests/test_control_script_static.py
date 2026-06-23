import os


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(path):
    with open(os.path.join(ROOT, path)) as fh:
        return fh.read()


def test_manage_app_is_the_only_full_startup_control_script():
    script = _read("manage-app.sh")

    for command in ("start", "deploy", "stop", "restart", "status", "logs", "seed", "open"):
        assert f"{command})" in script

    assert "pick_python()" in script
    assert "ensure_venv()" in script
    assert "seed_demo_history()" in script
    assert "wait_for_health()" in script
    assert "/api/health" in script
    assert "Dashboard URL:" in script


def test_manage_app_avoids_empty_array_expansion_under_nounset():
    script = _read("manage-app.sh")

    assert 'local -a db_arg=()' not in script
    assert '"${db_arg[@]}"' not in script
    assert 'if [ -f "$LOG_FILE" ]; then' in script
    assert "Log file was not created:" in script


def test_legacy_launchers_delegate_to_manage_app():
    run_sh = _read("run.sh")
    start_local = _read("start-local.command")

    assert "exec ./manage-app.sh" in run_sh
    assert "python3 -m gcanalyzer.app" not in run_sh
    assert "exec ./manage-app.sh" in start_local
    assert "python -m gcanalyzer.app" not in start_local


def test_readme_points_to_manage_app_control_commands():
    readme = _read("README.md")

    assert "./manage-app.sh start" in readme
    assert "./manage-app.sh deploy" in readme
    assert "./manage-app.sh status" in readme
    assert "./manage-app.sh stop" in readme
    assert "or just: ./run.sh" not in readme


def test_user_guide_documents_single_control_script():
    guide = _read("architecture_and_user_guide.html")

    assert "Application Control" in guide
    assert "./manage-app.sh deploy 8083" in guide
    assert "./manage-app.sh status 8083" in guide
    assert "./manage-app.sh stop 8083" in guide
    assert "run.sh" not in guide
    assert "start-local.command" not in guide


def test_user_guide_documents_health_grades_and_ml_preview():
    guide = _read("architecture_and_user_guide.html")
    readme = _read("README.md")

    for text in (guide, readme):
        assert "A = healthy" in text
        assert "C = watch" in text
        assert "D = at risk" in text
        assert "ML Tech Preview" in text
        assert "No trained model is running yet" in text
