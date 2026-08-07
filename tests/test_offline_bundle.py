from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def manifest_lines(relative_path):
    return (ROOT / relative_path).read_text().splitlines()


def test_offline_requirements_are_exact_and_include_ml():
    lines = [
        line.strip()
        for line in (ROOT / "requirements-offline.txt").read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    assert lines == [
        "fastapi==0.141.1",
        "uvicorn==0.52.1",
        "paramiko==5.0.0",
        "PyYAML==6.0.3",
        "scikit-learn==1.9.0",
    ]


def test_rhel8_package_roots_are_exact_and_ordered():
    assert manifest_lines("offline/rhel8-packages.txt") == [
        "python3.12",
        "python3.12-pip",
        "ca-certificates",
        "curl",
        "tar",
        "gzip",
        "shadow-utils",
    ]


def test_application_release_allowlist_is_exact_and_ordered():
    allowlist = manifest_lines("offline/app-files.txt")

    assert allowlist == [
        "frontend",
        "gcanalyzer",
        "seed",
        "web/app",
        "web/components",
        "web/lib",
        "web/next.config.js",
        "web/package.json",
        "web/package-lock.json",
        "web/tsconfig.json",
        "requirements.txt",
        "requirements-offline.txt",
        "manage-app.sh",
        "README.md",
        "architecture_and_user_guide.html",
    ]

    excluded_paths = {
        ".env",
        ".venv",
        "users.json",
        ".session_secret",
        "node_modules",
        ".next",
    }
    assert excluded_paths.isdisjoint(allowlist)
    assert not any(path.endswith(".db") for path in allowlist)
