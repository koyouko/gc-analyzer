from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


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
