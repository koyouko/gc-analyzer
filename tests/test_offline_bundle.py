from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_offline_requirements_are_exact_and_include_ml():
    lines = [
        line.strip()
        for line in (ROOT / "requirements-offline.txt").read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    assert "fastapi==0.141.1" in lines
    assert "uvicorn==0.52.1" in lines
    assert "paramiko==5.0.0" in lines
    assert "PyYAML==6.0.3" in lines
    assert "scikit-learn==1.9.0" in lines
    assert all("==" in line for line in lines)
