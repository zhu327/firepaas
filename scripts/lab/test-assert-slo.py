#!/usr/bin/env python3
import json
import pathlib
import subprocess
import tempfile

HERE = pathlib.Path(__file__).resolve().parent


def run(spec, rows):
    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp)
        (root / "spec.json").write_text(json.dumps(spec))
        (root / "observations.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows)
        )
        result = subprocess.run(
            [
                "python3",
                str(HERE / "assert-slo.py"),
                "--spec",
                str(root / "spec.json"),
                "--observations",
                str(root / "observations.jsonl"),
                "--output",
                str(root / "result.json"),
            ],
            capture_output=True,
            text=True,
        )
        return result, json.loads((root / "result.json").read_text())


spec = {
    "objectives": [
        {
            "name": "periodic-failover",
            "metric": "failover_seconds",
            "max_seconds": 60,
            "minimum_samples": 2,
        },
        {
            "name": "continuous-availability",
            "metric": "availability_seconds",
            "max_seconds": 15,
            "minimum_samples": 3,
        },
    ]
}
rows = [
    {"timestamp": "2026-01-01T00:00:00Z", "availability_seconds": 1},
    {
        "timestamp": "2026-01-01T00:01:00Z",
        "availability_seconds": 1,
        "failover_seconds": 20,
    },
    {
        "timestamp": "2026-01-01T00:02:00Z",
        "availability_seconds": 1,
        "failover_seconds": 25,
    },
]
result, output = run(spec, rows)
assert result.returncode == 0, result.stderr
assert output["result"] == "PASS"

result, output = run(spec, rows[:2])
assert result.returncode != 0
assert output["result"] == "FAIL"
assert "1 samples, requires 2" in output["error"]
