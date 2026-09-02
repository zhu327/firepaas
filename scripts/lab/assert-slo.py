#!/usr/bin/env python3
"""Fail-closed SLO evaluator for JSONL observations; emits result.json."""

import argparse
import json
import math
import os
import pathlib
import sys
import tempfile

parser = argparse.ArgumentParser()
parser.add_argument("--spec", required=True)
parser.add_argument("--observations", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()
output_path = pathlib.Path(args.output)


def write_result(payload):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=output_path.name + ".", dir=output_path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, indent=2)
            stream.write("\n")
        os.replace(temp_name, output_path)
    except BaseException:
        pathlib.Path(temp_name).unlink(missing_ok=True)
        raise


def fail(message, results=None):
    write_result(
        {
            "schema_version": "1.0",
            "result": "FAIL",
            "error": message,
            "objectives": results or [],
        }
    )
    print("FAIL: " + message, file=sys.stderr)
    raise SystemExit(1)


try:
    with open(args.spec, encoding="utf-8") as stream:
        if args.spec.endswith(".json"):
            spec = json.load(stream)
        else:
            try:
                import yaml
            except ModuleNotFoundError:
                fail("PyYAML is required for YAML SLO spec (install it; do not bypass evaluation)")
            spec = yaml.safe_load(stream)
except SystemExit:
    raise
except Exception as error:
    fail("cannot read SLO spec: " + str(error))

if not isinstance(spec, dict):
    fail("SLO spec must be an object")
objectives = spec.get("objectives")
if not isinstance(objectives, list) or not objectives:
    fail("SLO spec must contain at least one objective")

try:
    with open(args.observations, encoding="utf-8") as stream:
        rows = [json.loads(line) for line in stream if line.strip()]
except Exception as error:
    fail("cannot read observations: " + str(error))
if not rows:
    fail("no observations")
if not all(isinstance(row, dict) for row in rows):
    fail("every observation must be an object")

results = []
for objective in objectives:
    if not isinstance(objective, dict):
        fail("every objective must be an object", results)
    name = objective.get("name")
    metric = objective.get("metric")
    limit = objective.get("max_seconds")
    percentile = objective.get("percentile", 95)
    minimum = objective.get("minimum_samples", 1)
    if not isinstance(name, str) or not name.strip() or not isinstance(metric, str) or not metric.strip():
        fail("objective name and metric must be non-empty strings", results)
    if isinstance(limit, bool) or not isinstance(limit, (int, float)) or not math.isfinite(limit) or limit < 0:
        fail(f"{name}: max_seconds must be a finite non-negative number", results)
    if isinstance(percentile, bool) or not isinstance(percentile, (int, float)) or not math.isfinite(percentile) or not 0 < percentile <= 100:
        fail(f"{name}: percentile must be in (0, 100]", results)
    if isinstance(minimum, bool) or not isinstance(minimum, int) or minimum < 1:
        fail(f"{name}: minimum_samples must be a positive integer", results)

    values = []
    for index, row in enumerate(rows, start=1):
        timestamp = row.get("timestamp")
        if not isinstance(timestamp, str) or not timestamp.strip():
            fail(f"observation {index} missing required timestamp", results)
        if metric not in row:
            fail(f"observation {index} missing required metric {metric}", results)
        value = row[metric]
        if isinstance(value, bool):
            fail(f"observation {index} has non-numeric {metric}", results)
        try:
            value = float(value)
        except (ValueError, TypeError):
            fail(f"observation {index} has non-numeric {metric}", results)
        if not math.isfinite(value) or value < 0:
            fail(f"observation {index} has invalid {metric}", results)
        values.append(value)
    if len(values) < minimum:
        fail(f"{name}: {len(values)} samples, requires {minimum}", results)

    values.sort()
    rank = math.ceil(percentile / 100 * len(values)) - 1
    actual = values[rank]
    results.append(
        {
            "name": name,
            "metric": metric,
            "samples": len(values),
            "percentile": percentile,
            "actual_seconds": actual,
            "max_seconds": limit,
            "pass": actual <= limit,
        }
    )

result = "PASS" if all(item["pass"] for item in results) else "FAIL"
write_result({"schema_version": "1.0", "result": result, "objectives": results})
if result != "PASS":
    print("FAIL: one or more SLO objectives failed", file=sys.stderr)
    raise SystemExit(1)
print("PASS: SLO objectives satisfied")
