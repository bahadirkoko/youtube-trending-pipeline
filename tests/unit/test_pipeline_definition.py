"""Structural checks on the Step Functions definition (no AWS needed).

The template is rendered the way Terraform's templatefile() would, then walked:
every transition must lead somewhere, every state must be reachable, long-running
tasks must have timeouts, and every path into NotifyFailure must set $.error.
"""

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = ROOT / "orchestration" / "pipeline.asl.json.tpl"
MODULE_MAIN = ROOT / "infra" / "modules" / "orchestration" / "main.tf"

FAKE = {
    "ingestion_function_name": "yt-ingest",
    "json_to_parquet_function_name": "yt-json",
    "data_quality_function_name": "yt-dq",
    "bronze_to_silver_job_name": "yt-b2s",
    "silver_to_gold_job_name": "yt-s2g",
    "alerts_topic_arn": "arn:aws:sns:us-east-1:123456789012:alerts",
    "environment": "dev",
    "region": "us-east-1",
}


def render(values=None):
    values = values or FAKE
    return re.sub(r"\$\{(\w+)\}", lambda m: values[m.group(1)], TEMPLATE.read_text())


@pytest.fixture(scope="module")
def definition():
    return json.loads(render())


def walk(states, prefix=""):
    """Yield (path, name, state, siblings) for every state, including Parallel branches."""
    for name, state in states.items():
        yield prefix + name, name, state, states
        for branch in state.get("Branches", []):
            yield from walk(branch["States"], prefix + name + "/")


def targets(state):
    found = []
    if "Next" in state:
        found.append(state["Next"])
    if "Default" in state:
        found.append(state["Default"])
    found += [c["Next"] for c in state.get("Choices", [])]
    found += [c["Next"] for c in state.get("Catch", [])]
    return found


def test_template_variables_match_what_terraform_passes():
    used = set(re.findall(r"\$\{(\w+)\}", TEMPLATE.read_text()))
    block = MODULE_MAIN.read_text().split("templatefile(")[1].split("})")[0]
    passed = set(re.findall(r"^\s*(\w+)\s*=", block, re.M))
    assert used == passed
    assert used == set(FAKE)


def test_every_transition_points_at_a_real_state(definition):
    assert definition["StartAt"] in definition["States"]
    for path, _, state, siblings in walk(definition["States"]):
        for target in targets(state):
            assert target in siblings, f"{path} -> {target} does not exist"
        if "Branches" in state:
            for branch in state["Branches"]:
                assert branch["StartAt"] in branch["States"]


def test_every_state_is_reachable(definition):
    def reachable(states, start):
        seen, queue = set(), [start]
        while queue:
            name = queue.pop()
            if name in seen:
                continue
            seen.add(name)
            queue.extend(targets(states[name]))
        return seen

    top = reachable(definition["States"], definition["StartAt"])
    assert top == set(definition["States"]), set(definition["States"]) - top
    for path, _, state, _ in walk(definition["States"]):
        for branch in state.get("Branches", []):
            inner = reachable(branch["States"], branch["StartAt"])
            assert inner == set(branch["States"]), path


def test_every_path_ends(definition):
    for path, _, state, _ in walk(definition["States"]):
        if state["Type"] in ("Succeed", "Fail", "Choice"):
            continue
        assert "End" in state or "Next" in state, path


def test_tasks_have_timeouts_and_the_workflow_has_one(definition):
    assert definition["TimeoutSeconds"] > 0
    for path, _, state, _ in walk(definition["States"]):
        if state["Type"] == "Task" and "sns:publish" not in state["Resource"]:
            assert state.get("TimeoutSeconds", 0) > 0, f"{path} has no timeout"


def test_glue_jobs_are_waited_for(definition):
    for path, _, state, _ in walk(definition["States"]):
        if ":glue:" in state.get("Resource", ""):
            assert state["Resource"].endswith(".sync"), f"{path} would not wait for the job"


def test_failures_in_main_steps_route_to_notify_failure(definition):
    states = definition["States"]
    for name in ("Ingest", "BuildSilver", "RunDataQuality", "BuildGold"):
        catches = states[name]["Catch"]
        assert catches[0]["ErrorEquals"] == ["States.ALL"]
        assert catches[0]["Next"] == "NotifyFailure"
        assert catches[0]["ResultPath"] == "$.error"


def test_everything_entering_notify_failure_sets_the_error(definition):
    states = definition["States"]
    for name, state in states.items():
        if name == "NotifyFailure":
            continue
        for catch in state.get("Catch", []):
            if catch["Next"] == "NotifyFailure":
                assert catch["ResultPath"] == "$.error", name
        if state.get("Next") == "NotifyFailure":
            assert state["Type"] == "Pass" and state["ResultPath"] == "$.error", name


def test_data_quality_failure_stops_before_gold(definition):
    gate = definition["States"]["DataQualityGate"]
    assert gate["Choices"][0] == {
        "Variable": "$.dq.passed",
        "BooleanEquals": True,
        "Next": "BuildGold",
    }
    assert definition["States"][gate["Default"]]["Type"] == "Fail"


def test_gold_only_runs_after_the_gate(definition):
    incoming = [
        name for name, state in definition["States"].items() if "BuildGold" in targets(state)
    ]
    assert incoming == ["DataQualityGate"]


def test_silver_branches_run_in_parallel(definition):
    parallel = definition["States"]["BuildSilver"]
    starts = {branch["StartAt"] for branch in parallel["Branches"]}
    assert starts == {"ConvertCategories", "BronzeToSilver"}


def test_notification_messages_have_matching_placeholders(definition):
    pattern = re.compile(r"^States\.Format\('((?:[^']|'')*)', (.*)\)$", re.S)
    checked = 0
    for _, name, state, _ in walk(definition["States"]):
        for key, value in state.get("Parameters", {}).items():
            if key.endswith(".$") and value.startswith("States.Format("):
                match = pattern.match(value)
                assert match, name
                template, args = match.groups()
                # count top-level arguments (commas outside parentheses)
                depth, count = 0, 1
                for ch in args:
                    depth += ch == "("
                    depth -= ch == ")"
                    count += ch == "," and depth == 0
                assert template.count("{}") == count, f"{name}: {key}"
                checked += 1
    assert checked == 3


def test_sns_subjects_fit_the_100_character_limit(definition):
    for _, name, state, _ in walk(definition["States"]):
        subject = state.get("Parameters", {}).get("Subject")
        if subject:
            assert len(subject) <= 100, name
