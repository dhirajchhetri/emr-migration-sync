import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFINITION = ROOT / "infra" / "statemachine.asl.json"


def test_state_machine_covers_normalize_load_and_reconcile():
    document = json.loads(DEFINITION.read_text())
    states = document["States"]
    assert document["StartAt"] == "NormalizeExtract"
    assert states["NormalizeExtract"]["Resource"].endswith("glue:startJobRun.sync")
    assert states["NormalizeExtract"]["Parameters"]["JobName"] == "emr-sync-normalize-facility"
    assert states["NormalizeExtract"]["ResultPath"] == "$.glue"
    assert states["LoadCurated"]["Next"] == "Reconcile"
    assert states["Reconcile"]["Next"] == "Gate"
    assert states["Gate"]["Choices"][0]["Next"] == "PublishReport"
    assert states["Gate"]["Default"] == "MarkFailed"
    assert states["PublishReport"]["Type"] == "Succeed"
    assert "${LoadFunctionArn}" in states["LoadCurated"]["Resource"]
    assert "${ReconcileFunctionArn}" in states["Reconcile"]["Resource"]
