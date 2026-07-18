from rich.console import Console

from batchwizard.models import CollectionState, JobRecord
from batchwizard.processor import JobEvent
from batchwizard.ui import Dashboard


def test_dashboard_renders_local_attention_separately_from_remote_state():
    dashboard = Dashboard(Console(width=120, record=True))
    job = JobRecord(batch_id="batch_1", input_path="/tmp/input.jsonl")
    job.collection_state = CollectionState.FAILED
    job.last_local_error = "Artifact collection failed: disk full"

    dashboard.on_event(
        JobEvent(kind="attention", job=job, message=job.last_local_error)
    )
    rendered = dashboard._render()

    assert rendered is not None
    assert "needs attention" in "\n".join(dashboard.logs)
    assert dashboard.jobs[job.batch_id].is_actionable
