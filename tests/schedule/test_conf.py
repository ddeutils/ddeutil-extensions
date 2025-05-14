import pytest
from ddeutil.extensions.schedule.scheduler import Schedule
from ddeutil.workflow import FileLoad


def test_loader_find_schedule(schedule_path):
    assert len(list(FileLoad.finds(Schedule))) == 5

    for finding in FileLoad.finds(
        Schedule,
        excluded=[
            "schedule-common-wf",
            "schedule-multi-on-wf",
            "schedule-every-minute-wf",
            "schedule-every-minute-wf-parallel",
        ],
    ):
        assert finding[0] == "schedule-wf"

    for finding in FileLoad.finds(
        Schedule,
        excluded=[
            "schedule-common-wf",
            "schedule-multi-on-wf",
            "schedule-every-minute-wf",
            "schedule-every-minute-wf-parallel",
        ],
        paths=[schedule_path],
    ):
        assert finding[0] == "schedule-wf"
        assert finding[1] == {
            "desc": "Test multi config path",
            "type": "Schedule",
        }

    for finding in FileLoad.finds(
        Schedule,
        excluded=[
            "schedule-common-wf",
            "schedule-multi-on-wf",
            "schedule-every-minute-wf",
            "schedule-every-minute-wf-parallel",
        ],
        extras={"conf_paths": [schedule_path]},
    ):
        assert finding[0] == "schedule-wf"
        assert finding[1] == {
            "desc": "Test multi config path",
            "type": "Schedule",
        }

    with pytest.raises(TypeError):
        list(
            FileLoad.finds(
                Schedule,
                extras={"conf_paths": schedule_path},
            )
        )
