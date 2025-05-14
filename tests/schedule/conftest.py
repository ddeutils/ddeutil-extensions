from pathlib import Path

import pytest

from .utils import dump_yaml_context


@pytest.fixture(scope="session", autouse=True)
def create_schedule_yaml(conf_path: Path):
    with dump_yaml_context(
        conf_path / "demo/03_schedule.yml",
        data="""
        schedule-wf:
          type: Schedule
          desc: |
            # First Schedule template

            The first schedule config template for testing scheduler function able to
            use it
          workflows:
            - name: 'wf-scheduling'
              on: ['every_3_minute_bkk', 'every_minute_bkk']
              params:
                asat-dt: "${{ release.logical_date }}"

        schedule-common-wf:
          type: Schedule
          workflows:
            - name: 'wf-scheduling'
              on: 'every_3_minute_bkk'
              params:
                asat-dt: "${{ release.logical_date }}"

        schedule-multi-on-wf:
          type: Schedule
          workflows:
            - name: 'wf-scheduling-agent'
              on: ['every_minute_bkk', 'every_3_minute_bkk']
              params:
                name: "Foo"
                asat-dt: "${{ release.logical_date }}"

        schedule-every-minute-wf:
          type: Schedule
          workflows:
            - name: 'wf-scheduling-agent'
              on: 'every_minute_bkk'
              params:
                name: "Foo"
                asat-dt: "${{ release.logical_date }}"

        schedule-every-minute-wf-parallel:
          type: Schedule
          workflows:
            - alias: 'agent-01'
              name: 'wf-scheduling-agent'
              on: 'every_minute_bkk'
              params:
                name: "First"
                asat-dt: "${{ release.logical_date }}"
            - alias: 'agent-02'
              name: 'wf-scheduling-agent'
              on: 'every_minute_bkk'
              params:
                name: "Second"
                asat-dt: "${{ release.logical_date }}"
        """,
    ):
        yield
