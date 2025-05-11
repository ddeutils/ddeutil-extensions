from ddeutil.workflow import Stage, Workflow


def test_pl_tasks_csv(example_path):
    workflow = Workflow.from_conf("wf-test-polars-tasks")
    stage: Stage = workflow.job("csv-job").stage("count-csv")
    rs = stage.set_outputs(
        stage.handler_execute(
            params={
                "params": {
                    "source": str(example_path / "demo-file-customers-100.csv")
                },
            }
        ).context,
        to={},
    )
    assert rs == {"stages": {"count-csv": {"outputs": {"records": 100}}}}
