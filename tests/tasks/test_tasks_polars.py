from ddeutil.workflow import Result, Stage, Workflow


def test_pl_tasks_excel(example_path):
    workflow = Workflow.from_conf("wf-test-polars-tasks")
    stage: Stage = workflow.job("excel-job").stage("count-excel")
    rs: Result = stage.handler_execute(
        params={
            "params": {"source": str(example_path / "demo-file.xlsx")},
        }
    )
    assert rs.context == {"records": 100}

    stage: Stage = workflow.job("excel-job").stage("count-excel")
    rs: Result = stage.handler_execute(
        params={
            "params": {"source": str(example_path / "demo-file-empty.xlsx")},
        },
    )
    assert rs.context == {"records": 0}


def test_pl_tasks_csv(example_path):
    workflow = Workflow.from_conf("wf-test-polars-tasks")
    stage: Stage = workflow.job("csv-job").stage("count-csv")
    rs: Result = stage.handler_execute(
        params={
            "params": {
                "source": str(example_path / "demo-file-customers-100.csv")
            },
        }
    )
    assert rs.context == {"records": 100}

    stage: Stage = workflow.job("csv-job").stage("count-condition-csv")
    rs: Result = stage.handler_execute(
        params={
            "params": {
                "source": str(example_path / "demo-file-customers-100.csv")
            },
        }
    )
    assert rs.context == {"records": 1}


def test_pl_tasks_csv_to_parquet(example_path, target_path):
    workflow = Workflow.from_conf("wf-test-polars-tasks")
    stage: Stage = workflow.job("csv-job").stage("convert-csv-to-parquet")
    rs: Result = stage.handler_execute(
        params={
            "params": {
                "source": str(example_path / "demo-file-customers-100.csv"),
                "target": str(target_path / "demo-file-customers-100"),
            },
        },
    )
    print(rs.context)


def test_pl_tasks_parquet(example_path): ...


def test_pl_tasks_excel_to_parquet(example_path, target_path):
    workflow = Workflow.from_conf("wf-test-polars-tasks")
    stage: Stage = workflow.job("excel-job").stage("convert-excel-to-parquet")
    rs: Result = stage.handler_execute(
        params={
            "params": {
                "source": str(example_path / "demo-file.xlsx"),
                "sink": str(target_path / "demo-file-customers-100"),
            },
        },
    )
    print(rs.context)
