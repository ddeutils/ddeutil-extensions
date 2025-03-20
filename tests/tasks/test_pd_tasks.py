from ddeutil.workflow.workflow import Workflow


def test_pd_tasks_xlsx_to_fabric():
    workflow = Workflow.from_loader("wf-test-pandas-tasks")
    stage = workflow.job("first-job").stage("xlsx-to-fabric-deltalake")
    rs = stage.execute(
        params={
            "source": "tests/data/examples/demo-file.xlsx",
            "target": "lakehouse/demo",
            "token": "FOO",
        }
    )
    print(rs)
