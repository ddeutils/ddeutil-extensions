from ddeutil.workflow.workflow import Workflow


def test_pd_tasks_xlsx_to_fabric():
    workflow = Workflow.from_loader("wf-test-pandas-tasks")
    stage = workflow.job("first-job").stage("xlsx-to-fabric")
    rs = stage.execute(params={"source": "data"})
    print(rs)
