# from unittest import mock
# from ddeutil.workflow.workflow import Workflow
#
#
# @mock.patch("ddeutil.extensions.tasks.tasks_pd.write_deltalake")
# @mock.patch("ddeutil.extensions.tasks.tasks_pd.test_mock")
# def test_pd_tasks_xlsx_to_fabric(mock_test_func, mock_write_deltalake, example_path):
#     mock_test_func.retu.return_value = "prepare from testcase"
#     mock_write_deltalake.return_value = None
#
#     workflow = Workflow.from_conf("wf-test-pandas-tasks")
#     stage = workflow.job("first-job").stage("xlsx-to-fabric-deltalake")
#     rs = stage.execute(
#         params={
#             "params": {
#                 "source": str(example_path / "demo-file.xlsx"),
#                 "sink": "lakehouse/demo",
#                 "token": "FOO",
#             }
#         }
#     )
#     print(rs)
