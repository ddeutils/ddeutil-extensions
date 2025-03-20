from ddeutil.workflow.caller import make_registry


def test_make_registry():
    rgt = make_registry("tasks")
    print(rgt)
