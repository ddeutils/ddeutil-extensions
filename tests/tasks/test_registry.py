from ddeutil.workflow.reusables import make_registry


def test_make_registry():
    rgt = make_registry("tasks")
    print(rgt)
