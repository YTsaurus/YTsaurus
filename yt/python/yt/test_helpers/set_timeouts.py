import pytest


def get_timeout_mark(item):
    for mark in item.iter_markers():
        if mark.name == "timeout":
            return mark
    return None


def pytest_collection_modifyitems(items, config):
    for item in items:
        if get_timeout_mark(item) is None:
            item.add_marker(pytest.mark.timeout(180))
        # By default plugin limits execution time not only of test function body but
        # including fixtures (setup_method/teardown_method/setup_class/teardown_class).
        # No problem if it is per test method fixtures (setup_method and teardown_method).
        # The problem arises when initialization of first test method in class (test suite)
        # includes initialization of whole class (setup_class).
        # Class initialization can take much more time than one test method.
        # With func_only option pytest.mark.timeout limits execution time of test function body.
        # There is a bug in pytest-timeout which prevents to set func_only globally.
        # See https://github.com/pytest-dev/pytest-timeout/commit/d68013ad89027aa31d8f35cdb2414519d2db82c1
        mark = get_timeout_mark(item)
        mark.kwargs["func_only"] = True
