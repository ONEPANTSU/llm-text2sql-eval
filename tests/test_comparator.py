from evalsuite.compare.comparator import compare_results


def test_order_by_respected():
    gold = [[1], [2]]
    pred = [[2], [1]]
    res = compare_results(gold, pred, order_by=True, float_tol=1e-4)
    assert not res.match


def test_order_by_ignored_when_absent():
    gold = [[1], [2]]
    pred = [[2], [1]]
    res = compare_results(gold, pred, order_by=False, float_tol=1e-4)
    assert res.match


def test_float_tolerance():
    gold = [[1.0]]
    pred = [[1.00005]]
    res = compare_results(gold, pred, order_by=False, float_tol=1e-3)
    assert res.match


def test_column_order_insensitive():
    gold = [[1, "a"]]
    pred = [["a", 1]]
    res = compare_results(gold, pred, order_by=False, float_tol=1e-4, column_order_insensitive=True)
    assert res.match


def test_null_normalization():
    gold = [[None]]
    pred = [[None]]
    res = compare_results(gold, pred, order_by=False, float_tol=1e-4)
    assert res.match
