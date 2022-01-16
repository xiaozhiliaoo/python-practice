def inc(x):
    return x+1

def test_inc_success():
    assert inc(3) == 4

def test_inc_fail():
    assert inc(4) == 4
