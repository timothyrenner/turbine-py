import pytest
from turbine import Turbine


def identity(x):
    return x


@pytest.fixture
def topology():
    return Turbine(debug=True)


def test_source_sink(topology):
    @topology.source("input")
    def identity(x):
        return x

    sinker = []

    @topology.sink("input")
    def sink(x):
        sinker.append(x)

    data = ["a", "b", "c"]
    topology.run(data)

    assert sinker == data


def test_source_sink_multitask(topology):
    @topology.source("input")
    def identity(x):
        return x

    sinker = []

    @topology.sink("input", num_tasks=2)
    def sink(x):
        sinker.append(x)

    data = ["a", "b", "c"]
    topology.run(data)

    assert sinker == data


def test_source_exception(topology):
    @topology.source("input")
    def oops(x):
        raise ValueError("my bad")

    @topology.sink("input")
    def nope(x):
        print(x)

    data = ["it", "doesn't", "matter"]
    with pytest.raises(ValueError) as e:
        topology.run(data)
        assert str(e) == "my bad"


def test_scatter(topology):
    @topology.source("input")
    def identity(x):
        return x

    sinker1 = []
    sinker2 = []

    @topology.scatter("input", ["output_1", "output_2"])
    def scatter(x):
        return x + "!"

    @topology.sink("output_1")
    def sink1(x):
        sinker1.append(x)

    @topology.sink("output_2")
    def sink2(x):
        sinker2.append(x)

    data = ["a", "b", "c"]
    topology.run(data)

    truth = ["a!", "b!", "c!"]

    assert truth == sinker1
    assert truth == sinker2


def test_scatter_multitask(topology):
    @topology.source("input")
    def identity(x):
        return x

    sinker1 = []
    sinker2 = []

    @topology.scatter("input", ["output_1", "output_2"], num_tasks=2)
    def scatter(x):
        return x + "!"

    @topology.sink("output_1", num_tasks=1)
    def sink1(x):
        sinker1.append(x)

    @topology.sink("output_2")
    def sink2(x):
        sinker2.append(x)

    data = ["a", "b", "c"]
    topology.run(data)

    truth = ["a!", "b!", "c!"]

    assert truth == sinker1
    assert truth == sinker2


def test_scatter_exception(topology):
    @topology.source("input")
    def identity(x):
        return x

    @topology.scatter("input", ["output_1", "output_2"])
    def scatter(x):
        raise ValueError("my bad")

    @topology.sink("output_1")
    def sink_1(x):
        print(x)

    @topology.sink("output_2")
    def sink_2(x):
        print(x)

    data = ["it", "doesn't", "matter"]
    with pytest.raises(ValueError) as e:
        topology.run(data)
        assert str(e) == "my bad"


def test_gather(topology):
    topology.source("input")(identity)

    @topology.scatter("input", ["scatter1", "scatter2"])
    def scatter(x):
        return x + "!"

    @topology.gather(["scatter1", "scatter2"], "output")
    def gather(x, y):
        return " ".join([x, y])

    sinker = []

    @topology.sink("output")
    def sink(x):
        sinker.append(x)

    data = ["a", "b", "c"]
    topology.run(data)

    truth = ["a! a!", "b! b!", "c! c!"]
    assert truth == sinker


# @pytest.mark.skip
def test_gather_exception(topology):
    topology.source("input")(identity)

    topology.scatter("input", ["sc1", "sc2"])(identity)

    @topology.gather(["sc1", "sc2"], "output")
    def fail(x, t):
        raise ValueError("Oops")

    topology.sink("sc1")(print)
    topology.sink("sc2")(print)

    data = ["I'm", "going", "to", "fail"]
    with pytest.raises(ValueError) as e:
        topology.run(data)
        print(e)
        assert str(e) == "Oops"


def test_select(topology):
    topology.source("input")(identity)

    @topology.select("input", {0: "evens", 1: "odds"}, lambda x: x % 2)
    def selector(x):
        return x + 1

    even_sinker = []

    @topology.sink("evens")
    def sink_evens(x):
        even_sinker.append(x)

    odd_sinker = []

    @topology.sink("odds")
    def sink_odds(x):
        odd_sinker.append(x)

    data = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    truth_evens = [2, 4, 6, 8, 10]
    truth_odds = [1, 3, 5, 7, 9]

    topology.run(data)
    assert truth_evens == even_sinker
    assert truth_odds == odd_sinker


def test_select_default(topology):
    topology.source("input")(identity)

    topology.select(
        "input",
        {"a": "as", "b": "bs"},
        lambda x: x[0],
        default_outbound_channel="everything_else",
    )(identity)

    a_sinker = []

    @topology.sink("as")
    # a_sink ... get it?
    def a_sink(a):
        a_sinker.append(a)

    b_sinker = []

    @topology.sink("bs")
    def b_sink(b):
        b_sinker.append(b)

    everything_else_sinker = []

    @topology.sink("everything_else")
    def everything_else_sink(everything_else):
        everything_else_sinker.append(everything_else)

    data = ["aaa", "bbb", "ccc", "ddd"]
    a_sinker_truth = ["aaa"]
    b_sinker_truth = ["bbb"]
    everything_else_sinker_truth = ["ccc", "ddd"]

    topology.run(data)

    assert a_sinker_truth == a_sinker
    assert b_sinker_truth == b_sinker
    assert everything_else_sinker_truth == everything_else_sinker


def test_select_no_default(topology):
    topology.source("input")(identity)

    topology.select("input", {"a": "as", "b": "bs"}, lambda x: x[0],)(identity)

    a_sinker = []

    @topology.sink("as")
    # a_sink ... get it?
    def a_sink(a):
        a_sinker.append(a)

    b_sinker = []

    @topology.sink("bs", num_tasks=1)
    def b_sink(b):
        b_sinker.append(b)

    data = ["aaa", "bbb", "ccc", "ddd"]
    with pytest.raises(ValueError):
        topology.run(data)
