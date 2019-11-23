import pytest
from turbine import Turbine


def identity(x):
    return x


@pytest.fixture
def topology():
    return Turbine()


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
