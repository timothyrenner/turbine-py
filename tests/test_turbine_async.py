import pytest

from turbine.asyncio import Turbine


@pytest.fixture()
def topology():
    return Turbine(debug=True)


def test_source_sink_single_task(topology):
    topology.source("input")()

    sinker = []

    @topology.sink("input")
    def sink_array(x):
        sinker.append(x)

    data = ["a", "b", "c"]
    topology.run(data)

    assert data == sinker


def test_source_sink_multi_task(topology):
    topology.source("input")()

    sinker = []

    @topology.sink("input", num_tasks=3)
    def sink_array(x):
        sinker.append(x)

    data = ["a", "b", "c", "d", "e"]
    topology.run(data)

    assert set(data) == set(sinker)


def test_source_sink_source_exception(topology):
    @topology.source("input")
    def fail(x):
        raise ValueError("uh oh")

    sinker = []

    @topology.sink("input", num_tasks=3)
    def sink(x):
        sinker.append(x)

    data = ["a", "b", "c", "d", "e"]
    with pytest.raises(ValueError) as e:
        topology.run(data)

    assert "uh oh" == str(e.value)


def test_source_sink_sink_exception(topology):
    topology.source("input")()

    @topology.sink("input", num_tasks=3)
    def sink(x):
        raise ValueError("failure")

    data = ["a", "b", "c", "d", "e"]
    with pytest.raises(ValueError) as e:
        topology.run(data)

    assert "failure" == str(e.value)
