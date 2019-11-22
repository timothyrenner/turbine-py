from turbine import Turbine


def test_source_sink():
    topology = Turbine()

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
