from turbine import Turbine

topology = Turbine()


@topology.source("input")
def add_exclamation(input_str):
    return input_str + "!"


@topology.scatter("input", ["output_1", "output_2"], num_tasks=1)
def moar_exclamations(input_str):
    return input_str + "!!"


@topology.sink("output_1")
def print_val(val):
    print(val)


@topology.sink("output_2", num_tasks=2)
def print_val2(val):
    print(val.upper())


values = ["hello", "world"]

topology.run(values)
