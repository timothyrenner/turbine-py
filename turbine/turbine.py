from asyncio import Queue, create_task, run as async_run
from typing import Callable, List, Iterable
from itertools import repeat

# TODO Configurable queue sizes.
# TODO Check for the existence of the inbound channels.
# TODO Figure out how to implement union

# TODO Implement select
# TODO Implement splatter
# TODO Implement spread
# TODO Implement collect

# TODO Docstring source
# TODO Docstring scatter
# TODO Docstring union
# TODO Docstring gather
# TODO Docstring select
# TODO Docstring splatter
# TODO Docstring spread
# TODO Docstring collect
# TODO Docstring sink

# TODO Test select
# TODO Test splatter
# TODO Test spread
# TODO Test collect


class Turbine:
    def __init__(self):
        # Map the channel aliases to a channel.
        self._channels = {}
        self._channel_names = set()
        self._entry_point = None
        self._tasks = []

    def _add_channels(self, channels: List[str]) -> None:
        self._channel_names.update(channels)

    def source(self, outbound_name: str) -> Callable:
        # Add the outbound channel to the channel map.
        self._add_channels([outbound_name])

        # Now do the real decorator.
        def decorator(f: Callable) -> Callable:
            async def wrapper(*args, **kwargs):
                # Call the wrapped function on the input...
                value = f(*args, **kwargs)
                # ... and drop that on the outbound channel.
                await self._channels[outbound_name].put(value)

            # The entry point will get called with Turbine.run. We need this
            # separate from other tasks because it's not an infinite loop.
            # That's really the only difference between this decorator and the
            # others ... no while True.
            self._entry_point = wrapper
            return f

        return decorator

    def scatter(
        self, inbound_name: str, outbound_names: List[str], num_tasks: int = 1
    ) -> Callable:
        # Add any channels to the channel map.
        self._add_channels(outbound_names + [inbound_name])

        def decorator(f: Callable) -> Callable:
            # Create the async task that applies the function.
            async def task():
                while True:
                    input_value = await self._channels[inbound_name].get()
                    # Call the function on the inputs ...
                    output = f(input_value)
                    # ... and copy the outputs to each of the outbound
                    # channels.
                    for output, channel in zip(repeat(output), outbound_names):
                        await self._channels[channel].put(output)
                    self._channels[inbound_name].task_done()

            # Create all of the tasks.
            for _ in range(num_tasks):
                self._tasks.append(task)
            return f

        return decorator

    def union(self):
        pass

    def gather(
        self, inbound_names: List[str], outbound_name: str, num_tasks: int = 1
    ) -> Callable:
        self._add_channels(inbound_names + [outbound_name])

        def decorator(f: Callable) -> Callable:
            # Create the async task that applies the function.
            async def task():
                while True:
                    values = [
                        await self._channels[c].get() for c in inbound_names
                    ]
                    output = f(*values)
                    await self._channels[outbound_name].put(output)
                    for c in inbound_names:
                        self._channels[c].task_done()

            # Create the tasks.
            for _ in range(num_tasks):
                self._tasks.append(task)
            return f

        return decorator

    def select(self):
        pass

    def splatter(self):
        pass

    def spread(self):
        pass

    def task(self):
        # This is a one-in one-out route.
        # It's for parallelizing workloads.
        pass

    def collect(self):
        pass

    def sink(self, inbound_name: str, num_tasks: int = 1) -> Callable:
        def decorator(f: Callable) -> Callable:
            async def task():
                while True:
                    value = await self._channels[inbound_name].get()
                    f(value)
                    self._channels[inbound_name].task_done()

            # Create the tasks for the sinks.
            for _ in range(num_tasks):
                self._tasks.append(task)
            return f

        return decorator

    async def _run_tasks(self, seq: Iterable) -> None:
        # Create the queues inside the event loop attached to `run`.
        self._channels = {c: Queue() for c in self._channel_names}
        # Load the tasks into the loop.
        running_tasks = [create_task(t()) for t in self._tasks]
        # Load the entry point queue.
        for s in seq:
            await self._entry_point(s)
        # ... wait until work is completed.
        for _, q in self._channels.items():
            await q.join()
        # Now shut the tasks down.
        for t in running_tasks:
            t.cancel()

    def run(self, seq: Iterable) -> None:
        async_run(self._run_tasks(seq))
