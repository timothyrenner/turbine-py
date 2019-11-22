from asyncio import Queue, ensure_future, get_event_loop
from typing import Callable, List, Iterable
from functools import wraps
from itertools import repeat

# TODO Configurable queue sizes.
# TODO Implement union
# TODO Implement gather
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

# TODO Test scatter
# TODO Test union
# TODO Test gather
# TODO Test select
# TODO Test splatter
# TODO Test spread
# TODO Test collect


class Turbine:
    def __init__(self):
        # Map the channel aliases to a channel.
        self._channels = {}
        self._entry_point = None
        self._tasks = []

    def source(self, outbound_name: str) -> Callable:
        # Add the outbound channel to the channel map.
        if outbound_name not in self._channels:
            self._channels[outbound_name] = Queue()

        # Now do the real decorator.
        def decorator(f: Callable) -> Callable:
            @wraps(f)
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
            return wrapper

        return decorator

    def scatter(
        self, inbound_name: str, outbound_names: List[str], num_tasks: int = 1
    ) -> Callable:
        # Add the outbound channels to the channel map.
        for outbound_name in outbound_names:
            if outbound_name not in self._channels:
                self._channels[outbound_name] = Queue()

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

    def gather(self):
        pass

    def select(self):
        pass

    def splatter(self):
        pass

    def spread(self):
        pass

    def collect(self):
        pass

    def sink(self, inbound_name: str) -> Callable:
        def decorator(f: Callable) -> Callable:
            async def task():
                while True:
                    value = await self._channels[inbound_name].get()
                    f(value)
                    self._channels[inbound_name].task_done()

            self._tasks.append(task)

        return decorator

    async def _run_tasks(self, seq: Iterable) -> None:
        running_tasks = [ensure_future(t()) for t in self._tasks]
        for s in seq:
            await self._entry_point(s)
        for _, q in self._channels.items():
            await q.join()
        for t in running_tasks:
            t.cancel()

    def run(self, seq: Iterable) -> None:
        event_loop = get_event_loop()
        event_loop.run_until_complete(self._run_tasks(seq))
        event_loop.stop()
