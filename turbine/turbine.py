import asyncio
from asyncio import Queue, create_task, run as async_run, get_event_loop
from typing import Callable, List, Iterable, Dict, Any, TypeVar
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
T = TypeVar("T")


class Turbine:
    def __init__(self):
        # Map the channel aliases to a channel.
        self._channels = {}
        self._channel_names = set()
        self._entry_point = None
        self._tasks = []
        self._running_tasks = []

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
        pass  # ! This is a tough one. In Clojure I used alts!!.

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

    def select(
        self,
        inbound_channel: str,
        outbound_channels: Dict[T, str],
        selector_fn: Callable[[Any], T],
        default_outbound_channel: str = None,
        num_tasks: int = 1,
    ) -> Callable:
        def decorator(f: Callable) -> Callable:
            self._add_channels(
                list(outbound_channels.values()) + [inbound_channel]
            )
            if default_outbound_channel:
                self._add_channels([default_outbound_channel])

            # Create the async task that executes the function.
            async def task():
                while True:
                    value = await self._channels[inbound_channel].get()
                    output = f(value)
                    selector_value = selector_fn(output)
                    if (
                        selector_value not in outbound_channels
                        and default_outbound_channel
                    ):
                        # selector value is not in the outbound channel map,
                        # put on default.
                        await self._channels[default_outbound_channel].put(
                            output
                        )
                    elif selector_value not in outbound_channels:
                        # selector value is not in the outbound channel map and
                        # there isn't a default outbound channel.
                        raise ValueError(
                            "No channel for selector value {value}. "
                            "Add a default channel."
                        )
                        self._channels[inbound_channel].task_done()
                    else:
                        # selector value is in the outbound channel map, put
                        # the value on that channel.
                        await self._channels[
                            outbound_channels[selector_value]
                        ].put(output)
                    self._channels[inbound_channel].task_done()

            for _ in range(num_tasks):
                self._tasks.append(task)
            return f

        return decorator

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

    def _shutdown_and_raise(
        self, loop: asyncio.AbstractEventLoop, context: Dict[str, Any]
    ):
        for t in self._running_tasks:
            t.cancel()
        loop.stop()
        raise context["exception"]

    async def _run_tasks(self, seq: Iterable) -> None:
        get_event_loop().set_exception_handler(self._shutdown_and_raise)
        # Create the queues inside the event loop attached to `run`.
        self._channels = {c: Queue() for c in self._channel_names}
        # Load the tasks into the loop.
        self._running_tasks = [create_task(t()) for t in self._tasks]
        # Load the entry point queue.
        for s in seq:
            await self._entry_point(s)
        # ... wait until work is completed.
        for q in self._channels.values():
            await q.join()
        # Now shut the tasks down.
        for t in self._running_tasks:
            t.cancel()

    def run(self, seq: Iterable) -> None:
        async_run(self._run_tasks(seq))
