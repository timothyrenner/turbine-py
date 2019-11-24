from asyncio import (
    Queue,
    create_task,
    run as async_run,
    all_tasks,
    gather as gather_tasks,
)
from typing import Callable, List, Iterable, Dict, Any, TypeVar
from itertools import repeat

# TODO Configurable queue sizes.
# TODO Check for the existence of the inbound channels.
# TODO Figure out how to implement union
# TODO Get error handling under control in a semi-elegant way if possible.

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


class Stop:
    pass


class Fail(Stop):
    def __init__(self, exc: Exception, msg: str):
        self.exc = exc
        self.msg = msg

    def raise_exc(self):
        raise self.exc(self.msg)


class Turbine:
    def __init__(self):
        # Map the channel aliases to a channel.
        self._channels = {}
        self._channel_names = set()
        self._channel_num_tasks = {}  # channel name -> number of tasks.
        self._entry_point = None
        self._tasks = []
        self._running_tasks = []

    async def _stop_tasks(self):
        print("Inside _stop_tasks.")
        self._print_queue_statuses()
        print("\n\n".join(map(str, all_tasks())))
        tasks_done = await gather_tasks(
            *self._running_tasks, return_exceptions=True
        )
        for t in tasks_done:
            if isinstance(t, Fail):
                t.raise_exc()

    def _print_queue_statuses(self):
        for c, q in self._channels.items():
            print(f"{c}: {q.qsize()}.")

    def _clear_queues(self):
        for q in self._channels.values():
            q._queue.clear()
        print("queues allegedly cleared.")
        self._print_queue_statuses()

    def _add_channels(self, channels: List[str]) -> None:
        self._channel_names.update(channels)

    def source(self, outbound_name: str) -> Callable:
        # Add the outbound channel to the channel map.
        self._add_channels([outbound_name])

        # Now do the real decorator.
        def decorator(f: Callable) -> Callable:
            async def wrapper(*args, **kwargs):
                if len(args) > 1 or not isinstance(args[0], Stop):
                    print("not a stop.")
                    value = f(*args, **kwargs)
                else:
                    print("got a stop.")
                    value = args[0]
                # ... and drop that on the outbound channel.
                print(value)
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
        self._channel_num_tasks[inbound_name] = num_tasks

        def decorator(f: Callable) -> Callable:
            # Create the async task that applies the function.
            async def task():
                while True:
                    input_value = await self._channels[inbound_name].get()
                    if isinstance(input_value, Stop):
                        print(f"Scatter got a stop: {input_value}.")
                        for c in outbound_names:
                            for _ in range(self._channel_num_tasks[c]):
                                await self._channels[c].put(input_value)
                        self._channels[inbound_name].task_done()
                        return input_value
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
        for inbound_name in inbound_names:
            self._channel_num_tasks[inbound_name] = num_tasks

        def decorator(f: Callable) -> Callable:
            # Create the async task that applies the function.
            async def task():
                while True:
                    values = []
                    for c in inbound_names:
                        v = await self._channels[c].get()
                        if isinstance(v, Stop):
                            # ! task_done is pretty crucial.
                            values = v
                            break
                        values.append(v)
                    if isinstance(values, Stop):
                        await self._channels[outbound_name].put(values)
                        for c in inbound_names:
                            self._channels[c].task_done()
                        return values
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
            all_outbound_channels = list(outbound_channels.values())
            if default_outbound_channel:
                all_outbound_channels.append(default_outbound_channel)
            self._add_channels(all_outbound_channels + [inbound_channel])
            self._channel_num_tasks[inbound_channel] = num_tasks

            # Create the async task that executes the function.
            async def task():
                while True:
                    value = await self._channels[inbound_channel].get()
                    if isinstance(value, Stop):
                        print(f"Selector got a stop {value}.")
                        for c in all_outbound_channels:
                            await self._channels[c].put(value)
                        self._channels[inbound_channel].task_done()
                        return value
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
                        # await self._entry_point(Fail(ValueError, "fuck"))
                        fail = Fail(ValueError, "fuck")
                        self._clear_queues()
                        await self._entry_point(fail)
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
        self._channel_num_tasks[inbound_name] = num_tasks

        def decorator(f: Callable) -> Callable:
            async def task():
                while True:
                    value = await self._channels[inbound_name].get()
                    if isinstance(value, Stop):
                        print(f"Sinker got a stop: {value}.")
                        self._channels[inbound_name].task_done()
                        return value
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
        self._running_tasks = [create_task(t()) for t in self._tasks]

        # Load the entry point queue.
        for s in seq:
            await self._entry_point(s)
        await self._entry_point(Stop())
        self._print_queue_statuses()
        # Now shut the tasks down.
        await self._stop_tasks()

    def run(self, seq: Iterable) -> None:
        print(self._channel_num_tasks)
        async_run(self._run_tasks(seq), debug=True)
