import logging
import asyncio
from asyncio import (
    Queue,
    create_task,
    run as async_run,
    gather as gather_tasks,
    Task,
)
from typing import (
    Callable,
    List,
    Iterable,
    Dict,
    Any,
    Tuple,
    TypeVar,
    Type,
    Union,
)
from itertools import repeat

# ! Ladies and gentlemen we have a race condition.
# ! For a multilayer topology, the input queue can get filled, then plugged
# ! with a Stop. This stop can shutdown tasks early in the topology before a
# ! downstream task can send a failure and clear the queues. I think the
# ! solution is to limit the size of the internal queues (it's okay for the
# ! source queue to be huge but limiting the internal queue size will introduce
# ! the required backpressure
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | "
    "%(funcName)s | %(message)s"
)
logger = logging.getLogger("turbine")
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


class Stop:
    pass


class Fail(Stop):
    def __init__(self, exc: Type[Exception], msg: str):
        self.exc = exc
        self.msg = msg

    def raise_exc(self):
        raise self.exc(self.msg)


class Turbine:
    def __init__(self, debug=False):
        # Channel names gets added to as decorators are called.
        self._channel_names: Dict[str, int] = {}
        # Track the number of tasks associated with each channel so we know
        # how many stops to send downstream. Otherwise, we'll block forever.
        # Trust me. Trust. Me.
        self._channel_num_tasks: Dict[str, int] = {}
        # Map the channel aliases to a channel. This gets filled inside the
        # event loop.
        self._channels: Dict[str, asyncio.Queue] = {}
        # This is how the topology gets loaded.
        self._entry_point: Callable = None
        self._tasks: List[Callable] = []
        self._running_tasks: List[Task] = []
        if debug:
            logger.setLevel("DEBUG")
            logger.debug("Logger set to debug.")

    async def _send_stop(
        self, outbound_channels: List[str], stopper: Stop
    ) -> None:
        for c in outbound_channels:
            for _ in range(self._channel_num_tasks[c]):
                logger.debug(f"Sending stop to {c}.")
                await self._channels[c].put(stopper)

    async def _stop_tasks(self) -> None:
        logger.debug("Stopping tasks.")
        logger.debug(f"Queue statuses: {self._queue_statuses()}.")
        tasks_done = await gather_tasks(
            *self._running_tasks, return_exceptions=True
        )
        for t in tasks_done:
            if isinstance(t, Fail):
                t.raise_exc()

    def _queue_statuses(self) -> str:
        queue_statuses = [
            f"{c}: {q._unfinished_tasks}/{q.qsize()}"  # type: ignore
            for c, q in self._channels.items()
        ]
        return " | ".join(queue_statuses)

    def _clear_queues(self) -> None:
        for q in self._channels.values():
            # So this is ... questionable. Have to ignore the type to get it
            # to pass the checker because we shouldn't be reaching in for this.
            # There is no other way to clear a queue.
            q._queue.clear()  # type: ignore
        logger.debug("Queues allegedly cleared.")
        logger.debug(f"Queue statuses: {self._queue_statuses()}.")

    def _add_channels(self, channels: Iterable[Tuple[str, int]]) -> None:
        for name, size in channels:
            # This condition will apply to the entry point, which is set in the
            # source function because that queue needs to be unbounded.
            if name not in self._channel_names:
                self._channel_names[name] = size

    async def _source(
        self, outbound_name: str, f: Callable, *args, **kwargs
    ) -> None:
        if len(args) > 1 or not isinstance(args[0], Stop):
            value = f(*args, **kwargs)
            await self._channels[outbound_name].put(value)
        else:
            value = args[0]
            logger.debug(f"Source received stop: {value}.")
            logger.debug(f"Queue statuses - {self._queue_statuses()}.")
            await self._send_stop([outbound_name], value)

    def source(self, outbound_name: str) -> Callable:
        # Add the outbound channel to the channel map.
        self._add_channels([(outbound_name, 0)])

        # Now do the real decorator.
        def decorator(f: Callable) -> Callable:
            async def entry_point(*args, **kwargs):
                try:
                    await self._source(outbound_name, f, *args, **kwargs)
                except Exception as e:
                    fail = Fail(type(e), str(e))
                    self._clear_queues()
                    await self._entry_point(fail)

            # The entry point will get called with Turbine.run. We need this
            # separate from other tasks because it's not an infinite loop.
            # That's really the only difference between this decorator and the
            # others ... no while True.
            self._entry_point = entry_point
            return f

        return decorator

    async def _scatter(
        self, inbound_channel: str, outbound_channels: List[str], f: Callable,
    ) -> Stop:
        while True:
            input_value = await self._channels[inbound_channel].get()
            if isinstance(input_value, Stop):
                logger.debug(f"Scatter received stop: {input_value}.")
                logger.debug(f"Queue statuses - {self._queue_statuses()}.")
                await self._send_stop(outbound_channels, input_value)
                self._channels[inbound_channel].task_done()
                return input_value
            # Call the function on the inputs ...
            output = f(input_value)
            # ... and copy the outputs to each of the outbound channels.
            for output, channel in zip(repeat(output), outbound_channels):
                await self._channels[channel].put(output)
            self._channels[inbound_channel].task_done()

    def scatter(
        self,
        inbound_channel: str,
        outbound_channels: List[str],
        num_tasks: int = 1,
    ) -> Callable:
        # Add the inbound channels to the channel map.
        self._add_channels([(inbound_channel, 1)])
        self._channel_num_tasks[inbound_channel] = num_tasks

        def decorator(f: Callable) -> Callable:
            # Create the async task that applies the function.
            async def task() -> Stop:
                try:
                    return await self._scatter(
                        inbound_channel, outbound_channels, f
                    )
                except Exception as e:
                    logger.exception(f"Scatter got exception {e}.")
                    fail = Fail(type(e), str(e))
                    self._clear_queues()
                    await self._entry_point(fail)
                    # We need to restart the function so the topology is
                    # repaired. This ensures the failure is appropriately
                    # propagated.
                    return await self._scatter(
                        inbound_channel, outbound_channels, f
                    )

            # Create all of the tasks.
            for _ in range(num_tasks):
                self._tasks.append(task)
            return f

        return decorator

    def union(self):
        pass  # ! This is a tough one. In Clojure I used alts!!.

    async def _gather(
        self, inbound_channels: List[str], outbound_channel: str, f: Callable
    ) -> Stop:
        while True:
            values = []
            stop: Union[Stop, None] = None
            # Read off the inbound channels sequentially.
            for c in inbound_channels:
                v = await self._channels[c].get()
                if not isinstance(v, Stop):
                    values.append(v)
                else:
                    stop = v
                    break  # Stop means stop.
            # Determine if a stop came from any of the inbound channels and
            # send the stop message along.
            if stop:
                logger.debug(f"Gather received stop: {stop}.")
                logger.debug(f"Queue statuses - {self._queue_statuses()}.")
                await self._send_stop([outbound_channel], stop)
                for c in inbound_channels:
                    self._channels[c].task_done()
                return stop

            # If we don't stop, apply the function and send the result
            # downstream.
            output = f(*values)
            await self._channels[outbound_channel].put(output)
            for c in inbound_channels:
                self._channels[c].task_done()

    def gather(
        self,
        inbound_channels: List[str],
        outbound_channel: str,
        num_tasks: int = 1,
    ) -> Callable:
        self._add_channels(zip(inbound_channels, repeat(1)))
        for inbound_name in inbound_channels:
            self._channel_num_tasks[inbound_name] = num_tasks

        def decorator(f: Callable) -> Callable:
            # Create the async task that applies the function.
            async def task():
                try:
                    return await self._gather(
                        inbound_channels, outbound_channel, f
                    )
                except Exception as e:
                    logger.exception(f"Gather got an exception: {e}.")
                    fail = Fail(type(e), str(e))
                    self._clear_queues()
                    await self._entry_point(fail)
                    # We need to restart the function so the topology is
                    # repaired. This ensures the failure is appropriately
                    # propagated.
                    return await self._gather(
                        inbound_channels, outbound_channel, f
                    )

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
            self._add_channels([(inbound_channel, 1)])
            self._channel_num_tasks[inbound_channel] = num_tasks

            # Create the async task that executes the function.
            async def task():
                while True:
                    value = await self._channels[inbound_channel].get()
                    if isinstance(value, Stop):
                        logger.debug(f"Selector got a stop {value}.")
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
        self._add_channels([(inbound_name, 1)])

        def decorator(f: Callable) -> Callable:
            async def task():
                while True:
                    value = await self._channels[inbound_name].get()
                    if isinstance(value, Stop):
                        logger.debug(f"Sinker received stop: {value}.")
                        self._channels[inbound_name].task_done()
                        logger.debug(
                            f"Queue statuses - {self._queue_statuses()}."
                        )
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
        self._channels = {
            c: Queue(maxsize=s) for c, s in self._channel_names.items()
        }
        # Load the tasks into the loop.
        self._running_tasks = [create_task(t()) for t in self._tasks]

        # Load the entry point queue.
        for s in seq:
            await self._entry_point(s)
        await self._entry_point(Stop())
        logger.debug(f"Queue statuses: {self._queue_statuses()}.")
        # Now shut the tasks down.
        await self._stop_tasks()

    def run(self, seq: Iterable) -> None:
        queue_sizes = " | ".join(
            [f"{q}: {n}" for q, n in self._channel_names.items()]
        )
        num_tasks = " | ".join(
            [f"{q}: {n}" for q, n in self._channel_num_tasks.items()]
        )
        logger.debug(f"Queue sizes: {queue_sizes}.")
        logger.debug(f"Task concurrencies: {num_tasks}.")
        async_run(self._run_tasks(seq), debug=True)
