import logging

from typing import Callable, Type, Dict, List, Tuple, Iterable
from asyncio import Queue, create_task, gather, run as async_run

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | "
    "%(funcName)s | %(message)s"
)
logger = logging.getLogger("turbine")

# TODO Topology validation.


def identity(x):
    return x


class Stop:
    pass


class Fail(Stop):
    def __init__(self, exc: Type[Exception], msg: str):
        self.exc: Type[Exception] = exc
        self.msg: str = msg

    def raise_exc(self):
        raise self.exc(self.msg)


class Turbine:
    def __init__(self, debug=False):
        # Entry point for loading the topology.
        self.entry_point: Callable = None
        # Map of channel names to their queues.
        # Initialized within the event loop.
        self._channels: Dict[str, Queue] = {}
        # Map of channel to the number of running tasks.
        # Used to shut the topology down.
        self._channel_num_tasks: Dict[str, int] = {}
        # List of tasks to be run.
        # Used to start the tasks in the event loop.
        self._tasks: List[Callable] = []
        # Flag indicating the status of the topology.
        # This is used to terminate loading.
        self._topology_running: bool = False
        # Entry point for the topology.
        self._entry_point: Callable = None
        if debug:
            logger.setLevel("DEBUG")
            logger.debug("Logger set to debug.")

    def _log_topology_status(self):
        queue_statuses = " | ".join(
            [
                f"{c}: {q._unfinished_tasks}/{q.qsize()}"
                for c, q in self._channels.items()
            ]
        )
        queue_tasks = " | ".join(
            [f"{c}: {n}" for c, n in self._channel_num_tasks.items()]
        )
        logger.debug(f"Queue statuses: {queue_statuses}.")
        logger.debug(f"Queue tasks: {queue_tasks}.")
        logger.debug(f"Topology running: {self._topology_running}.")

    async def _fail_topology(self, fail: Fail) -> None:
        for channel, queue in self._channels.items():
            # This particular move is grimey. There's no "sanctioned" way to
            # clear these queues so we have to rely on internal implementation.
            queue._queue.clear()  # type: ignore
            queue._unfinished_tasks = 0  # type: ignore
            self._topology_running = False
            for _ in range(self._channel_num_tasks[channel]):
                await queue.put(fail)
        logger.debug(f"Queues cleared and fails placed.")
        self._log_topology_status()

    async def _stop_downstream(
        self, downstream_channels: List[str], stopper: Stop
    ) -> None:
        for channel in downstream_channels:
            for _ in range(self._channel_num_tasks[channel]):
                await self._channels[channel].put(stopper)

    def _add_channels(self, channels: List[Tuple[str, int]]) -> None:
        for channel, tasks in channels:
            self._channel_num_tasks[channel] = tasks

    async def _source(
        self, outbound_channel: str, f: Callable, *args, **kwargs
    ) -> None:
        if len(args) > 1 or not isinstance(args[0], Stop):
            value = f(*args, **kwargs)
            await self._channels[outbound_channel].put(value)
        else:
            await self._stop_downstream([outbound_channel], args[0])

    def source(self, outbound_channel: str) -> Callable:
        def decorator(f: Callable = identity) -> Callable:
            async def entry_point(*args, **kwargs):
                try:
                    await self._source(outbound_channel, f, *args, **kwargs)
                except Exception as e:
                    logger.exception(f"Source got exception {e}.")
                    await self._fail_topology(Fail(type(e), str(e)))

            self._entry_point = entry_point
            return f

        return decorator

    async def _sink(self, inbound_channel: str, f: Callable) -> Stop:
        inbound_queue = self._channels[inbound_channel]
        while True:
            value = await inbound_queue.get()
            if isinstance(value, Stop):
                logger.debug(f"Sink got a stop: {value}.")
                inbound_queue.task_done()
                self._channel_num_tasks[inbound_channel] -= 1
                return value
            else:
                f(value)
                inbound_queue.task_done()

    def sink(self, inbound_channel: str, num_tasks: int = 1) -> Callable:
        self._channel_num_tasks[inbound_channel] = num_tasks
        self._add_channels([(inbound_channel, num_tasks)])

        def decorator(f: Callable) -> Callable:
            async def sink_task() -> Stop:
                try:
                    return await self._sink(inbound_channel, f)
                except Exception as e:
                    logger.exception("sink got an exception.")
                    fail = Fail(type(e), str(e))
                    self._channel_num_tasks[inbound_channel] -= 1
                    await self._fail_topology(fail)
                    return fail

            for _ in range(num_tasks):
                self._tasks.append(sink_task)

            return f

        return decorator

    async def _run_tasks(self, seq: Iterable) -> None:
        # First create the channels.
        logger.debug("Creating channels.")
        self._channels = {
            c: Queue(maxsize=s) for c, s in self._channel_num_tasks.items()
        }

        # Create the tasks.
        logger.debug("Launching tasks.")
        running_tasks = [create_task(t()) for t in self._tasks]
        self._topology_running = True
        self._log_topology_status()

        # Load the entry point.
        for s in seq:
            if self._topology_running:
                await self._entry_point(s)
        # Now stop the topology.
        if self._topology_running:
            await self._entry_point(Stop())

        # Check the status of the completed tasks.
        completed_tasks = await gather(*running_tasks)
        for t in completed_tasks:
            if isinstance(t, Fail):
                t.raise_exc()

    def run(self, seq: Iterable) -> None:
        self._log_topology_status()
        async_run(self._run_tasks(seq))
