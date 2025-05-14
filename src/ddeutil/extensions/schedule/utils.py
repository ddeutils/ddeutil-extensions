# ------------------------------------------------------------------------------
# Copyright (c) 2023 Priyanshu Panwar. All rights reserved.
# Licensed under the MIT License.
# This code refs from: https://github.com/priyanshu-panwar/fastapi-utilities
# ------------------------------------------------------------------------------
# Example:
#
# NOTE: Enable the schedules route.
# if api_config.enable_route_schedule:
#     from ..logs import get_audit
#     from ..scheduler import schedule_task
#     from .routes import schedule
#
#     app.include_router(schedule, prefix=api_config.prefix_path)
#
#     @schedule.on_event("startup")
#     @repeat_at(cron="* * * * *", delay=2)
#     def scheduler_listener():
#         """Schedule broker every minute at 02 second."""
#         logger.debug(
#             f"[SCHEDULER]: Start listening schedule from queue "
#             f"{app.state.scheduler}"
#         )
#         if app.state.workflow_tasks:
#             schedule_task(
#                 app.state.workflow_tasks,
#                 stop=datetime.now(config.tz) + timedelta(minutes=1),
#                 queue=app.state.workflow_queue,
#                 threads=app.state.workflow_threads,
#                 audit=get_audit(),
#             )
#
#     @schedule.on_event("startup")
#     @repeat_at(cron="*/5 * * * *", delay=10)
#     def monitoring():
#         """Monitoring workflow thread that running in the background."""
#         logger.debug("[MONITOR]: Start monitoring threading.")
#         snapshot_threads: list[str] = list(app.state.workflow_threads.keys())
#         for t_name in snapshot_threads:
#
#             thread_release: ReleaseThread = app.state.workflow_threads[t_name]
#
#             # NOTE: remove the thread that running success.
#             if not thread_release["thread"].is_alive():
#                 app.state.workflow_threads.pop(t_name)
from __future__ import annotations

import asyncio
import logging
from asyncio import ensure_future
from datetime import datetime
from functools import wraps

try:
    from starlette.concurrency import run_in_threadpool
except ImportError:
    run_in_threadpool = None

from ddeutil.workflow import CronJob, config

logger = logging.getLogger("uvicorn.error")


def get_cronjob_delta(cron: str) -> float:
    """This function returns the time delta between now and the next cron
    execution time.

    :rtype: float
    """
    now: datetime = datetime.now(tz=config.tz)
    cron = CronJob(cron)
    return (cron.schedule(now).next - now).total_seconds()


def cron_valid(cron: str, raise_error: bool = True) -> bool:
    """Check this crontab string value is valid with its cron syntax.

    :rtype: bool
    """
    try:
        CronJob(cron)
        return True
    except Exception as err:
        if raise_error:
            raise ValueError(f"Crontab value does not valid, {cron}") from err
        return False


async def run_func(
    is_coroutine,
    func,
    *args,
    raise_exceptions: bool = False,
    **kwargs,
):
    """Run function inside the repeat decorator functions."""
    try:
        if is_coroutine:
            await func(*args, **kwargs)
        else:
            if run_in_threadpool is None:
                raise ImportError(
                    "Please install `starlette` to use `run_in_threadpool`"
                )
            await run_in_threadpool(func, *args, **kwargs)
    except Exception as e:
        logger.exception(e)
        if raise_exceptions:
            raise e


def repeat_at(
    *,
    cron: str,
    delay: float = 0,
    raise_exceptions: bool = False,
    max_repetitions: int = None,
):
    """This function returns a decorator that makes a function execute
    periodically as per the cron expression provided.

    :param cron: (str) A Cron-style string for periodic execution, e.g.
        '0 0 * * *' every midnight
    :param delay: (float) A delay seconds value.
    :param raise_exceptions: (bool) A raise exception flag. Whether to raise
        exceptions or log them if raise was set be false.
    :param max_repetitions: int (default None)
        Maximum number of times to repeat the function. If None, repeat
        indefinitely.
    """
    if max_repetitions and max_repetitions <= 0:
        raise ValueError(
            "max_repetitions should more than zero if it want to set"
        )

    def decorator(func):
        is_coroutine: bool = asyncio.iscoroutinefunction(func)

        @wraps(func)
        def wrapper(*_args, **_kwargs):
            repetitions: int = 0
            cron_valid(cron)

            async def loop(*args, **kwargs):
                nonlocal repetitions
                while max_repetitions is None or repetitions < max_repetitions:
                    sleep_time = get_cronjob_delta(cron) + delay
                    await asyncio.sleep(sleep_time)
                    await run_func(
                        is_coroutine,
                        func,
                        *args,
                        raise_exceptions=raise_exceptions,
                        **kwargs,
                    )
                    repetitions += 1

            ensure_future(loop(*_args, **_kwargs))

        return wrapper

    return decorator


def repeat_every(
    *,
    seconds: float,
    wait_first: bool = False,
    raise_exceptions: bool = False,
    max_repetitions: int = None,
):
    """This function returns a decorator that schedules a function to execute
    periodically after every `seconds` seconds.

    :param seconds: float
        The number of seconds to wait before executing the function again.
    :param wait_first: bool (default False)
        Whether to wait `seconds` seconds before executing the function for the
        first time.
    :param raise_exceptions: bool (default False)
        Whether to raise exceptions instead of logging them.
    :param max_repetitions: int (default None)
        The maximum number of times to repeat the function. If None, the
        function will repeat indefinitely.
    """
    if max_repetitions and max_repetitions <= 0:
        raise ValueError(
            "max_repetitions should more than zero if it want to set"
        )

    def decorator(func):
        is_coroutine: bool = asyncio.iscoroutinefunction(func)

        @wraps(func)
        async def wrapper(*_args, **_kwargs):
            repetitions = 0

            async def loop(*args, **kwargs):
                nonlocal repetitions

                if wait_first:
                    await asyncio.sleep(seconds)

                while max_repetitions is None or repetitions < max_repetitions:
                    await run_func(
                        is_coroutine,
                        func,
                        *args,
                        raise_exceptions=raise_exceptions,
                        **kwargs,
                    )

                    repetitions += 1
                    await asyncio.sleep(seconds)

            ensure_future(loop(*_args, **_kwargs))

        return wrapper

    return decorator
