# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
"""Workflow module is the core module of this package. It keeps Release,
ReleaseQueue, and Workflow models.

    This package implement timeout strategy on the workflow execution layer only
because the main propose of this package is using Workflow to be orchestrator.

    ReleaseQueue is the memory storage of Release for tracking this release
already run or pending in the current session.
"""
from __future__ import annotations

from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
    as_completed,
)
from dataclasses import field
from datetime import datetime, timedelta
from enum import Enum
from functools import partial, total_ordering
from heapq import heappop, heappush
from threading import Lock
from typing import Optional, Union

from ddeutil.workflow import (
    SUCCESS,
    Audit,
    CronRunner,
    Result,
    Workflow,
    WorkflowException,
    clear_tz,
    dynamic,
    gen_id,
    get_audit,
    get_dt_now,
    reach_next_minute,
    replace_sec,
    wait_until_next_minute,
)
from pydantic import ConfigDict, Field
from pydantic.dataclasses import dataclass
from typing_extensions import Self

from ..__types import DictData


class ReleaseType(str, Enum):
    """Release Type Enum support the type field on the Release dataclass."""

    DEFAULT = "manual"
    SCHEDULE = "schedule"
    POKING = "poking"
    FORCE = "force"


@total_ordering
@dataclass(config=ConfigDict(use_enum_values=True))
class Release:
    """Release object that use for represent the release datetime."""

    date: datetime = Field(
        description=(
            "A release date that should has second and millisecond equal 0."
        )
    )
    type: ReleaseType = Field(
        default=ReleaseType.DEFAULT,
        description="A type of release that create before start execution.",
    )

    def __repr__(self) -> str:
        """Override __repr__ method for represent value of `date` field.

        :rtype: str
        """
        return repr(f"{self.date:%Y-%m-%d %H:%M:%S}")

    def __str__(self) -> str:
        """Override string value of this release object with the `date` field.

        :rtype: str
        """
        return f"{self.date:%Y-%m-%d %H:%M:%S}"

    @classmethod
    def from_dt(cls, dt: Union[datetime, str]) -> Self:
        """Construct Release object from `datetime` or `str` objects.

            This method will replace second and millisecond value to 0 and
        replace timezone to the `tz` config setting or extras overriding before
        create Release object.

        :param dt: (Union[datetime, str]) A datetime object or string that want to
            construct to the Release object.

        :raise TypeError: If the type of the dt argument does not valid with
            datetime or str object.

        :rtype: Release
        """
        if isinstance(dt, str):
            dt: datetime = datetime.fromisoformat(dt)
        elif not isinstance(dt, datetime):
            raise TypeError(
                f"The `from_dt` need the `dt` parameter type be `str` or "
                f"`datetime` only, not {type(dt)}."
            )
        return cls(date=replace_sec(dt.replace(tzinfo=None)))

    def __eq__(self, other: Union[Release, datetime]) -> bool:
        """Override equal property that will compare only the same type or
        datetime.

        :rtype: bool
        """
        if isinstance(other, self.__class__):
            return self.date == other.date
        elif isinstance(other, datetime):
            return self.date == other
        return NotImplemented

    def __lt__(self, other: Union[Release, datetime]) -> bool:
        """Override less-than property that will compare only the same type or
        datetime.

        :rtype: bool
        """
        if isinstance(other, self.__class__):
            return self.date < other.date
        elif isinstance(other, datetime):
            return self.date < other
        return NotImplemented


class ReleaseQueue:
    """ReleaseQueue object that is storage management of Release objects on
    the memory with list object.
    """

    def __init__(
        self,
        queue: Optional[list[Release]] = None,
        running: Optional[list[Release]] = None,
        complete: Optional[list[Release]] = None,
        extras: Optional[DictData] = None,
    ):
        self.queue: list[Release] = queue or []
        self.running: list[Release] = running or []
        self.complete: list[Release] = complete or []
        self.extras: DictData = extras or {}
        self.lock: Lock = Lock()

    @classmethod
    def from_list(
        cls,
        queue: Optional[Union[list[datetime], list[Release]]] = None,
    ) -> Self:
        """Construct ReleaseQueue object from an input queue value that passing
        with list of datetime or list of Release.

        :param queue: A queue object for create ReleaseQueue instance.

        :raise TypeError: If the type of input queue does not valid.

        :rtype: ReleaseQueue
        """
        if queue is None:
            return cls()

        if isinstance(queue, list):
            if all(isinstance(q, datetime) for q in queue):
                return cls(queue=[Release.from_dt(q) for q in queue])

            if all(isinstance(q, Release) for q in queue):
                return cls(queue=queue)

        raise TypeError(
            "Type of the queue does not valid with ReleaseQueue "
            "or list of datetime or list of Release."
        )

    @property
    def is_queued(self) -> bool:
        """Return True if it has workflow release object in the queue.

        :rtype: bool
        """
        return len(self.queue) > 0

    def check_queue(self, value: Union[Release, datetime]) -> bool:
        """Check a Release value already exists in list of tracking
        queues.

        :param value: A Release object that want to check it already in
            queues.

        :rtype: bool
        """
        if isinstance(value, datetime):
            value = Release.from_dt(value)

        with self.lock:
            return (
                (value in self.queue)
                or (value in self.running)
                or (value in self.complete)
            )

    def mark_complete(self, value: Release) -> Self:
        """Push Release to the complete queue. After push the release, it will
        delete old release base on the `CORE_MAX_QUEUE_COMPLETE_HIST` value.

        :param value: (Release) A Release value that want to push to the
            complete field.

        :rtype: Self
        """
        with self.lock:
            if value in self.running:
                self.running.remove(value)

            heappush(self.complete, value)

            # NOTE: Remove complete queue on workflow that keep more than the
            #   maximum config value.
            num_complete_delete: int = len(self.complete) - dynamic(
                "max_queue_complete_hist", extras=self.extras
            )

            if num_complete_delete > 0:
                for _ in range(num_complete_delete):
                    heappop(self.complete)

        return self

    def gen(
        self,
        end_date: datetime,
        audit: type[Audit],
        runner: CronRunner,
        name: str,
        *,
        force_run: bool = False,
        extras: Optional[DictData] = None,
    ) -> Self:
        """Generate a Release model to the queue field with an input CronRunner.

        Steps:
            - Create Release object from the current date that not reach the end
              date.
            - Check this release do not store on the release queue object.
              Generate the next date if it exists.
            - Push this release to the release queue

        :param end_date: (datetime) An end datetime object.
        :param audit: (type[Audit]) An audit class that want to make audit
            instance.
        :param runner: (CronRunner) A `CronRunner` object.
        :param name: (str) A target name that want to check at pointer of audit.
        :param force_run: (bool) A flag that allow to release workflow if the
            audit with that release was pointed. (Default is False).
        :param extras: (DictDatA) An extra parameter that want to override core
            config values.

        :rtype: ReleaseQueue

        """
        if clear_tz(runner.date) > clear_tz(end_date):
            return self

        release = Release(
            date=clear_tz(runner.date),
            type=(ReleaseType.FORCE if force_run else ReleaseType.POKING),
        )

        while self.check_queue(release) or (
            audit.is_pointed(name=name, release=release.date, extras=extras)
            and not force_run
        ):
            release = Release(
                date=clear_tz(runner.next),
                type=(ReleaseType.FORCE if force_run else ReleaseType.POKING),
            )

        if clear_tz(runner.date) > clear_tz(end_date):
            return self

        with self.lock:
            heappush(self.queue, release)

        return self


class WorkflowPoke(Workflow):
    """Workflow Poke model that was implemented the poke method."""

    def queue(
        self,
        offset: float,
        end_date: datetime,
        queue: ReleaseQueue,
        audit: type[Audit],
        *,
        force_run: bool = False,
    ) -> ReleaseQueue:
        """Generate Release from all on values from the on field and store them
        to the ReleaseQueue object.

        :param offset: An offset in second unit for time travel.
        :param end_date: An end datetime object.
        :param queue: A workflow queue object.
        :param audit: An audit class that want to make audit object.
        :param force_run: A flag that allow to release workflow if the audit
            with that release was pointed.

        :rtype: ReleaseQueue
        """
        for on in self.on:

            queue.gen(
                end_date,
                audit,
                on.next(get_dt_now(offset=offset).replace(microsecond=0)),
                self.name,
                force_run=force_run,
            )

        return queue

    def poke(
        self,
        params: Optional[DictData] = None,
        start_date: Optional[datetime] = None,
        *,
        run_id: Optional[str] = None,
        periods: int = 1,
        audit: Optional[Audit] = None,
        force_run: bool = False,
        timeout: int = 1800,
        max_poking_pool_worker: int = 2,
    ) -> Result:
        """Poke workflow with a start datetime value that will pass to its
        `on` field on the threading executor pool for execute the `release`
        method (It run all schedules that was set on the `on` values).

            This method will observe its `on` field that nearing to run with the
        `self.release()` method.

            The limitation of this method is not allow run a date that gather
        than the current date.

        :param params: (DictData) A parameter data.
        :param start_date: (datetime) A start datetime object.
        :param run_id: (str) A workflow running ID for this poke.
        :param periods: (int) A periods in minutes value that use to run this
            poking. (Default is 1)
        :param audit: (Audit) An audit object that want to use on this poking
            process.
        :param force_run: (bool) A flag that allow to release workflow if the
            audit with that release was pointed. (Default is False)
        :param timeout: (int) A second value for timeout while waiting all
            futures run completely.
        :param max_poking_pool_worker: (int) The maximum poking pool worker.
            (Default is 2 workers)

        :raise WorkflowException: If the periods parameter less or equal than 0.

        :rtype: Result
        :return: A list of all results that return from `self.release` method.
        """
        audit: type[Audit] = audit or get_audit(extras=self.extras)
        result: Result = Result(
            run_id=(run_id or gen_id(self.name, unique=True))
        )

        # VALIDATE: Check the periods value should gather than 0.
        if periods <= 0:
            raise WorkflowException(
                "The period of poking should be `int` and grater or equal "
                "than 1."
            )

        if len(self.on) == 0:
            result.trace.warning(
                f"[POKING]: {self.name!r} not have any schedule!!!"
            )
            return result.catch(status=SUCCESS, context={"outputs": []})

        # NOTE: Create the current date that change microsecond to 0
        current_date: datetime = datetime.now().replace(microsecond=0)

        if start_date is None:
            # NOTE: Force change start date if it gathers than the current date,
            #   or it does not pass to this method.
            start_date: datetime = current_date
            offset: float = 0
        elif start_date <= current_date:
            start_date = start_date.replace(microsecond=0)
            offset: float = (current_date - start_date).total_seconds()
        else:
            raise WorkflowException(
                f"The start datetime should less than or equal the current "
                f"datetime, {current_date:%Y-%m-%d %H:%M:%S}."
            )

        # NOTE: The end date is using to stop generate queue with an input
        #   periods value. It will change to MM:59.
        #   For example:
        #       (input)  start_date = 12:04:12, offset = 2
        #       (output) end_date = 12:06:59
        end_date: datetime = start_date.replace(second=0) + timedelta(
            minutes=periods + 1, seconds=-1
        )

        result.trace.info(
            f"[POKING]: Execute Poking: {self.name!r} "
            f"({start_date:%Y-%m-%d %H:%M:%S} ==> {end_date:%Y-%m-%d %H:%M:%S})"
        )

        params: DictData = {} if params is None else params
        context: list[Result] = []
        q: ReleaseQueue = ReleaseQueue()

        # NOTE: Create reusable partial function and add Release to the release
        #   queue object.
        partial_queue = partial(
            self.queue, offset, end_date, audit=audit, force_run=force_run
        )
        partial_queue(q)
        if not q.is_queued:
            result.trace.warning(
                f"[POKING]: Skip {self.name!r}, not have any queue!!!"
            )
            return result.catch(status=SUCCESS, context={"outputs": []})

        with ThreadPoolExecutor(
            max_workers=dynamic(
                "max_poking_pool_worker",
                f=max_poking_pool_worker,
                extras=self.extras,
            ),
            thread_name_prefix="wf_poking_",
        ) as executor:

            futures: list[Future] = []

            while q.is_queued:

                # NOTE: Pop the latest Release object from the release queue.
                release: Release = heappop(q.queue)

                if reach_next_minute(release.date, offset=offset):
                    result.trace.debug(
                        f"[POKING]: Skip Release: "
                        f"{release.date:%Y-%m-%d %H:%M:%S}"
                    )
                    heappush(q.queue, release)
                    wait_until_next_minute(get_dt_now(offset=offset))

                    # WARNING: I already call queue poking again because issue
                    #   about the every minute crontab.
                    partial_queue(q)
                    continue

                heappush(q.running, release)
                futures.append(
                    executor.submit(
                        self.release,
                        release=release,
                        params=params,
                        audit=audit,
                        queue=q,
                        parent_run_id=result.run_id,
                    )
                )

                partial_queue(q)

            # WARNING: This poking method does not allow to use fail-fast
            #   logic to catching parallel execution result.
            for future in as_completed(futures, timeout=timeout):
                context.append(future.result())

        return result.catch(
            status=SUCCESS,
            context={"outputs": context},
        )


@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class WorkflowTask:
    """Workflow task Pydantic dataclass object that use to keep mapping data and
    workflow model for passing to the multithreading task.

        This dataclass object is mapping 1-to-1 with workflow and cron runner
    objects.

        This dataclass has the release method for itself that prepare necessary
    arguments before passing to the parent release method.

    :param alias: (str) An alias name of Workflow model.
    :param workflow: (Workflow) A Workflow model instance.
    :param runner: (CronRunner)
    :param values: A value data that want to parameterize.
    :param extras: An extra parameter that use to override core config values.
    """

    alias: str
    workflow: Workflow
    runner: CronRunner
    values: DictData = field(default_factory=dict)
    extras: DictData = field(default_factory=dict)

    def release(
        self,
        release: Optional[Union[Release, datetime]] = None,
        run_id: Optional[str] = None,
        audit: type[Audit] = None,
        queue: Optional[ReleaseQueue] = None,
    ) -> Result:
        """Release the workflow task that passing an override parameter to
        the parent release method with the `values` field.

            This method can handler not passing release value by default
        generate step. It uses the `runner` field for generate release object.

        :param release: A release datetime or Release object.
        :param run_id: A workflow running ID for this release.
        :param audit: An audit class that want to save the execution result.
        :param queue: A ReleaseQueue object that use to mark complete.

        :raise ValueError: If a queue parameter does not pass while release
            is None.
        :raise TypeError: If a queue parameter does not match with ReleaseQueue
            type.

        :rtype: Result
        """
        audit: type[Audit] = audit or get_audit(extras=self.extras)

        if release is None:

            if queue is None:
                raise ValueError(
                    "If pass None release value, you should to pass the queue"
                    "for generate this release."
                )
            elif not isinstance(queue, ReleaseQueue):
                raise TypeError(
                    "The queue argument should be ReleaseQueue object only."
                )

            if queue.check_queue(self.runner.date):
                release = self.runner.next

                while queue.check_queue(release):
                    release = self.runner.next
            else:
                release = self.runner.date

        return self.workflow.release(
            release=release,
            params=self.values,
            run_id=run_id,
            audit=audit,
            queue=queue,
            override_log_name=self.alias,
        )

    def queue(
        self,
        end_date: datetime,
        queue: ReleaseQueue,
        audit: type[Audit],
        *,
        force_run: bool = False,
    ) -> ReleaseQueue:
        """Generate Release from the runner field and store it to the
        ReleaseQueue object.

        :param end_date: An end datetime object.
        :param queue: A workflow queue object.
        :param audit: An audit class that want to make audit object.
        :param force_run: (bool) A flag that allow to release workflow if the
            audit with that release was pointed.

        :rtype: ReleaseQueue
        """
        return queue.gen(
            end_date,
            audit,
            self.runner,
            self.alias,
            force_run=force_run,
            extras=self.extras,
        )

    def __repr__(self) -> str:
        """Override the `__repr__` method.

        :rtype: str
        """
        return (
            f"{self.__class__.__name__}(alias={self.alias!r}, "
            f"workflow={self.workflow.name!r}, runner={self.runner!r}, "
            f"values={self.values})"
        )

    def __eq__(self, other: WorkflowTask) -> bool:
        """Override the equal property that will compare only the same type.

        :rtype: bool
        """
        if isinstance(other, WorkflowTask):
            return (
                self.workflow.name == other.workflow.name
                and self.runner.cron == other.runner.cron
            )
        return NotImplemented
