import argparse
import logging
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # annotations only: the heavy imports are lazy inside the handlers
    from uuid import UUID

    from ranvier._internal.config.settings import Settings
    from ranvier._internal.runtime.events import RunEvent
    from ranvier._internal.usage import ReportRow

type Handler = Callable[[argparse.Namespace], int]
type SubparsersAction = argparse._SubParsersAction[argparse.ArgumentParser]

# Extension point: each task appends its `_add_<name>(sub)` function here.
# An `_add_<name>` registers a subparser and binds its handler with
# `set_defaults(handler=...)`.
_COMMANDS: list[Callable[[SubparsersAction], None]] = []


def _load_settings() -> "Settings | None":
    """`Settings()` or None after printing why (one line per problem, never the input values)."""
    import sys
    import tomllib

    from pydantic import ValidationError

    from ranvier._internal.config.settings import Settings

    try:
        return Settings()
    except (tomllib.TOMLDecodeError, FileNotFoundError) as exc:  # one line, no traceback
        print(f"manifest: {type(exc).__name__}: {exc}", file=sys.stderr)
    except ValidationError as exc:  # input is hidden: loc + msg never echo values
        for err in exc.errors(include_input=False, include_url=False, include_context=False):
            print(f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}", file=sys.stderr)
    return None


logger = logging.getLogger(__name__)
LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def _configure_logging() -> None:
    """INFO to stderr for the long-running commands, unless the host already set logging up.

    Only entrypoints configure logging; library code just uses `logging.getLogger`.
    """
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)  # no-op when root has handlers


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ranvier")
    sub = parser.add_subparsers(dest="command", metavar="<command>", required=True)
    for add_command in _COMMANDS:
        add_command(sub)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    handler: Handler = args.handler
    return handler(args)


def _add_validate(sub: SubparsersAction) -> None:
    parser = sub.add_parser("validate", help="check ranvier.toml and compile every flow")
    parser.add_argument("--diagram", action="store_true", help="print a Mermaid diagram per flow")
    parser.add_argument(
        "--offline", action="store_true", help="fake models, no MCP, no provider env vars"
    )
    parser.set_defaults(handler=_validate)


def _validate(args: argparse.Namespace) -> int:
    # Lazy imports: langgraph and the settings stack are slow to load; other commands skip them.
    import asyncio
    import sys

    from ranvier._internal.runtime.validate import validate_manifest

    settings = _load_settings()
    if settings is None:
        return 1

    def show_diagram(flow: str, mermaid: str) -> None:
        print(f"flow: {flow}\n```mermaid\n{mermaid}\n```")

    problems = asyncio.run(
        validate_manifest(
            settings, offline=args.offline, on_diagram=show_diagram if args.diagram else None
        )
    )
    for problem in problems:
        print(problem, file=sys.stderr)
    if not problems:
        print(f"ok: {len(settings.flows)} flow(s), {len(settings.tools)} tool(s)")
    return 1 if problems else 0


_COMMANDS.append(_add_validate)


DEFAULT_SINCE = "7d"
# shortcut: literal copy of `usage.GROUP_BYS` (importing it loads langchain for every command);
# a unit test pins both to the same tuple.
USAGE_GROUP_BYS = ("flow", "model", "agent")
USAGE_POOL_SIZE = 1  # one query per invocation
_UNIT = {"h": timedelta(hours=1), "d": timedelta(days=1)}


def _parse_since(text: str) -> datetime:
    """`24h` / `7d` (relative to now) or an ISO date/datetime (naive means UTC)."""
    if len(text) > 1 and text[-1] in _UNIT and text[:-1].isdigit():
        return datetime.now(UTC) - int(text[:-1]) * _UNIT[text[-1]]
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{text!r}: use 24h, 7d or an ISO date") from None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


async def _fetch_usage(settings: "Settings", since: datetime, by: str) -> "list[ReportRow]":
    from ranvier._internal.db.pool import open_app_pool
    from ranvier._internal.usage import UsageRepository

    pool = await open_app_pool(settings.database.url.get_secret_value(), USAGE_POOL_SIZE)
    try:
        return await UsageRepository(pool).report_rows(since, by)
    finally:
        await pool.close()


def _usage(args: argparse.Namespace) -> int:
    import asyncio

    from ranvier._internal.usage import build_report, format_report

    settings = _load_settings()
    if settings is None:
        return 1
    rows = asyncio.run(_fetch_usage(settings, args.since, args.by))  # only the fetch needs a loop
    # A model name served by two providers is ambiguous: leave it unmapped (priced by name).
    names: dict[str, set[str]] = {}
    for profile in settings.llm.profiles.values():
        names.setdefault(profile.model, set()).add(profile.provider)
    providers = {model: next(iter(p)) for model, p in names.items() if len(p) == 1}
    print(format_report(build_report(rows, providers), args.by))
    return 0


def _add_usage(sub: SubparsersAction) -> None:
    p = sub.add_parser("usage", help="tokens and estimated cost from the usage ledger")
    p.add_argument(
        "--since", type=_parse_since, default=DEFAULT_SINCE
    )  # string: argparse parses it
    p.add_argument("--by", choices=USAGE_GROUP_BYS, default="flow")
    p.set_defaults(handler=_usage)


_COMMANDS.append(_add_usage)


# --- migrate, run -----------------------------------------------------------------------------

EXIT_USAGE = 2  # argparse's own code for bad arguments
_OK_END_STATUSES = frozenset({"success", "interrupted"})


def _add_migrate(sub: SubparsersAction) -> None:
    parser = sub.add_parser("migrate", help="apply the SQL migrations and set up the checkpointer")
    parser.set_defaults(handler=_migrate)


def _migrate(args: argparse.Namespace) -> int:
    import asyncio

    _configure_logging()
    settings = _load_settings()
    if settings is None:
        return 1
    asyncio.run(_migrate_database(settings))
    return 0


async def _migrate_database(settings: "Settings") -> None:
    from ranvier._internal.db.migrate import advisory_lock, apply_migrations_locked
    from ranvier._internal.db.pool import open_app_pool, open_checkpointer_pool
    from ranvier._internal.queue import apply_procrastinate_schema
    from ranvier._internal.runtime.checkpointer import setup_checkpointer

    db = settings.database
    url = db.url.get_secret_value()
    # One lock for the whole sequence: concurrent `migrate` runs cannot interleave steps.
    async with (
        await open_app_pool(url, db.app_pool_max) as app_pool,
        advisory_lock(app_pool) as conn,
    ):
        applied = await apply_migrations_locked(conn)
        print(f"migrations: applied {applied}" if applied else "migrations: up to date")
        # The saver needs the schema (applied above) because its pool pins `search_path`.
        async with await open_checkpointer_pool(url, db.checkpointer_pool_max) as cp_pool:
            await setup_checkpointer(cp_pool)
        print("checkpointer: ready")
        recorded = await apply_procrastinate_schema(conn)
        print(
            f"procrastinate: recorded {len(recorded)} file(s)"
            if recorded
            else "procrastinate: up to date"
        )


def _json_arg(text: str) -> object:
    import json

    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"invalid JSON: {exc}") from exc
    if value is None:  # JSON null counts as absent (design: XOR input/command)
        raise argparse.ArgumentTypeError("JSON null is not a valid value here")
    return value


def _add_run(sub: SubparsersAction) -> None:
    import uuid

    parser = sub.add_parser("run", help="run a flow inline and print its events as JSON lines")
    parser.add_argument("flow", help="flow name from ranvier.toml")
    what = parser.add_mutually_exclusive_group(required=True)
    what.add_argument("--input", type=_json_arg, metavar="JSON", help="input of a new run")
    what.add_argument(
        "--resume",
        type=_json_arg,
        metavar="JSON",
        help="resume value, a map {interrupt_id: value}; needs --thread and the database",
    )
    parser.add_argument("--thread", type=uuid.UUID, metavar="ID", help="thread id (default: new)")
    parser.add_argument(
        "--memory", action="store_true", help="InMemorySaver, no database; the thread dies with us"
    )
    parser.set_defaults(handler=_run)


async def _consume(events: "AsyncIterator[RunEvent]") -> int | None:
    """Emit events up to the first terminal one; return its exit code.

    None when no terminal event arrived.

    The caller closes the stream. An unserializable intermediate event stops consumption with
    exit 1 (`internal_error`); in database mode closing the stream cancels the run, so the CLI
    exits 1 while the run row ends `cancelled` (accepted: it needs a state the platform cannot
    serialize, design section 6).
    """
    async for event in events:
        if (done := _emit(event)) is not None:
            return done
    return None


def _emit(event: "RunEvent") -> int | None:
    """Print one event as a JSON line; the exit code once the event is terminal, else None."""
    import sys

    from pydantic_core import PydanticSerializationError, to_json

    from ranvier._internal.runtime.events import EndEvent, ErrorEvent
    from ranvier._internal.runtime.runs import ErrorCode

    name = type(event).__name__.removesuffix("Event").lower()  # same names as the SSE events
    try:
        line = to_json({"event": name, "data": event}, inf_nan_mode="null")
    except PydanticSerializationError as exc:  # state the platform cannot serialize (design 6)
        print(f"event {name!r} is not JSON-serializable: {exc}", file=sys.stderr)
        line = to_json(
            {
                "event": "error",
                "data": ErrorEvent(ErrorCode.INTERNAL_ERROR, "unserializable event", False),
            },
            inf_nan_mode="null",
        )
        print(line.decode(), flush=True)
        return 1
    print(line.decode(), flush=True)
    if isinstance(event, EndEvent):
        return 0 if event.status in _OK_END_STATUSES else 1
    return 1 if isinstance(event, ErrorEvent) else None


def _run(args: argparse.Namespace) -> int:
    import asyncio
    import sys
    import uuid

    if args.resume is not None and (args.memory or args.thread is None):
        print("--resume needs --thread and the database (no --memory)", file=sys.stderr)
        return EXIT_USAGE
    settings = _load_settings()
    if settings is None:
        return 1
    thread_id: uuid.UUID = args.thread or uuid.uuid4()
    from ranvier._internal.llm import LlmError

    try:
        if args.memory:
            return asyncio.run(_run_memory(settings, args.flow, args.input, thread_id))
        return asyncio.run(_run_database(settings, args.flow, args.input, args.resume, thread_id))
    except LlmError as exc:  # startup config error, like `validate`: one line, no traceback
        print(exc, file=sys.stderr)
        return 1


async def _run_memory(settings: "Settings", name: str, input: object, thread_id: "UUID") -> int:
    import asyncio
    import uuid
    from contextlib import aclosing

    from ranvier._internal.bootstrap import open_container
    from ranvier._internal.runtime.context import RunContext
    from ranvier._internal.runtime.events import ErrorEvent
    from ranvier._internal.runtime.runner import DEFAULT_STREAM_MODES, run_config, stream_graph
    from ranvier._internal.runtime.runs import ErrorCode, RunError

    async with open_container(settings, memory=True) as container:
        try:
            flow = await container.flows.get(name)
        except RunError as exc:
            _emit(ErrorEvent(exc.code.value, str(exc), exc.retryable))
            return 1
        run_id = str(uuid.uuid4())
        ctx = RunContext(run_id, str(thread_id), name, flow.version, 1, None)
        stream = stream_graph(
            flow,
            input,
            run_config(ctx, flow, {}),
            ctx,
            DEFAULT_STREAM_MODES,
            container.observers,
        )
        timeout = asyncio.timeout(flow.timeout_s)
        try:
            # stream_graph applies no timeout; the stream is closed here, in the consuming task.
            async with timeout, aclosing(stream):
                code = await _consume(stream)
        except Exception as exc:  # reported, never swallowed: type name only (may carry secrets)
            if timeout.expired():
                error = ErrorEvent(ErrorCode.RUN_TIMEOUT, f"run exceeded {flow.timeout_s}s", False)
            else:
                logging.getLogger(__name__).error("run %s failed", run_id, exc_info=exc)
                error = ErrorEvent(ErrorCode.INTERNAL_ERROR, type(exc).__name__, False)
            _emit(error)
            return 1
        return 1 if code is None else code  # an empty stream is not a success


async def _run_database(
    settings: "Settings", name: str, input: object, resume: object, thread_id: "UUID"
) -> int:
    import sys
    from contextlib import aclosing

    from ranvier._internal.bootstrap import open_container
    from ranvier._internal.runtime.events import ErrorEvent
    from ranvier._internal.runtime.runner import (
        cancel_pending_run,
        execute_run,
        prepare_thread_run,
    )
    from ranvier._internal.runtime.runs import RunError

    async with open_container(settings) as container:
        runs = container.runs
        if runs is None:
            raise RuntimeError("database mode opened a container without a runs repository")
        try:
            pin = await prepare_thread_run(
                container,
                runs,
                flow=name,
                thread_id=thread_id,
                tenant_id=None,
                input=input,
                resume=resume,
            )
            run = await runs.create(
                thread_id=thread_id,
                flow=name,
                flow_version=pin.flow_version,
                tenant_id=pin.tenant_id,
                input=input,
                command=None if resume is None else {"resume": resume},
            )
            code = None
            async with aclosing(execute_run(container, run.run_id)) as events:
                try:
                    # The claim (thread_busy) happens before the first event.
                    first = await anext(events, None)
                except BaseException:
                    # Claim refused, DB error, Ctrl-C: the row must not stay claimable (1z, 1q).
                    await cancel_pending_run(container, runs, run)
                    raise
                if first is not None:
                    code = _emit(first)
                    if code is None:
                        code = await _consume(events)
            if code is None:  # no terminal event: the claim or lease was lost
                print(f"run {run.run_id} lost its claim or lease", file=sys.stderr)
                return 1
            return code
        except RunError as exc:
            _emit(ErrorEvent(exc.code.value, str(exc), exc.retryable))
            return 1


_COMMANDS.append(_add_migrate)
_COMMANDS.append(_add_run)


def _serve(args: argparse.Namespace) -> int:
    from ranvier._internal.api.app import serve

    _configure_logging()
    settings = _load_settings()  # validates up front with readable errors
    if settings is None:
        return 1
    serve(settings)
    return 0


def _add_serve(sub: SubparsersAction) -> None:
    parser = sub.add_parser("serve", help="run the HTTP API (host and port: api.* settings)")
    parser.set_defaults(handler=_serve)


_COMMANDS.append(_add_serve)


# --- worker, crons -----------------------------------------------------------------------------


def _add_worker(sub: SubparsersAction) -> None:
    parser = sub.add_parser("worker", help="Procrastinate worker: execute, sweep, prune, crons")
    parser.set_defaults(handler=_worker)


def _worker(args: argparse.Namespace) -> int:
    import asyncio
    import sys

    from ranvier._internal.llm import LlmError

    _configure_logging()
    settings = _load_settings()
    if settings is None:
        return 1
    try:
        asyncio.run(_serve_jobs(settings))
    except LlmError as exc:  # startup config error, like `run`: one line, no traceback
        print(exc, file=sys.stderr)
        return 1
    return 0


async def _serve_jobs(settings: "Settings") -> None:
    import asyncio

    from ranvier._internal.bootstrap import open_container
    from ranvier._internal.worker import WORKER_EXTRA_CONNECTIONS, run_worker

    main = asyncio.current_task()
    assert main is not None
    worker: asyncio.Task[None] | None = None

    def cancel() -> None:  # the worker's cancel path is the graceful stop; before it exists, abort
        (worker or main).cancel()

    # The handlers outlive `open_container`: a signal during the close (pools, telemetry flush)
    # must not raise KeyboardInterrupt in the middle of it or kill the process with SIG_DFL.
    async with (
        _until_signalled(cancel),
        open_container(settings, extra_connections=WORKER_EXTRA_CONNECTIONS) as container,
    ):
        worker = asyncio.create_task(run_worker(container))
        await worker


@asynccontextmanager
async def _until_signalled(cancel: Callable[[], None]) -> AsyncIterator[None]:
    """Call `cancel` on the first SIGTERM/SIGINT and swallow the cancellation it causes.

    The block returns normally after a signal (a cancellation from outside still propagates).
    Ours instead of the interpreter's, so Ctrl+C is not a KeyboardInterrupt in the middle of the
    shutdown; a second signal is ignored (SIGKILL forces).
    """
    import asyncio
    import signal

    loop = asyncio.get_running_loop()
    signalled = False

    def on_signal(sig: signal.Signals) -> None:
        nonlocal signalled
        if signalled:
            logger.warning("already shutting down (%s ignored); SIGKILL forces", sig.name)
            return
        signalled = True
        logger.info("%s received: draining runs and shutting down", sig.name)
        cancel()

    handled = (signal.SIGINT, signal.SIGTERM)
    for sig in handled:
        loop.add_signal_handler(sig, on_signal, sig)
    try:
        yield
    except asyncio.CancelledError:
        if not signalled:  # cancelled from outside, not by us
            raise
    finally:
        for sig in handled:
            loop.remove_signal_handler(sig)


def _add_crons(sub: SubparsersAction) -> None:
    parser = sub.add_parser("crons", help="list the declared crons with their next fire time (UTC)")
    parser.set_defaults(handler=_crons)


def _crons(args: argparse.Namespace) -> int:
    from ranvier._internal.config.manifest import next_fire

    settings = _load_settings()
    if settings is None:
        return 1
    now = datetime.now(UTC)
    rows = [
        (flow, cron.schedule, next_fire(cron.schedule, now).strftime("%Y-%m-%d %H:%M:%S UTC"))
        for flow, spec in settings.flows.items()
        for cron in spec.crons
    ]
    if not rows:
        print("no crons declared")
        return 0
    widths = [max(len(col) for col in column) for column in zip(*rows, strict=True)]
    for row in rows:
        print(
            "  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)).rstrip()
        )
    return 0


_COMMANDS.append(_add_worker)
_COMMANDS.append(_add_crons)
