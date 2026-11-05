import argparse
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # annotations only: the heavy imports are lazy inside the handlers
    from graph_agent._internal.config.settings import Settings
    from graph_agent._internal.usage import ReportRow

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

    from graph_agent._internal.config.settings import Settings

    try:
        return Settings()
    except (tomllib.TOMLDecodeError, FileNotFoundError) as exc:  # one line, no traceback
        print(f"manifest: {type(exc).__name__}: {exc}", file=sys.stderr)
    except ValidationError as exc:  # input is hidden: loc + msg never echo values
        for err in exc.errors(include_input=False, include_url=False, include_context=False):
            print(f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}", file=sys.stderr)
    return None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="graph-agent")
    sub = parser.add_subparsers(dest="command", metavar="<command>", required=True)
    for add_command in _COMMANDS:
        add_command(sub)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    handler: Handler = args.handler
    return handler(args)


def _add_validate(sub: SubparsersAction) -> None:
    parser = sub.add_parser("validate", help="check graph-agent.toml and compile every flow")
    parser.add_argument("--diagram", action="store_true", help="print a Mermaid diagram per flow")
    parser.add_argument(
        "--offline", action="store_true", help="fake models, no MCP, no provider env vars"
    )
    parser.set_defaults(handler=_validate)


def _validate(args: argparse.Namespace) -> int:
    # Lazy imports: langgraph and the settings stack are slow to load; other commands skip them.
    import asyncio
    import sys

    from graph_agent._internal.runtime.validate import validate_manifest

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
    from graph_agent._internal.db.pool import open_app_pool
    from graph_agent._internal.usage import UsageRepository

    pool = await open_app_pool(settings.database.url.get_secret_value(), USAGE_POOL_SIZE)
    try:
        return await UsageRepository(pool).report_rows(since, by)
    finally:
        await pool.close()


def _usage(args: argparse.Namespace) -> int:
    import asyncio

    from graph_agent._internal.usage import build_report, format_report

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
