import argparse
from collections.abc import Callable, Sequence

type Handler = Callable[[argparse.Namespace], int]
type SubparsersAction = argparse._SubParsersAction[argparse.ArgumentParser]

# Extension point: each task appends its `_add_<name>(sub)` function here.
# An `_add_<name>` registers a subparser and binds its handler with
# `set_defaults(handler=...)`.
_COMMANDS: list[Callable[[SubparsersAction], None]] = []


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
    import tomllib

    from pydantic import ValidationError

    from graph_agent._internal.config.settings import Settings
    from graph_agent._internal.runtime.validate import validate_manifest

    try:
        settings = Settings()
    except (tomllib.TOMLDecodeError, FileNotFoundError) as exc:  # one line, no traceback
        print(f"manifest: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    except ValidationError as exc:  # input is hidden: loc + msg never echo values
        for err in exc.errors(include_input=False, include_url=False, include_context=False):
            print(f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}", file=sys.stderr)
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
