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
