import logging

import pytest

from graph_agent._internal import cli


def test_logging_defaults_to_info_when_root_has_no_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [])  # restored (with the new handler dropped) on exit
    monkeypatch.setattr(root, "level", logging.WARNING)
    cli._configure_logging()
    assert root.level == logging.INFO
    assert len(root.handlers) == 1


def test_logging_is_left_alone_when_the_host_configured_it(monkeypatch: pytest.MonkeyPatch) -> None:
    root = logging.getLogger()
    handler = logging.NullHandler()
    monkeypatch.setattr(root, "handlers", [handler])
    monkeypatch.setattr(root, "level", logging.WARNING)
    cli._configure_logging()
    assert root.level == logging.WARNING
    assert root.handlers == [handler]
