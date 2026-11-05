import logging
import signal

import pytest
import uvicorn
from fastapi import FastAPI

from graph_agent._internal.api.app import DrainingServer


def test_a_second_signal_is_ignored_not_a_force_exit(caplog: pytest.LogCaptureFixture) -> None:
    app = FastAPI()
    server = DrainingServer(uvicorn.Config(app), app)
    server.handle_exit(signal.SIGTERM, None)
    assert server.should_exit
    with caplog.at_level(logging.WARNING, logger="graph_agent._internal.api.app"):
        server.handle_exit(signal.SIGINT, None)  # upstream would set force_exit here
    assert not server.force_exit
    assert "SIGINT ignored" in caplog.text
