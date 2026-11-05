"""Run every S3 check. No network, dummy keys only.

Run: uv run python -W ignore::DeprecationWarning bench/spikes/S3/run.py
"""

import asyncio

import agent_name
import http_clients

if __name__ == "__main__":
    asyncio.run(http_clients.main())
    asyncio.run(agent_name.main())
