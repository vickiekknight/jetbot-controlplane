"""
Entry point for running the cloud service as a long-lived process.

Usage:
    python -m cloud_service                 # default: bind 0.0.0.0:8000
    python -m cloud_service --port 8080
    python -m cloud_service --public-url http://cloud.internal:8000

The --public-url flag controls the WebSocket URLs that the cloud returns
to robots and users in API responses. If you bind to 0.0.0.0:8000 but
your robots reach the cloud via http://cloud.internal:8000, set
--public-url accordingly so the WebSocket URLs are reachable.
"""

from __future__ import annotations

import argparse

import uvicorn

from cloud_service.app import create_app
from common.logging import configure_logging


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the cloud service")
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Interface to bind on (default: 0.0.0.0 — all interfaces)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind on (default: 8000)",
    )
    parser.add_argument(
        "--public-url",
        default=None,
        help=(
            "Public URL for this cloud service, used to construct "
            "WebSocket URLs returned in API responses. Defaults to "
            "http://<host>:<port> if --host is concrete, else "
            "http://localhost:<port>."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    configure_logging(args.log_level)

    # If the user bound to 0.0.0.0 (all interfaces), localhost is the
    # right default for the advertised public URL — that's what clients
    # on the same host will use.
    if args.public_url is None:
        host_for_url = "localhost" if args.host == "0.0.0.0" else args.host
        public_url = f"http://{host_for_url}:{args.port}"
    else:
        public_url = args.public_url

    app = create_app(public_url=public_url)
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level.lower(),
        # wsproto avoids deprecation warnings from uvicorn's `websockets`
        # backend when running against websockets >= 14.0.
        ws="wsproto",
    )


if __name__ == "__main__":
    main()