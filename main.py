from __future__ import annotations

import uvicorn

import app.settings


def main() -> int:
    uvicorn.run(
        "app:asgi_app",
        reload=app.settings.DEBUG,
        server_header=False,
        date_header=False,
        host=app.settings.HOST,
        port=app.settings.PORT,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
