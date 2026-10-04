FROM python:3.13-slim

WORKDIR /app

# deps first so edits to the app don't invalidate the install layer
COPY pyproject.toml uv.lock ./

RUN pip install --no-cache-dir uv \
 && uv sync --frozen --no-dev --no-install-project

COPY . .

RUN uv sync --frozen --no-dev

EXPOSE 8080

CMD ["uv", "run", "--no-dev", "python", "main.py"]
