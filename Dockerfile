# =============================================================================
# Stage 1 — Build stage
# Install heavy build deps here, but do not keep them in final image.
# =============================================================================
FROM python:3.11-slim AS builder

# System build tools (needed only at build stage for dbt, meltano deps)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    gcc \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY requirements_docker.txt .

# Install all dependencies into a target folder (not global)
RUN pip install --no-cache-dir --prefix=/build/deps -r requirements_docker.txt


# =============================================================================
# Stage 2 — Final runtime image (minimal)
# Only copies python interpreter + pure python wheels + your code.
# =============================================================================
FROM python:3.11-slim

# Create a dedicated non-root user for security
RUN useradd -m appuser

WORKDIR /app

# Copy installed dependencies from builder stage
COPY --from=builder /build/deps /usr/local

# Copy project files
COPY . .

# Dagster runtime env
ENV DAGSTER_HOME=/app/orchestration
ENV PYTHONPATH=/app

# Ensure the appuser owns the home directory
RUN chown -R appuser:appuser /app

USER appuser

# Expose Dagster webserver port
EXPOSE 3000

# Default CMD — overridden by docker-compose
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "worksplace.yaml"]
