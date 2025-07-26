# Install all dependencies
install:
    uv sync --all-groups

# Run tests
test:
    uv run --group test pytest --cov=src --cov=tests --cov-report=xml

# Run type checking
typing:
    uv run --group typing --group test --isolated ty check

# Run linting and formatting
lint:
    uvx --with pre-commit-uv pre-commit run -a

# Run all checks (format, lint, typing, test)
check: lint typing test

# Run tests with lowest dependency resolution
test-lowest:
    uv run --group test --resolution lowest-direct pytest

# Run tests with highest dependency resolution
test-highest:
    uv run --group test --resolution highest pytest
