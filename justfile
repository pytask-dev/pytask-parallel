# Install all dependencies
install:
    uv sync --all-groups

# Run tests
test *args="":
    uv run --group test pytest --cov=src --cov=tests --cov-report=xml {{args}}

# Run tests with lowest dependency resolution
test-lowest *args="":
    uv run --group test --resolution lowest-direct pytest {{args}}

# Run tests with highest dependency resolution
test-highest *args="":
    uv run --group test --resolution highest pytest {{args}}

# Run type checking
typing:
    uv run --group typing --group test --isolated ty check

# Run linting and formatting
lint:
    uvx prek run -a

# Build documentation
docs:
    uv run --group docs sphinx-build docs/source docs/build

# Serve documentation with auto-reload
docs-serve:
    uv run --group docs sphinx-autobuild docs/source docs/build

# Run all checks (format, lint, typing, test)
check: lint typing test
