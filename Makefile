# ------------------------
# Makefile for PulsePipe
# ------------------------

PROJECT_NAME=pulsepipe

# ------------------------
# Install dependencies
# ------------------------

install:
	poetry install

# ------------------------
# Format code
# ------------------------

format:
	poetry run black src/ tests/
	poetry run isort src/ tests/

# ------------------------
# Type checking
# ------------------------

typecheck:
	poetry run mypy src/ tests/

# ------------------------
# Run tests
# ------------------------

test:
	poetry run pytest --cov=src/ --cov-report=term-missing tests/

# ------------------------
# Lint (format + typecheck)
# ------------------------

lint: format typecheck

# ------------------------
# Clean Python artifacts
# ------------------------

clean:
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type d -name "*.egg-info" -exec rm -r {} +
	find . -type d -name ".pytest_cache" -exec rm -r {} +
	find . -name ".mypy_cache" -type d -exec rm -r {} +

# ------------------------
# Full check (format + typecheck + test)
# ------------------------

check: lint test
