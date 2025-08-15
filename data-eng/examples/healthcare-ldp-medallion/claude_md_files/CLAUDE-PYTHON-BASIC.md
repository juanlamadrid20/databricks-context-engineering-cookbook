# CLAUDE-PYTHON-BASIC.md

This file provides comprehensive guidance to Claude Code when working with Python code in this repository.

## Core Development Philosophy

### KISS (Keep It Simple, Stupid)

Simplicity should be a key goal in design. Choose straightforward solutions over complex ones whenever possible. Simple solutions are easier to understand, maintain, and debug.

### YAGNI (You Aren't Gonna Need It)

Avoid building functionality on speculation. Implement features only when they are needed, not when you anticipate they might be useful in the future.

### Design Principles

- **Dependency Inversion**: High-level modules should not depend on low-level modules. Both should depend on abstractions.
- **Open/Closed Principle**: Software entities should be open for extension but closed for modification.
- **Single Responsibility**: Each function, class, and module should have one clear purpose.
- **Fail Fast**: Check for potential errors early and raise exceptions immediately when issues occur.

### File and Function Limits

- **Never create a file longer than 500 lines of code**. If approaching this limit, refactor by splitting into modules.
- **Functions should be under 50 lines** with a single, clear responsibility.
- **Classes should be under 100 lines** and represent a single concept or entity.
- **Organize code into clearly separated modules**, grouped by feature or responsibility.
- **Line lenght should be max 100 characters** ruff rule in pyproject.toml

### Design Principles
- Apply the DRY principle - Don't Repeat Yourself
- Prefer composition over inheritance for more maintainable code
- Write pure functions when possible (no side effects, same output for same input)
- Follow SOLID principles for maintainable object-oriented design
- Write tests first (TDD) or alongside code development.
- Use dataclasses for data containers

### Handling Complexity

- Hide implementation details behind clean interfaces
- Create abstractions that eliminate complexity for users
- Encapsulate related data and behavior in cohesive classes
- Use interfaces or abstract base classes to define contracts, but only when necessary!
- Apply dependency injection for more flexible and testable code
- Favor simple solutions over complex or clever ones
- Design for the most common use case first
- Keep component coupling loose through well-defined interfaces

### Error Handling

- Handle exceptions properly with specific exception types
- Avoid bare except statements that hide errors
- Validate inputs early to catch errors at the boundary
- Raise exceptions instead of returning error codes

### Efficient Python

- Use list/dict comprehensions over explicit for loops when appropriate
- Use f-strings
- Leverage built-in functions (map, filter, zip) for data manipulation
- Use context managers (with statement) for resource management
- Default to immutable types where possible (tuples, frozensets)
- Avoid mutable default arguments in function parameters
- Use generator expressions for large datasets to conserve memory

### Code Style

- Use snake_case for variables, functions, methods, and modules
- Use CamelCase for class names
- Use UPPER_CASE for constants
- Keep lines short (79-88 characters maximum)
- Always use type hints in functions.
- Use descriptive names that reveal intent, avoiding abbreviations
- Return early to reduce nesting and improve readability
- Keep functions small with a single responsibility
- Use docstrings (Google style) to document code
- Add blank lines to separate logical sections
- Follow Zen of Python principles.

### Testing

- **Class-based test organization**: Use `unittest.TestCase` classes to group related tests logically, leverage `setUp()` and `tearDown()` methods for consistent test initialization and cleanup, and organize test methods within classes by functionality or component being tested.
- **Comprehensive mocking with proper cleanup**: Use `unittest.mock.patch` judiciously - consult with user on what to mock and prefer mocking only external APIs where necessary (avoid extensive mocking). Organize patches in `setUp()` with systematic cleanup in `tearDown()`. Store patches in lists for batch management and always stop patches to prevent test interference.
- **Isolated test environments with temporary resources**: Create temporary directories/files using `tempfile` for file system operations, mock external dependencies (APIs, databases, file systems), and ensure complete cleanup of temporary resources in `tearDown()` methods.
- **Descriptive test methods with clear scenarios**: Name test methods with descriptive patterns like `test_<scenario>_<expected_outcome>` (e.g., `test_execution_with_system`, `test_malformed_json`), include comprehensive docstrings explaining the test purpose, and test both success and failure paths with edge cases.
- **Reusable helper methods and assertion patterns**: Create helper methods for common test setup (e.g., `create_test_log_file()`, `_test_read_token()`) that can be reused across multiple test methods, use specific assertions to verify exact behavior (`assert_called_once_with()`, `assertEqual()` for specific values), and verify both direct outputs and side effects (mock calls, file states, error conditions).


## 🛠️ Development Environment

### UV Package Management

This project uses UV for blazing-fast Python package and environment management.

```bash
# Install UV (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment
uv venv

# Sync dependencies
uv sync

# Add a package ***NEVER UPDATE A DEPENDENCY DIRECTLY IN PYPROJECT.toml***
# ALWAYS USE UV ADD
uv add requests

# Add development dependency
uv add --dev pytest ruff mypy

# Remove a package
uv remove requests

# Run commands in the environment
uv run python script.py
uv run pytest
uv run ruff check .

# Install specific Python version
uv python install 3.12
```

### Development Commands

```bash
# Run all tests
uv run pytest

# Run specific tests with verbose output
uv run pytest tests/test_module.py -v

# Run tests with coverage
uv run pytest --cov=src --cov-report=html

# Format code
uv run ruff format .

# Check linting
uv run ruff check .

# Fix linting issues automatically
uv run ruff check --fix .

# Type checking
uv run mypy src/

# Run pre-commit hooks
uv run pre-commit run --all-files
```
