# Windows Testing Support

## Overview

This directory contains special support for running tests on Windows environments, where file handling can be problematic due to file locking, path separators, and file descriptor leaks.

## Windows Mock Module

The `windows_mock.py` module provides a comprehensive solution for Windows-specific file operation issues during tests:

- Mocks file I/O operations to use a virtual in-memory file system
- Normalizes path separators to always use forward slashes (Unix-style)
- Prevents "I/O operation on closed file" errors by not writing to disk
- Intercepts logging operations to prevent file handle issues

## How It Works

The mock module replaces these key functions when running on Windows during tests:

- `open()` - Uses an in-memory file representation
- `os.path.*` - Path manipulation functions normalized to use forward slashes
- `os.makedirs()`, `os.listdir()`, `os.remove()` - File system operations
- `logging.FileHandler` - Prevents actual file writes for logs

## Usage

The module is automatically loaded by `conftest.py` when tests are run on Windows. You don't need to do anything special to use it - it activates automatically when:

1. The platform is Windows (`sys.platform == 'win32'`)
2. Tests are running (detected by checking for `PYTEST_CURRENT_TEST` environment variable)

## Supported Test Patterns

The mock handles these common Windows test issues:

1. **Path Normalization**: Automatically converts backslashes to forward slashes
2. **File Locking**: Prevents issues with files being locked between tests
3. **Closed File Errors**: Prevents "I/O operation on closed file" errors
4. **Directory Creation**: Safely "creates" directories without touching the disk

## For Test Writers

When writing tests that will run on Windows:

- Test fixtures (files in the `/fixtures` directory) will still be read from disk normally
- Other file operations will use the virtual file system
- Path operations will be normalized automatically
- Use forward slashes in path strings for consistency

## Debugging

If you encounter issues with Windows tests while using this module:

1. Check your test doesn't rely on actual file system side effects
2. Ensure you're not depending on raw file paths matching exactly between platforms
3. Remember that log files won't actually be written to disk

## When to Avoid

For some rare cases, you might need to bypass the mock temporarily. You can use:

```python
from tests.windows_mock import disable_windows_mocks, enable_windows_mocks

# Temporarily disable mocks
disable_windows_mocks()
try:
    # Do operations that need the real file system
    ...
finally:
    # Re-enable mocks
    enable_windows_mocks()
```