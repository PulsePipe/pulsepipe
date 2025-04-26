# Windows Testing Guide for PulsePipe

This document provides guidance for running tests on Windows platforms, as there are some platform-specific considerations.

## Known Windows Testing Issues

Windows has different file handling semantics than Unix-like systems, which can cause issues with:
1. Path normalization (backslash vs forward slash)
2. File locking
3. File handle management, especially during test teardown

The most common error seen is: `ValueError: I/O operation on closed file`

## How We Handle Windows Tests

### The Windows Mock Module

We've implemented a Windows-specific mock module in `tests/windows_mock.py` that:
1. Creates an in-memory virtual file system
2. Normalizes all paths to use forward slashes
3. Prevents I/O operations on closed files
4. Handles special test cases

This module is automatically loaded by `conftest.py` when tests are run on Windows.

### Test Decorators

For tests that have Windows-specific issues, we provide decorators in `tests/mock_decorators.py`:

```python
from tests.mock_decorators import windows_safe_test, windows_skip_test

@windows_safe_test
def test_that_has_file_handling():
    # This test will be patched on Windows to use safe file operations
    
@windows_skip_test("Reason for skipping")
def test_to_skip_on_windows():
    # This test will be skipped on Windows
```

## Troubleshooting

If you encounter `I/O operation on closed file` errors:

1. Add the `@windows_safe_test` decorator to the failing test method

2. If that doesn't resolve the issue, you may need to add mocks:
   ```python
   # In your test file:
   from unittest.mock import patch
   
   # Mock file handlers
   with patch('logging.FileHandler', MockFileHandler):
       # Your test code here
   ```

3. For tests that can't be easily fixed, use `@windows_skip_test`

## Adding New Tests

When adding new tests that involve file operations:

1. Use the `windows_safe_test` decorator on any method that does file I/O
2. Use unittest mocks instead of actual file operations when possible
3. Normalize paths with `os.path.join` and use forward slashes for paths in test fixtures

## How It Works

The Windows mock module patches:
- `builtins.open`
- File operations from `os.path`
- `logging.FileHandler`
- `tempfile` functions
- `pathlib.Path` methods

These patches redirect file operations to an in-memory mock filesystem, which prevents the I/O errors common on Windows.