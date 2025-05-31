"""
Run all tests in the project.
This script is designed to be run from the command line.
"""

import unittest
import sys
import logging

logging.disable(logging.CRITICAL)  # Disable logging for test runs


def run_all_tests():
    """
    Run all tests in the project.

    Discovers and runs all test cases in the 'test' directory.

    Returns:
        int: Exit code - 0 if all tests passed, 1 otherwise.
    """
    test_loader = unittest.defaultTestLoader

    # Discover all tests in the test directory
    test_suite = test_loader.discover("test", pattern="test_*.py")

    # Create a test runner that will output results
    test_runner = unittest.TextTestRunner(verbosity=1)

    # Run the tests
    result = test_runner.run(test_suite)

    # Return appropriate exit code
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(run_all_tests())
