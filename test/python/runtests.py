"""
This runs the unit test suite for all Pegasus Python libraries
"""
import os
import sys
import unittest

def main():
    import test_dax3
    import test_utils

    modules = [
        test_dax3,
        test_utils
    ]

    loader = unittest.TestLoader()

    suites = [loader.loadTestsFromModule(m) for m in modules]

    alltests = unittest.TestSuite(suites)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(alltests)

    if result.wasSuccessful():
        return 0
    return 1

if __name__ == '__main__':
    sys.exit(main())

