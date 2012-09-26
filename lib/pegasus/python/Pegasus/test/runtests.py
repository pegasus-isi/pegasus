"""
This runs the unit test suite for all Pegasus Python libraries
"""
import os
import sys
import unittest

# Set the Python path so that we can run this from a source checkout
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
LIB_DIR = os.path.abspath(os.path.join(TEST_DIR, "..", ".."))
sys.path.insert(0, LIB_DIR)

def main():
    from Pegasus.test import test_dax3
    from Pegasus.test import test_utils
    
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
    exit(main())
