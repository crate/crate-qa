import unittest


def suite():
    """
    To be executed with `python -m unittest` from same directory as this file.
    """
    return unittest.TestLoader().discover('./tests', pattern='test_*.py')


if __name__ == '__main__':
    """
    To be executed from anywhere using `python path/to/tests.py`.
    """
    unittest.TextTestRunner(verbosity=2).run(suite())
