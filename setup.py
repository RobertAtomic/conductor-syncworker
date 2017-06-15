"""
Setup for conductor-syncworker
"""

from setuptools import find_packages, setup

setup(
    name='conductor-syncworker',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[i.strip() for i in open("requirements.txt").readlines()],
    tests_require=[
        'nose',
        'coverage',
        'pylint',
        'git-pylint-commit-hook'],
    test_suite='nose.collector',
)
