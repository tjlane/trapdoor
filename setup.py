#coding: utf8

"""
Setup script for trapdoor.
"""

from glob import glob


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(name='trapdoor',
      version='0.0.1',
      author="TJ Lane",
      author_email="tjlane@stanford.edu",
      description='Detector protection, hit rate monitoring',
      packages=["trapdoor"],
      package_dir={"trapdoor": "trapdoor"},
      scripts=[s for s in glob('scripts/*') if not s.endswith('__.py')],
      test_suite="test")
