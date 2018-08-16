#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from setuptools import setup, find_packages
import emdfparse


entry_points = {
    "console_scripts": [
        "emdfparse = emdfparse.cli:main",
    ]
}

with open("requirements.txt") as f:
    requires = [l for l in f.read().splitlines() if l]

readme = "README.md"
if os.path.exists("README.rst"):
    readme = "README.rst"
with open(readme) as f:
    long_description = f.read()


setup(
    name="emdfparse",
    version=emdfparse.__version__,
    author="lostsummer",
    author_email="lostsummer@gmail.com",
    description="emoney data file parser",
    long_description=long_description,
    packages=find_packages(),
    include_package_data=True,
    install_requires=requires,
    entry_points=entry_points,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)
