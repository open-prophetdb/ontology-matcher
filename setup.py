#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open("README.md") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = ["Click>=7.0", "pandas"]

test_requirements = []

setup(
    author="Jingcheng Yang",
    author_email="yjcyxky@163.com",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="It's a simple ontology matcher for building a set of cleaned ontologies. These ontologies will be used for building a knowledge graph.",
    entry_points={
        "console_scripts": [
            "onto-match=ontology.cli:cli",
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="Ontology Matcher",
    name="ontology-matcher",
    packages=find_packages(include=["ontology", "ontology.*"]),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/yjcyxky/ontology-matcher",
    version="0.1.0",
    zip_safe=False,
)
