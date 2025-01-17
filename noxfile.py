# -*- coding: utf-8 -*-
#
# Copyright 2020 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd


from __future__ import absolute_import

import os
import shutil

import nox

BLACK_VERSION = "black==19.10b0"
BLACK_PATHS = [
    "docs",
    "django_spanner",
    "tests",
    "noxfile.py",
    "setup.py",
]

DEFAULT_PYTHON_VERSION = "3.8"
SYSTEM_TEST_PYTHON_VERSIONS = ["3.8"]
UNIT_TEST_PYTHON_VERSIONS = ["3.6", "3.7", "3.8"]


@nox.session(python=DEFAULT_PYTHON_VERSION)
def lint(session):
    """Run linters.

    Returns a failure if the linters find linting errors or sufficiently
    serious code quality issues.
    """
    session.install("flake8", BLACK_VERSION)
    session.run("black", "--check", *BLACK_PATHS)
    session.run("flake8", "django_spanner", "tests")


@nox.session(python="3.6")
def blacken(session):
    """Run black.

    Format code to uniform standard.

    This currently uses Python 3.6 due to the automated Kokoro run of synthtool.
    That run uses an image that doesn't have 3.6 installed. Before updating this
    check the state of the `gcp_ubuntu_config` we use for that Kokoro run.
    """
    session.install(BLACK_VERSION)
    session.run("black", *BLACK_PATHS)


@nox.session(python=DEFAULT_PYTHON_VERSION)
def lint_setup_py(session):
    """Verify that setup.py is valid (including RST check)."""
    session.install("docutils", "pygments")
    session.run(
        "python", "setup.py", "check", "--restructuredtext", "--strict"
    )


def default(session):
    # Install all test dependencies, then install this package in-place.
    session.install(
        "django~=2.2", "mock", "mock-import", "pytest", "pytest-cov"
    )
    session.install("-e", ".")

    # Run py.test against the unit tests.
    session.run(
        "py.test",
        "--quiet",
        "--cov=django_spanner",
        "--cov=tests.unit",
        "--cov-append",
        "--cov-config=.coveragerc",
        "--cov-report=",
        "--cov-fail-under=20",
        os.path.join("tests", "unit"),
        *session.posargs
    )


@nox.session(python=DEFAULT_PYTHON_VERSION)
def docs(session):
    """Build the docs for this library."""

    session.install("-e", ".[tracing]")
    session.install("sphinx", "alabaster", "recommonmark")

    shutil.rmtree(os.path.join("docs", "_build"), ignore_errors=True)
    session.run(
        "sphinx-build",
        "-W",  # warnings as errors
        "-T",  # show full traceback on exception
        "-N",  # no colors
        "-b",
        "html",
        "-d",
        os.path.join("docs", "_build", "doctrees", ""),
        os.path.join("docs", ""),
        os.path.join("docs", "_build", "html", ""),
    )
