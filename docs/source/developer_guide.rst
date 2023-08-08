Developer Guide
===============

Installation
------------

To install the necessary dependencies for building the documentation, running unit tests, and performing pre-commit linting, run the following command from the package root directory:

.. code-block::

    pipenv install --dev

Documentation
-------------
If changes are made to the package structure or additional modules are created, you can update the Sphinx source ``cisticola.*.rst`` files by running the following command from the ``docs/`` directory:

.. code-block::

    pipenv run make apidoc

Formatting
----------
Cisticola uses `black <https://github.com/psf/black>`_ and `isort <https://pycqa.github.io/isort/>`_ to format source code. These packages are configured in a `pre-commit <https://pre-commit.com/>`_ hook in the root directory (``.pre-commit-config.yaml``) to ensure that all commits are properly formatted, which avoids unnecessarily large diffs.