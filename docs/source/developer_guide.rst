Developer Guide
===============

Installation
------------

To install the necessary dependencies for building the documentation and running unit tests, run the following command from the package root directory:

.. code-block::

    pipenv install --dev

Documentation
-------------
If changes are made to the package structure or additional modules are created, you can update the Sphinx source ``cisticola.*.rst`` files by running the following command from the ``docs/`` directory:

.. code-block::

    pipenv run make apidoc

Formatting
----------
Cisticola uses `black <https://github.com/psf/black>`_ to format source code.