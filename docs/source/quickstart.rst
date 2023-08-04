Quickstart
==========

Installation
------------

The *cisticola* application uses pipenv_ for dependency management. To install the dependencies of *cisticola*, first install pipenv using the following command:

.. code-block::

    pip install pipenv

and then install the dependencies using the following command from the package root directory:

.. code-block::

    pipenv install

Environment Variables
---------------------

One of the scrapers in *cisticola* (:py:mod:`~cisticola.scraper.telegram_telethon.TelegramTelethonScraper`) requires platform credentials to work correctly. 

Telegram Telethon
"""""""""""""""""

The Telegram credentials can be configured by setting the following environment variables, either in the project's ``.env`` file or in the system's environment:

- ``TELEGRAM_API_ID``: API ID number for your Telegram application
- ``TELEGRAM_API_HASH``: API hash for your Telegram application
- ``TELEGRAM_PHONE``: phone number for the account corresponding to your your Telegram application

If you do not already have a Telegram application, you can create one by following the instructions on `this page`_.

To initialize a Telegram session, run the following script from the package's root directory using the command-line:

.. bash::

    bash telethon_session_init.py

Documentation
-------------

The *cisticola* application uses Sphinx_ to generate and display its documentation. To build the documentation in the HTML format, run the following command from the ``docs/`` directory:

.. code-block::

    pipenv run make html

For developers, if changes are made to the package structure or additional modules are created, you can update the Sphinx source ``*.rst`` files by running the following command from the ``docs/`` directory:

.. code-block::

    pipenv run make apidoc

Testing
-------

The *cisticola* application uses pytest_ for unit testing. To run the full test suite, run the following command from the package root directory:

.. code-block::

    pipenv run pytest

To see the logging output from a test run, add the ``--capture=no`` flag to the command. 

Examples
--------

The script ``app.py`` is included in the package root directory, showing how the list of channels to scrape is defined, and how the :py:mod:`~cisticola.scraper.base.ScraperController` and :py:mod:`~cisticola.transformer.base.Transformer` classes are used.

.. _pipenv: https://pipenv.pypa.io/en/latest/
.. _Sphinx: https://www.sphinx-doc.org/en/master/
.. _pytest: https://docs.pytest.org/en/7.1.x/
.. _this page: https://core.telegram.org/api/obtaining_api_id