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

To install the necessary dependencies for building the documentation and running unit tests, run the following command from the package root directory:

.. code-block::

    pipenv install --dev

Environment Variables
---------------------

Three of the scrapers in *cisticola* (:py:mod:`~cisticola.scraper.gab.GabScraper`,  :py:mod:`~cisticola.scraper.instagram.InstagramScraper`, and :py:mod:`~cisticola.scraper.telegram_telethon.TelegramTelethonScraper`) require platform credentials to work correctly. 

Gab
"""

The Gab credentials can be configured by running the following command from the root directory:

.. code-block::

    pipenv run garc configure 

which will direct you to provide the username and password for your Gab account.

Instagram
"""""""""

The Instagram credentials can be configured by setting the following environment variables, either in the project's ``.env`` file or in the system's environment:

- ``INSTAGRAM_USERNAME``: username of your Instagram account
- ``INSTAGRAM_PASSWORD``: password of your Instagram account

Telegram Telethon
"""""""""""""""""

The Telegram credentials can be configured by setting the following environment variables, either in the project's ``.env`` file or in the system's environment:

- ``TELEGRAM_API_ID``: API ID number for your Telegram application
- ``TELEGRAM_API_HASH``: API hash for your Telegram application
- ``TELEGRAM_PHONE``: phone number for the account corresponding to your your Telegram application

If you do not already have a Telegram application, you can create one by following the instructions on `this page`_.

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

An example of a *cisticola* ingest file ``russian_telegram_ingest.py`` is included in the package root directory, showing how the list of channels to scrape is defined, and how the :py:mod:`~cisticola.scraper.base.ScraperController` and :py:mod:`~cisticola.transformer.base.Transformer` classes are used. To run the ingest script, run the following command from the package root directory:

.. code-block::

    pipenv run python russian_telegram_ingest.py

.. _pipenv: https://pipenv.pypa.io/en/latest/
.. _Sphinx: https://www.sphinx-doc.org/en/master/
.. _pytest: https://docs.pytest.org/en/7.1.x/
.. _this page: https://core.telegram.org/api/obtaining_api_id