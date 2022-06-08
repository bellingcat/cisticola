#!/bin/bash

pipenv run python -m spacy download xx_ent_wiki_sm
pipenv run python -m spacy download fr_core_news_sm
pipenv run python -m spacy download de_core_news_sm
pipenv run python -m spacy download nl_core_news_sm
pipenv run python -m spacy download it_core_news_sm
pipenv run python -m spacy download ru_core_news_sm
pipenv run python -m spacy download en_core_web_sm