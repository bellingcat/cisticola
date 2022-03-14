import pytest

from sqlalchemy import create_engine

from cisticola.scraper import ScraperController
from cisticola.transformer import ETLController

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

BITCHUTE_CHANNEL_KWARGS = {
    'id': 0,
    'name': 'bestonlinejewelrystoresusa@gmail.com (test)',
    'platform_id': 'bestonlinejewelrystoresusagmailcom',
    'category': 'test',
    'followers': None,
    'platform': 'Bitchute',
    'url': 'https://www.bitchute.com/channel/bestonlinejewelrystoresusagmailcom/',
    'screenname': None,
    'country': 'US',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': ''}

GAB_CHANNEL_KWARGS = {
    'id': 1,
    'name': 'Capt. Marc Simon (test)',
    'platform_id': 'marc_capt',
    'category': 'test',
    'followers': None,
    'platform': 'Gab',
    'url': 'https://gab.com/marc_capt',
    'screenname': 'marc_capt',
    'country': 'CA',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': ''}

GETTR_CHANNEL_KWARGS = {
    'id': 2,
    'name': 'LizardRepublic (test)',
    'platform_id': 'lizardrepublic',
    'category': 'test',
    'followers': None,
    'platform': 'Gettr',
    'url': 'https://www.gettr.com/user/lizardrepublic',
    'screenname': 'lizardrepublic',
    'country': 'US',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': ''}

ODYSEE_CHANNEL_KWARGS = {
    'id': 3,
    'name': "Mak1n' Bacon (test)",
    'platform_id': 'Mak1nBacon',
    'category': 'test',
    'followers': None,
    'platform': 'Odysee',
    'url': 'https://odysee.com/@Mak1nBacon',
    'screenname': 'Mak1nBacon',
    'country': 'US',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': ''}

RUMBLE_CHANNEL_KWARGS = {
    'id': 4,
    'name': 'we are uploading videos wow products',
    'platform_id': 'c-916305',
    'category': 'test',
    'followers': None,
    'platform': 'Rumble',
    'url': 'https://rumble.com/c/c-916305',
    'screenname': 'we are uploading',
    'country': 'CA',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': ''}

TELEGRAM_CHANNEL_KWARGS = {
    'id': 5,
    'name': 'South West Ohio Proud Boys (test)',
    'platform_id': -1001276612436,
    'category': 'test',
    'followers': None,
    'platform': 'Telegram',
    'url': 'https://t.me/SouthwestOhioPB',
    'screenname': 'SouthwestOhioPB',
    'country': 'US',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': ''}

TWITTER_CHANNEL_KWARGS = {
    'id': 5,
    'name': 'L Weber (test)',
    'platform_id': 1424979017749442595,
    'category': 'test',
    'followers': None,
    'platform': 'Twitter',
    'url': 'https://twitter.com/LWeber33662141',
    'screenname': 'LWeber33662141',
    'country': 'US',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': ''}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#


@pytest.fixture(scope='package')
def engine(tmpdir_factory):
    """Initialize a SQLite database and SQLAlchemy engine to be used for all
    tests in the package"""

    file = tmpdir_factory.mktemp('test_data').join('test.db')
    engine = create_engine(f'sqlite:///{file}')
    
    return engine


@pytest.fixture(scope='package')
def controller(engine):
    """Initialize ScraperController to be used for all tests in the package."""

    scraper_controller = ScraperController()
    scraper_controller.connect_to_db(engine)

    return scraper_controller

@pytest.fixture(scope='package')
def etl_controller(engine):
    """Initialize ETLController to be used for all tests in the package."""

    etl_controller = ETLController()
    etl_controller.connect_to_db(engine)

    return etl_controller

@pytest.fixture(scope='package')
def channel_kwargs():
    """Define keyword arguments to use for defining test channels for each 
    platform to be scraped.
    """

    return {
        'bitchute': BITCHUTE_CHANNEL_KWARGS,
        'gab': GAB_CHANNEL_KWARGS,
        'gettr': GETTR_CHANNEL_KWARGS,
        'odysee': ODYSEE_CHANNEL_KWARGS,
        'rumble': RUMBLE_CHANNEL_KWARGS,
        'telegram': TELEGRAM_CHANNEL_KWARGS,
        'twitter': TWITTER_CHANNEL_KWARGS}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
