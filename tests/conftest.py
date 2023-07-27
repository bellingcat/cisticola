import os
import pytest

from sqlalchemy import create_engine

from cisticola.scraper import ScraperController
from cisticola.transformer import ETLController

BITCHUTE_CHANNEL_KWARGS = {
    'name': 'bestonlinejewelrystoresusa@gmail.com (test)',
    'platform_id': 'bestonlinejewelrystoresusagmailcom',
    'category': 'test',
    'platform': 'Bitchute',
    'url': 'https://www.bitchute.com/channel/bestonlinejewelrystoresusagmailcom/',
    'screenname': None,
    'country': 'US',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': '',
    'source': 'researcher'}

GAB_CHANNEL_KWARGS = {
    'name': 'Capt. Marc Simon (test)',
    'platform_id': 'marc_capt',
    'category': 'test',
    'platform': 'Gab',
    'url': 'https://gab.com/marc_capt',
    'screenname': 'marc_capt',
    'country': 'CA',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': '',
    'source': 'researcher'}

GAB_GROUP_KWARGS = {
    'name': 'iran group (test)',
    'platform_id': "10001",
    'category': 'test',
    'platform': 'Gab',
    'url': 'https://gab.com/groups/10001',
    'screenname': 'iran group',
    'country': 'IR',
    'influencer': None,
    'public': True,
    'chat': True,
    'notes': '',
    'source': 'researcher'}

GETTR_CHANNEL_KWARGS = {
    'name': 'LizardRepublic (test)',
    'platform_id': 'lizardrepublic',
    'category': 'test',
    'platform': 'Gettr',
    'url': 'https://www.gettr.com/user/lizardrepublic',
    'screenname': 'lizardrepublic',
    'country': 'US',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': '',
    'source': 'researcher'}

INSTAGRAM_CHANNEL_KWARGS = {
    'name': 'borland.88 (test)',
    'platform_id': 'borland.88',
    'category': 'test',
    'platform': 'Instagram',
    'url': 'https://www.instagram.com/borland.88/',
    'screenname': 'borland.88',
    'country': 'UA',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': '',
    'source': 'researcher'}

ODYSEE_CHANNEL_KWARGS = {
    'name': "Mak1n' Bacon (test)",
    'platform_id': 'Mak1nBacon',
    'category': 'test',
    'platform': 'Odysee',
    'url': 'https://odysee.com/@Mak1nBacon',
    'screenname': 'Mak1nBacon',
    'country': 'US',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': '',
    'source': 'researcher'}

RUMBLE_CHANNEL_KWARGS = {
    'name': 'we are uploading videos wow products (test)',
    'platform_id': 'c-916305',
    'category': 'test',
    'platform': 'Rumble',
    'url': 'https://rumble.com/c/c-916305',
    'screenname': 'we are uploading',
    'country': 'CA',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': '',
    'source': 'researcher'}

TELEGRAM_CHANNEL_KWARGS = {
    'name': 'Star Game (test)',
    'platform_id': "-1001866374682",
    'category': 'test',
    'platform': 'Telegram',
    'url': 'https://t.me/stargameinfo',
    'screenname': 'stargameinfo',
    'country': 'RU',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': '',
    'source': 'researcher'}
    
TWITTER_CHANNEL_KWARGS = {
    'name': 'L Weber (test)',
    'platform_id': "1424979017749442595",
    'category': 'test',
    'platform': 'Twitter',
    'url': 'https://twitter.com/LWeber33662141',
    'screenname': 'LWeber33662141',
    'country': 'US',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': '',
    'source': 'researcher'}

VKONTAKTE_CHANNEL_KWARGS = {
    'name': 'Wwg1wgA (test)',
    'platform_id': 'club201278078',
    'category': 'test',
    'platform': 'Vkontakte',
    'url': 'https://vk.com/club201278078',
    'screenname': 'Wwg1wgA',
    'country': 'FR',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': '',
    'source': 'researcher'}

YOUTUBE_CHANNEL_KWARGS = {
    'name': 'AnEs87 (test)',
    'platform_id': 'UCP6exBqGoxGLv_pM9Dxk2pA',
    'category': 'test',
    'platform': 'Youtube',
    'url': 'https://www.youtube.com/channel/UCP6exBqGoxGLv_pM9Dxk2pA',
    'screenname': 'AnEs87',
    'country': 'SV',
    'influencer': None,
    'public': True,
    'chat': False,
    'notes': '',
    'source': 'researcher'}


@pytest.fixture(scope='package')
def engine(tmpdir_factory):
    """Initialize a SQLite database and SQLAlchemy engine to be used for all
    tests in the package"""

    engine = create_engine(os.environ["TEST_DB"])
    
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
        'bitchute' : BITCHUTE_CHANNEL_KWARGS,
        'gab' : GAB_CHANNEL_KWARGS,
        'gab_group' : GAB_GROUP_KWARGS,
        'gettr' : GETTR_CHANNEL_KWARGS,
        'instagram' : INSTAGRAM_CHANNEL_KWARGS,
        'odysee' : ODYSEE_CHANNEL_KWARGS,
        'rumble' : RUMBLE_CHANNEL_KWARGS,
        'telegram' : TELEGRAM_CHANNEL_KWARGS,
        'twitter' : TWITTER_CHANNEL_KWARGS,
        'vkontakte' : VKONTAKTE_CHANNEL_KWARGS,
        'youtube' : YOUTUBE_CHANNEL_KWARGS}