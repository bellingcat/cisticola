import cisticola
import cisticola.scraper.telegram_snscrape
import cisticola.scraper.twitter
import cisticola.scraper.gettr
import cisticola.scraper.bitchute
import cisticola.scraper.odysee
import cisticola.scraper.gab

from sqlalchemy import create_engine


test_channels = [
    cisticola.base.Channel(id=0, name="Logan Williams (test)", platform_id=891729132,
                                   category="test", followers=None, platform="Twitter",
                                   url="https://twitter.com/obtusatum", screenname="obtusatum", country="US",
                                   influencer=None, public=True, chat=False,
                                   notes=""),
                 cisticola.base.Channel(id=1, name="South West Ohio Proud Boys (test)", platform_id=-1001276612436,
                                   category="test", followers=None, platform="Telegram",
                                   url="https://t.me/SouthwestOhioPB", screenname="SouthwestOhioPB", country="US",
                                   influencer=None, public=True, chat=False, notes=""),
                 cisticola.base.Channel(id=2, name="LizardRepublic (test)", platform_id='lizardrepublic',
                                   category="test", followers=None, platform="Gettr",
                                   url="https://www.gettr.com/user/lizardrepublic", screenname="lizardrepublic", country="US",
                                   influencer=None, public=True, chat=False, notes=""),
                cisticola.base.Channel(
                                    id=4, name="bestonlinejewelrystoresusa@gmail.com (test)", platform_id='bestonlinejewelrystoresusagmailcom',
                                    category="test", followers=None, platform="Bitchute",
                                    url="https://www.bitchute.com/channel/bestonlinejewelrystoresusagmailcom/", screenname=None, country="US",
                                    influencer=None, public=True, chat=False, notes=""),
                cisticola.base.Channel(
                                    id=5, name="Mak1n' Bacon (test)", platform_id='Mak1nBacon',
                                    category="test", followers=None, platform="Odysee",
                                    url="https://odysee.com/@Mak1nBacon", screenname='Mak1nBacon', country="US",
                                    influencer=None, public=True, chat=False, notes=""),
                cisticola.base.Channel(
                                    id=6, name="Capt. Marc Simon (test)", platform_id='marc_capt',
                                    category="test", followers=None, platform="Gab",
                                    url="https://gab.com/marc_capt", screenname='marc_capt', country="CA",
                                    influencer=None, public=True, chat=False, notes="")]


controller = cisticola.ScraperController()

twitter = cisticola.scraper.twitter.TwitterScraper()
controller.register_scraper(twitter)

telegram = cisticola.scraper.telegram_snscrape.TelegramSnscrapeScraper()
controller.register_scraper(telegram)

gettr = cisticola.scraper.gettr.GettrScraper()
controller.register_scraper(gettr)

bitchute = cisticola.scraper.bitchute.BitchuteScraper()
controller.register_scraper(bitchute)

odysee = cisticola.scraper.odysee.OdyseeScraper()
controller.register_scraper(odysee)

gab = cisticola.scraper.gab.GabScraper()
controller.register_scraper(gab)

engine = create_engine('sqlite:///test3.db')
controller.connect_to_db(engine)

controller.scrape_channels(test_channels)

