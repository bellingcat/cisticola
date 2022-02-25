import cisticola
import cisticola.scraper.telegram_snscrape
import cisticola.scraper.twitter
import cisticola.scraper.gettr
import cisticola.scraper.bitchute

from sqlalchemy import create_engine


test_channels = [
    cisticola.base.Channel(id=0, name="Logan Williams (test)", platform_id=891729132,
                                   category="test", followers=None, platform="Twitter",
                                   url="https://twitter.com/obtusatum", screenname="obtusatum", country="US",
                                   influencer=None, public=True, chat=False,
                                   notes=""),
                 cisticola.base.Channel(id=1, name="JQHN SPARTAN", platform_id=-1001181961026,
                                   category="qanon", followers=None, platform="Telegram",
                                   url="https://t.me/jqhnspartan", screenname="jqhnspartan", country="FR",
                                   influencer="JQNH SPARTAN", public=True, chat=False, notes=""),
                 cisticola.base.Channel(id=2, name="LizardRepublic", platform_id='lizardrepublic',
                                   category="qanon", followers=None, platform="Gettr",
                                   url="https://www.gettr.com/user/lizardrepublic", screenname="lizardrepublic", country="US",
                                   influencer=None, public=True, chat=False, notes=""),
                cisticola.base.Channel(
                                    id=4, name="bestonlinejewelrystoresusa@gmail.com", platform_id='bestonlinejewelrystoresusagmailcom',
                                    category="spam", followers=None, platform="Bitchute",
                                    url="https://www.bitchute.com/channel/bestonlinejewelrystoresusagmailcom/", screenname=None, country="US",
                                    influencer=None, public=True, chat=False, notes=""),]


controller = cisticola.ScraperController()

twitter = cisticola.scraper.twitter.TwitterScraper()
controller.register_scraper(twitter)

telegram = cisticola.scraper.telegram_snscrape.TelegramSnscrapeScraper()
controller.register_scraper(telegram)

gettr = cisticola.scraper.gettr.GettrScraper()
controller.register_scraper(gettr)

bitchute = cisticola.scraper.bitchute.BitchuteScraper()
controller.register_scraper(gettr)

engine = create_engine('sqlite:///test3.db')
controller.connect_to_db(engine)

controller.scrape_channels(test_channels)

