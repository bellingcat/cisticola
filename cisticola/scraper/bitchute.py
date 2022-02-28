from datetime import datetime
import time
import re 
from html.parser import HTMLParser
import dateparser
import json
from typing import Generator

import requests
from bs4 import BeautifulSoup

import cisticola.base

class BitchuteScraper(cisticola.scraper.base.Scraper):
    """An implementation of a Scraper for Bitchute, using classes from the 4cat
    library"""
    __version__ = "BitchuteScraper 0.0.1"

    # TODO snscrape should be able to scrape from user ID alone, but there is
    # currently a bug/other issue, so it is extracting the username from URL
    def get_username_from_url(url):
        username = url.split('bitchute.com/channel/')[-1].strip('/')

        return username

    def get_posts(self, channel: cisticola.base.Channel, since: cisticola.base.ScraperResult = None) -> Generator[cisticola.base.ScraperResult, None, None]:

        session = requests.Session()
        session.headers.update(self.headers)
        request = session.get("https://www.bitchute.com/search")
        csrftoken = BeautifulSoup(request.text, 'html.parser').findAll(
            "input", {"name": "csrfmiddlewaretoken"})[0].get("value")
        time.sleep(0.25)

        # Don't scrape comment information 
        #TODO implement framework for processing and storing comments
        detail = 'comments'

        username = BitchuteScraper.get_username_from_url(channel.url)
        scraper = get_videos_user(session, username, csrftoken, detail)

        for post in scraper:

            if since is not None and datetime.fromtimestamp(post['timestamp']) <= since.date:
                break

            archived_urls = {}

            if 'video_url' in post:
                url = post['video_url']
                media_blob, content_type, key = self.url_to_blob(url)
                archived_url = self.archive_media(media_blob, content_type, key)
                archived_urls[url] = archived_url

            yield cisticola.base.ScraperResult(
                scraper=self.__version__,
                platform="Bitchute",
                channel=channel.id,
                platform_id=post['id'],
                date=datetime.fromtimestamp(post['timestamp']),
                date_archived=datetime.now(),
                raw_data=json.dumps(post),
                archived_urls=archived_urls)

    def can_handle(self, channel):
        if channel.platform == "Bitchute" and BitchuteScraper.get_username_from_url(channel.url) is not None:
            return True

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

def strip_tags(html, convert_newlines=True):
    r"""
    Strip HTML from a string

    :param html: HTML to strip
    :param convert_newlines: Convert <br> and </p> tags to \n before stripping
    :return: Stripped HTML
    """
    if not html:
        return ""

    deduplicate_newlines = re.compile(r"\n+")

    if convert_newlines:
        html = html.replace("<br>", "\n").replace("</p>", "</p>\n")
        html = deduplicate_newlines.sub("\n", html)

    class HTMLStripper(HTMLParser):
        def __init__(self):
            super().__init__()
            self.reset()
            self.strict = False
            self.convert_charrefs = True
            self.fed = []

        def handle_data(self, data):
            self.fed.append(data)

        def get_data(self):
            return "".join(self.fed)

    stripper = HTMLStripper()
    stripper.feed(html)
    return stripper.get_data()

#-----------------------------------------------------------------------------#

def request_from_bitchute(session, method, url, headers=None, data=None):
    """
    Request something via the BitChute API (or non-API)

    To avoid having to write the same error-checking everywhere, this takes
    care of retrying on failure, et cetera

    :param session:  Requests session
    :param str method: GET or POST
    :param str url:  URL to fetch
    :param dict header:  Headers to pass with the request
    :param dict data:  Data/params to send with the request

    :return:  Requests response
    """
    retries = 0
    response = None
    while retries < 3:
        try:
            if method.lower() == "post":
                request = session.post(url, headers=headers, data=data)
            elif method.lower() == "get":
                request = session.get(url, headers=headers, params=data)
            else:
                raise NotImplemented()

            if request.status_code >= 300:
                raise ValueError("Response %i from BitChut for URL %s, need to retry" % (request.status_code, url))

            response = request.json()
            return response

        except (ConnectionResetError, requests.RequestException, ValueError) as e:
            retries += 1
            time.sleep(retries * 2)

        except json.JSONDecodeError as e:
            raise RuntimeError()

    if not response:
        raise RuntimeError()

    return response

#-----------------------------------------------------------------------------#

def append_details(video, detail):
    """
    Append extra metadata to video data

    Fetches the BitChute video detail page to scrape extra data for the given video.

    :param dict video:  Video details as scraped so far
    :param str detail:  Detail level. If 'comments', also scrape video comments.

    :return dict:  Tuple, first item: updated video data, second: list of comments
    """
    comments = []

    video = {
        **video,
        "likes": "",
        "dislikes": "",
        "channel_subscribers": "",
        "comments": "",
        "hashtags": "",
        "parent_id": "",
        "video_url": ""
    }

    try:
        # to get more details per video, we need to request the actual video detail page
        # start a new session, to not interfere with the CSRF token from the search session
        video_session = requests.session()
        video_page = video_session.get(video["url"])

        if "<h1 class=\"page-title\">Video Restricted</h1>" in video_page.text or \
                "<h1 class=\"page-title\">Video Blocked</h1>" in video_page.text or \
                "<h1 class=\"page-title\">Channel Blocked</h1>" in video_page.text or \
                "<h1 class=\"page-title\">Channel Restricted</h1>" in video_page.text:
            if "This video is unavailable as the contents have been deemed potentially illegal" in video_page.text:
                video["category"] = "moderated-illegal"
                return (video, [])

            elif "Viewing of this video is restricted, as it has been marked as Not Safe For Life" in video_page.text:
                video["category"] = "moderated-nsfl"
                return (video, [])

            elif "Contains Incitement to Hatred" in video_page.text:
                video["category"] = "moderated-incitement"
                return (video, [])

            elif "Platform Misuse" in video_page.text:
                video["category"] = "moderated-misuse"
                return (video, [])

            elif "Terrorism &amp; Violent Extremism" in video_page.text:
                video["category"] = "moderated-terrorism-extremism"
                return (video, [])

            elif "Copyright</h4>" in video_page.text:
                video["category"] = "moderated-copyright"
                return (video, [])

            else:
                video["category"] = "moderated-other"
                return (video, [])

        elif "<iframe class=\"rumble\"" in video_page.text:
            # some videos are actually embeds from rumble?
            # these are iframes, so at the moment we cannot simply extract
            # their info from the page, so we skip them. In the future we
            # could add an extra request to get the relevant info, but so
            # far the only examples I've seen are actually 'video not found'
            video = {
                **video,
                "category": "error-embed-from-rumble"
            }
            return (video, [])

        elif video_page.status_code != 200:
            video = {
                **video,
                "category": "error-%i" % video_page.status_code
            }
            return (video, [])

        soup = BeautifulSoup(video_page.text, 'html.parser')
        video_csfrtoken = soup.findAll("input", {"name": "csrfmiddlewaretoken"})[0].get("value")

        video["video_url"] = soup.select_one("video#player source").get("src")
        video["thumbnail_image"] = soup.select_one("video#player").get("poster")
        video["subject"] = soup.select_one("h1#video-title").text
        video["author"] = soup.select_one("div.channel-banner p.name a").text
        video["author_id"] = soup.select_one("div.channel-banner p.name a").get("href").split("/")[2]
        video["body"] = soup.select_one("div#video-description").encode_contents().decode("utf-8").strip()

        # we need *two more requests* to get the comment count and like/dislike counts
        # this seems to be because bitchute uses a third-party comment widget
        video_session.headers = {'Referer': video["url"], 'Origin': video["url"]}
        counts = request_from_bitchute(video_session, "POST", "https://www.bitchute.com/video/%s/counts/" % video["id"], data={"csrfmiddlewaretoken": video_csfrtoken})

        if detail == "comments":
            # if comments are also to be scraped, this is anothe request to make, which returns
            # a convenient JSON response with all the comments to the video
            # we need yet another token for this, which we can extract from a bit of inline
            # javascript on the page
            comment_script = None
            for line in video_page.text.split("\n"):
                if "initComments(" in line:
                    comment_script = line.split("initComments(")[1]
                    break

            if not comment_script:
                # no script to extract comments from, cannot load
                comment_count = -1
            else:
                # make the request
                comment_count = 0
                url = comment_script.split("'")[1]
                comment_csrf = comment_script.split("'")[3]
                comments_data = request_from_bitchute(video_session, "POST", url + "/api/get_comments/", data={"cf_auth": comment_csrf, "commentCount": 0})

                for comment in comments_data:
                    comment_count += 1

                    if comment.get("profile_picture_url", None):
                        thumbnail_image = url + comment.get("profile_picture_url")
                    else:
                        thumbnail_image = ""

                    comments.append({
                        "id": comment["id"],
                        "thread_id": video["id"],
                        "subject": "",
                        "body": comment["content"],
                        "author": comment["fullname"],
                        "author_id": comment["creator"],
                        "timestamp": int(dateparser.parse(comment["created"]).timestamp()),
                        "url": "",
                        "views": "",
                        "length": "",
                        "hashtags": "",
                        "thumbnail_image": thumbnail_image,
                        "likes": comment["upvote_count"],
                        "category": "comment",
                        "dislikes": "",
                        "channel_subscribers": "",
                        "comments": "",
                        "parent_id": comment.get("parent", "") if "parent" in comment else video["id"],
                    })

        else:
            # if we don't need the full comments, we still need another request to get the *amount*
            # of comments,
            comment_count = request_from_bitchute(video_session, "POST",
                "https://commentfreely.bitchute.com/api/get_comment_count/",
                data={"csrfmiddlewaretoken": video_csfrtoken,
                      "cf_thread": "bc_" + video["id"]})["commentCount"]

    except RuntimeError as e:
        # we wrap this in one big try-catch because doing it for each request separarely is tedious
        # hm... maybe this should be in a helper function
#         self.dataset.update_status("Error while interacting with BitChute (%s) - try again later." % e,
#                                    is_final=True)
        return (None, None)

    # again, no structured info available for the publication date, but at least we can extract the
    # exact day it was uploaded
    try:
        published = dateparser.parse(
            soup.find(class_="video-publish-date").text.split("published at")[1].strip()[:-1])
    except AttributeError as e:
        # publication date not on page?
        published = None

    # merge data
    video = {
        **video,
        "category": re.findall(r'<td><a href="/category/([^/]+)/"', video_page.text)[0],
        "likes": counts["like_count"],
        "dislikes": counts["dislike_count"],
        "channel_subscribers": counts["subscriber_count"],
        "comments": comment_count,
        "parent_id": "",
        "hashtags": ",".join([tag.text for tag in soup.select("#video-hashtags li a")]),
        "views": counts["view_count"]
    }

    if published:
        video["timestamp"] = int(published.timestamp())

    # may need to be increased? bitchute doesn't seem particularly strict
    time.sleep(0.25)
    return (video, comments)

#-----------------------------------------------------------------------------#

def get_videos_user(session, user, csrftoken, detail):
    """
    Scrape videos for given BitChute user

    :param session:  HTTP Session to use
    :param str user:  Username to scrape videos for
    :param str csrftoken:  CSRF token to use for requests
    :param str detail:  Detail level to scrape, basic/detail/comments

    :return:  Video data dictionaries, as a generator
    """
    max_items = 100
    num_items = 0
    offset = 0
    
    base_url = "https://www.bitchute.com/channel/%s/" % user
    url = base_url + "extend/"

    container = session.get(base_url)
    container_soup = BeautifulSoup(container.text, 'html.parser')
    headers = {'Referer': base_url, 'Origin': "https://www.bitchute.com/"}

    while True:

        post_data = {"csrfmiddlewaretoken": csrftoken, "name": "", "offset": str(offset)}

        try:
            request = session.post(url, data=post_data, headers=headers)
            if request.status_code != 200:
                raise ConnectionError()
            response = request.json()

        except (json.JSONDecodeError, requests.RequestException, ConnectionError) as e:
            raise ValueError('FALSE')
        soup = BeautifulSoup(response["html"], 'html.parser')
        videos = soup.select(".channel-videos-container")
        comments = []

        if len(videos) == 0 or num_items >= max_items:
            break
            

        for video_element in videos:
            if num_items >= max_items:
                break
            else:
                num_items += 1

            offset += 1

            link = video_element.select_one(".channel-videos-title a")
            video = {
                "id": link["href"].split("/")[-2],
                "thread_id": link["href"].split("/")[-2],
                "subject": link.text,
                "body": strip_tags(video_element.select_one(".channel-videos-text").text),
                "author": container_soup.select_one(".details .name a").text,
                "author_id": container_soup.select_one(".details .name a")["href"].split("/")[2],
                "timestamp": int(
                    dateparser.parse(
                        video_element.select_one(".channel-videos-details.text-right.hidden-xs").text).timestamp()),
                "url": "https://www.bitchute.com" + link["href"],
                "views": video_element.select_one(".video-views").text.strip(),
                "length": video_element.select_one(".video-duration").text.strip(),
                "thumbnail_image": video_element.select_one(".channel-videos-image img")["src"],
            }

            if detail != "basic":
                video, comments = append_details(video, detail)
                if not video:
                    # unrecoverable error while scraping details
                    return

            yield video
            for comment in comments:
                # these need to be yielded *after* the video because else the result file will have the comments
                # before the video, which is weird
                yield comment
#-----------------------------------------------------------------------------#

def get_about(user):
    """
    Extract fields from channel's "About" tab
    """
    base_url = "https://www.bitchute.com/channel/%s/" % user
    
    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    about_soup = soup.find('div', {'id' : 'channel-about'})
    info_list = about_soup.find('div', {'class' : 'channel-about-details'}).find_all('p')
    description_soup = about_soup.find('div', {'id' : 'channel-description'})

    about = {
        'description' : description_soup.text,
        'description_links' : [a['href'] for a in description_soup.find_all('a', href = True)],
        'created': re.sub('\s', ' ', info_list[0].text.split('Created')[1].strip('. ')),
        'videos' : int(info_list[1].text.split('videos')[0].strip()),
        'owner_url' : soup.find('p', {'class' : 'owner'}).find('a', href = True)['href'],
        'owner_name' : soup.find('p', {'class' : 'owner'}).text,
        'category' : info_list[-1].text.split('Category')[1].strip(),
        'image' : about_soup.find('img', {'alt' : 'Channel Image'})['data-src']
    }
    
    return about
