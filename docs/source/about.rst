About Cisticola
===============

The *cisticola* application enables users to easily collect, process, and analyze large-scale data from several social media platforms.

Definitions
-----------
- *Platform*: a social media website, for example Telegram, YouTube, or Rumble.
- *Channel*: an account or group on a platform, for example Twitter users, Telegram private chat groups, YouTube channels, and Gab groups.
- *Post*: a single item created by a channel, for example a Telegram message, a Tweet, or a YouTube video. Posts can contain one or more media attachments.
- *Media*: a file uploaded to a platform by a channel as part of a post.

Components
----------
Cisticola has many components

- :py:mod:`cisticola.base`: contains Object Relational Mapping (ORM) dataclasses that imperatively map to pre-defined SQL tables
- :py:mod:`cisticola.scraper`: contains platform-specific modules for scraping raw data from platforms. For example, the :py:mod:`cisticola.scraper.bitchute` module extracts raw data from Bitchute.
- :py:mod:`cisticola.transformer`: contains platform-specific modules for converting raw data into a standardized, cross-platform format.

The data extracted by scrapers varies by platform, but typically includes media files attached to posts. 

Separating the "scraping" and "transforming" steps is useful because it ensures that no data is thrown away during the transormation. There may be some fields in the raw data that aren't included in the transformed format, but could be found to be useful in the future.

TODO
- Add diagram
- Describe common workflow and steps
- Update environment variables