# This script makes minor changes to the *.rst files created by Sphinx apidoc

RST_SOURCE_DIR=source
HIDE_COOKIESTRING="   :exclude-members: cookiestring"
REPLACE_MAXDEPTH="s/   :maxdepth: 4/   :maxdepth: 1/g"

# Remove display of ``cookiestring`` class variable, otherwise Sphinx generates docs containing the value of your cookiestring, based on your ``YOUTUBE_COOKIESTRING`` environment variable
for file in cisticola.scraper.base.rst cisticola.scraper.rumble.rst
do
    echo "$HIDE_COOKIESTRING" >> $RST_SOURCE_DIR/$file;
done

# Set max depth to 1 for subpackages (only showing module files), makes it less confusing
for file in cisticola.rst cisticola.scraper.rst cisticola.transformer.rst
do
    sed -i "${REPLACE_MAXDEPTH}" ${RST_SOURCE_DIR}/${file};
done