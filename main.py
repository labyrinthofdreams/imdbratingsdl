"""
The MIT License (MIT)

Copyright (c) 2014-2015 https://github.com/labyrinthofdreams/

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

from gevent import monkey
from gevent.pool import Pool
# patches stdlib (including socket and ssl modules) to cooperate with other greenlets
monkey.patch_all()
import argparse
import codecs
import logging
import os.path
import re
import sys
import time
import traceback
import requests
import bs4
from unicodewriter import UnicodeWriter

config = {}

page_key = None

session = requests.Session()

logfmt = logging.Formatter('%(asctime)s %(message)s')
fh = logging.FileHandler('out.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(logfmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(fh)

def read_cookies(cookie_file):
    imdbcookies = {}
    if cookie_file is None or not os.path.exists(cookie_file):
        return imdbcookies

    try:
        with open(cookie_file, 'rb') as f:
            data = f.read()
            for item in data.split('; '):
                parts = item.split('=', 1)
                imdbcookies[parts[0]] = parts[1]
        return imdbcookies
    except IOError:
        raise

def get_start_positions(last_page, start_from=1):
    cur_page = start_from
    while cur_page <= last_page:
        start_pos = (cur_page - 1) * 100
        yield (cur_page, start_pos)
        cur_page += 1

def pretty_seconds(t):
    sep = lambda n: (int(n / 60), int(n % 60))
    mins, secs = sep(t)
    hrs, mins = sep(mins)
    return '{0}h {1}m {2}s'.format(hrs, mins, secs)

next_url = None

def download_page(cur_page, start_pos):
    global next_url
    global page_key

    print 'Downloading page', cur_page, 'of', config['num_pages']
    # http://www.imdb.com/user/ur{0}/ratings?0
    if cur_page == 1:
        next_url = 'http://www.imdb.com/user/ur' + config['uid'] + '/ratings'
    #else:
    #    next_url = 'http://www.imdb.com/user/ur' + config['uid'] + '/ratings?sort=date_added%2Cdesc&mode=detail&lastPosition=' + str(start_pos) + '&paginationKey=' + page_key
    logger.info('Downloading page %s: %s', cur_page, next_url)
    trs = None
    # Try downloading the page until we've received all 100 rows of rating data
    while True:
        resp = session.get(next_url)
        if resp.status_code != 200:
            print 'Failed to download page', cur_page, '(or private list). Retrying...'
            logger.info('Failed to download page %s (or private list). Retrying...', cur_page)
            continue
        html = bs4.BeautifulSoup(resp.text, 'html.parser')
        # Update pagination key
        # Note: This will break if using more than 1 thread
        
        page_key = re.search('paginationKey=([^&]+)', parsed_html.select('a.next-page')[0]['href']).group(1)
        next_url = 'http://www.imdb.com' + html.select('a.next-page')[0]['href']
        trs = html.find_all('div', class_='lister-item')
        is_last_page = (cur_page == config['num_pages'])
        if len(trs) != 100 and not is_last_page:
            print 'Error: Received less data than expected (100 ratings, received', len(trs), '). Retrying...'
            continue
        else:
            break
    print 'Parsing page', cur_page
    for tr in trs:
        try:
            position = unicode(len(imdb_all) + 1)
            title_elems = tr.select('.lister-item-header')[0].find_all('a')
            if len(title_elems) == 1:
                # Non-TV
                title = title_elems[0].get_text()
                imdb_url = title_elems[0]['href']
                title_type = 'Feature Film'
            else:
                # TV Episode
                title = '{0}: {1}'.format(title_elems[0].get_text(), title_elems[1].get_text())
                imdb_url = title_elems[1]['href']
                title_type = 'TV Episode'
            imdb_id = re.search('(tt[0-9]{7})', imdb_url).group(1)
            
            rater = tr.select('div.ipl-rating-star--other-user > span.ipl-rating-star__rating')
            if len(rater):
                # Other user's rating, or own but logged out
                user_rating = rater[0].get_text().strip()
            else:
                # Your own rating, logged in
                rater = tr.select('div.ipl-rating-star > span.ipl-rating-star__rating')
                if len(rater) == 1:
                    user_rating = rater[0].get_text().strip()
                else:
                    # No rating found at all
                    continue

            imdb_rating = tr.select('div.ipl-rating-star > span.ipl-rating-star__rating')
            if len(imdb_rating):
                imdb_rating = imdb_rating[0].get_text().strip()
            else:
                imdb_rating = u''
                # TODO: Might not work with new layout
            if imdb_rating == '0.0':
                imdb_rating = u''
            
            runtime = tr.select('span.runtime')
            if len(runtime):
                runtime = runtime[0].get_text()
            else:
                runtime = u''
            
            year_elems = tr.find_all('span', class_='lister-item-year')
            if len(year_elems) == 1:
                year = year_elems[0].get_text()
            elif len(year_elems) > 1:
                # TV Episode year
                year = year_elems[1].get_text()
            else:
                year = u''
            
            genres = tr.select('span.genre')
            if len(genres):
                genres = genres[0].get_text()
            else:
                genres = u''

            rating_date = tr.select('div.lister-item-content > p')[1].get_text()

            desc = tr.select('div.lister-item-content > p')[2].get_text()

            found = tr.find('span', attrs={'name': 'nv'})
            num_votes = found['data-value'] if found is not None else "0"
            # TODO: This might not work with new layout
            if num_votes == '-':
                num_votes = u'0'

            directors = []
            castcrew = tr.select('div.lister-item-content > p')[3]
            for el in castcrew.children:
                if el.name == 'a':
                    directors.append(el.text)
                elif el.name == 'span':
                    # <span> tag separates directors from actors
                    break

            data = [position, imdb_id, rating_date, u'', desc, title, title_type,
                    ", ".join(directors), user_rating, imdb_rating, runtime, year, genres,
                    num_votes, u'', unicode('http://www.imdb.com' + imdb_url)]
            is_dupe = any(e[1] == imdb_id for e in imdb_all)
            # TV Episodes may have the same show IMDb id
            if is_dupe and title_type not in ('TV Episode', 'TV Series', 'Mini-Series'):
                # This may happen if user rates another film while we're downloading
                print 'Found a duplicate entry:', imdb_id, title
                logger.info('[%s] Found a duplicate entry: %s', args.outfile, str(data))
            else:
                imdb_all.append(data)
        except Exception as e:
            print traceback.format_exc()
            print 'Error: {0}'.format(str(e))
            logger.exception('Error while parsing: %s', str(e))

if __name__ == '__main__':
    opts = argparse.ArgumentParser(description='Download IMDb ratings')
    opts.add_argument('ratings_url', help='URL to IMDb user ratings page')
    opts.add_argument('outfile', help='Path to output CSV file')
    opts.add_argument('--start', type=int, default=1, help='Specify page number to start from')
    opts.add_argument('--cookies', default='cookies.txt', help='Load cookies from file')
    opts.add_argument('--threads', default=1, type=int, help='Number of simultaneous downloads')
    args = opts.parse_args()

    if os.path.exists(args.outfile):
        answer = raw_input('Output file {0} already exists. Overwrite? [y/n]: '.format(args.outfile))
        if answer.lower() == 'n':
            sys.exit('Aborted')

    pool = Pool(args.threads)

    if args.start < 1:
        print 'Start page cannot be less than 1. Setting start page to 1...'
        args.start = 1

    imdbcookies = read_cookies(args.cookies)
    session.cookies = requests.utils.cookiejar_from_dict(imdbcookies)

    logger.debug('Cookies: %s', imdbcookies)

    start_time = time.time()
    # Get number of pages and pagination key
    print 'Downloading pagination key'
    config['uid'] = re.search('ur([0-9]{7,8})', args.ratings_url).group(1)
    url = 'http://www.imdb.com/user/ur{0}/ratings'.format(config['uid'])
    #logger.info('Retrieving number of pages from %s', url)
    resp = session.get(url)
    while resp.status_code != 200:
        print 'Failed to retrieve number of pages (or private list)'
        print 'Retrying...'
        resp = session.get(url)
    logger.info('Parsing content')
    parsed_html = bs4.BeautifulSoup(resp.text, 'html.parser')
    # Get ratings count
    pages_text = parsed_html.find('span', class_='pagination-range').get_text()
    num_ratings = int(re.search('100 of ([0-9]*,?[0-9]+)', pages_text).group(1).replace(",", ""))
    config['num_pages'] = (num_ratings / 100) + 1
    print 'Found {0} pages'.format(config['num_pages'])
    if args.start > config['num_pages']:
        print 'Start page', args.start, 'is greater than found pages:', config['num_pages']
        print 'Setting start pages to last page'
        args.start = config['num_pages']
    # Get pagination key    
    page_key = re.search('paginationKey=([^&]+)', parsed_html.select('a.next-page')[0]['href']).group(1)
    print 'Found pagination key:', page_key
    imdb_all = []
    username = os.path.splitext(os.path.basename(args.outfile))[0]
    with codecs.open(args.outfile, 'wb') as outfile:
        w = UnicodeWriter(outfile)
        # Only output header if file didn't exist
        w.writerow(['position','const','created','modified','description','Title','Title type','Directors',
                    '{0} rated'.format(username),'IMDb Rating','Runtime (mins)','Year','Genres','Num. Votes',
                    'Release Date (month/day/year)','URL'])
    for page in get_start_positions(config['num_pages'], args.start):
        pool.spawn(download_page, page[0], page[1])
    pool.join()
    with codecs.open(args.outfile, 'ab') as outfile:
        w = UnicodeWriter(outfile)
        w.writerows(imdb_all)
    end_time = time.time()
    print 'Downloaded', len(imdb_all), 'ratings in', pretty_seconds(end_time - start_time)
    logger.info('Downloaded %s ratings in %s', len(imdb_all), pretty_seconds(end_time - start_time))
    print 'Saved results in', args.outfile
