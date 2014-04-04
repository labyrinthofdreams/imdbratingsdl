import argparse
import codecs
import logging
import os.path
import re
import sys
import time
import requests
import bs4
from unicodewriter import UnicodeWriter

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
        start_pos = (cur_page - 1) * 250 + 1
        yield (cur_page, start_pos)
        cur_page += 1

def pretty_seconds(t):
    sep = lambda n: (int(n / 60), int(n % 60))
    mins, secs = sep(t)
    hrs, mins = sep(mins)
    return '{0}h {1}m {2}s'.format(hrs, mins, secs)

if __name__ == '__main__':
    opts = argparse.ArgumentParser(description='Download large IMDb rating lists.')
    opts.add_argument('ratings_url', help='URL to IMDb user ratings page')
    opts.add_argument('outfile', help='Path to output CSV file')
    opts.add_argument('--start', type=int, default=1, help='Specify page number to start from')
    opts.add_argument('--cookies', default='cookies.txt', help='Load cookies from file')
    args = opts.parse_args()
    
    if args.start < 1:
        print 'Start page cannot be less than 1. Setting start page to 1...'
        args.start = 1

    imdbcookies = read_cookies(args.cookies)
    session.cookies = requests.utils.cookiejar_from_dict(imdbcookies)

    logger.debug('Cookies: %s', imdbcookies)

    start_time = time.time()
    # Get number of pages
    print 'Retrieving number of pages'
    url = args.ratings_url + '?start=1&view=compact&sort=ratings_date:desc&defaults=1'
    logger.info('Retrieving number of pages from %s', url)
    resp = requests.get(url)
    while resp.status_code != 200:
        print 'Failed to retrieve number of pages (or private list)'
        print 'Retrying...'
        resp = requests.get(url)
    logger.info('Parsing content')
    parsed_html = bs4.BeautifulSoup(resp.text)
    # Note: There should be only one element called div.desc
    # but there's no guarantee
    pages_text = parsed_html.find('div', class_='desc').get_text()
    num_pages = int(re.search('Page 1 of ([0-9]+)', pages_text).group(1))
    logger.info('Found %s pages', num_pages)
    if args.start > num_pages:
        print 'Start page', args.start, 'is greater than found pages:', num_pages
        print 'Setting start pages to last page'
        args.start = num_pages

    imdb = []
    username = os.path.splitext(os.path.basename(args.outfile))[0]
    outfile_exists = os.path.exists(args.outfile)
    with codecs.open(args.outfile, 'ab') as outfile:
        w = UnicodeWriter(outfile)
        if not outfile_exists:
            # Only output header if file didn't exist   
            w.writerow(['position','const','created','modified','description','Title','Title type','Directors',
                    '{0} rated'.format(username),'IMDb Rating','Runtime (mins)','Year','Genres','Num. Votes',
                    'Release Date (month/day/year)','URL'])
        for cur_page, start_pos in get_start_positions(num_pages, args.start):
            print 'Downloading page', cur_page, 'of', num_pages
            url = args.ratings_url + '?start=' + str(start_pos) + '&view=compact&sort=ratings_date:desc&defaults=1'
            logger.info('Downloading page %s: %s', cur_page, url)
            trs = None
            # Try downloading the page until we've received all 201 rows of rating data
            while True:
                resp = requests.get(url)
                if resp.status_code != 200:
                    print 'Failed to download page', cur_page, '(or private list). Retrying...'
                    logger.info('Failed to download page %s (or private list). Retrying...', cur_page)
                html = bs4.BeautifulSoup(resp.text, 'html.parser')
                trs = html.find_all('tr', class_='list_item')
                is_last_page = (cur_page == num_pages)
                if len(trs) != 251 and not is_last_page:
                    print 'Error: Received less data than expected (251 ratings, received', len(trs), '). Retrying...'
                else:
                    break
            print 'Parsing page', cur_page
            del trs[0] # Skip header
            for tr in trs:
                try:
                    position = unicode(len(imdb) + 1)
                    html_title = tr.find('td', class_='title')
                    imdb_url = html_title.a['href']
                    imdb_id = re.search('(tt[0-9]{7})', imdb_url).group(1)
                    if 'episode' in html_title['class']:
                        # This ensures that there's a space between
                        # series title and episode name
                        html_title.a.append(' ')
                    title = html_title.get_text()
                    title_type = tr.find('td', class_='title_type').get_text().strip()
                    if title_type == 'Feature':
                        title_type = 'Feature Film'
                    user_rating = tr.select('td.rater_ratings > a')[0].get_text()
                    imdb_rating = tr.find('td', class_='user_rating').get_text()
                    if imdb_rating == '0.0':
                        imdb_rating = u''
                    year = tr.find('td', class_='year').get_text()
                    num_votes = tr.find('td', class_='num_votes').get_text().replace(',', '')
                    if num_votes == '-':
                        num_votes = u'0'
                    url = unicode('http://www.imdb.com' + imdb_url)
    
                    imdb.append([position, imdb_id, u'', u'', u'', title, title_type,
                                 u'', user_rating, imdb_rating, u'', year, u'',
                                 num_votes, u'', url])                             
                except Exception as e:
                    logger.exception('Error while parsing: %s', str(e))
                    logger.debug('<tr> content: %s', tr)
            # Write results to CSV file
            print 'Saving results', start_pos, '-', (start_pos + 250)
            w.writerows(imdb)
            imdb = []
    end_time = time.time()
    print 'Downloaded', len(imdb), 'ratings in', pretty_seconds(end_time - start_time)
    logger.info('Downloaded %s ratings in %s', len(imdb), pretty_seconds(end_time - start_time)) 
    print 'Saved results in', args.outfile
