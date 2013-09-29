import argparse
import re
import os.path
import time
import codecs
import requests
import bs4
from unicodewriter import UnicodeWriter

def get_start_positions(last_page):
    cur_page = 1
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
    args = opts.parse_args()

    start_time = time.time()
    # Get number of pages
    print 'Retrieving number of pages'
    url = args.ratings_url + '?start=1&view=compact&sort=ratings_date:desc&defaults=1'
    resp = requests.get(url)
    while resp.status_code != 200:
        print 'Failed to retrieve number of pages (or private list)'
        print 'Retrying...'
        resp = requests.get(url)
    parsed_html = bs4.BeautifulSoup(resp.text)
    # Note: There should be only one element called div.desc
    # but there's no guarantee
    pages_text = parsed_html.find('div', class_='desc').get_text()
    num_pages = int(re.search('Page 1 of ([0-9]+)', pages_text).group(1))

    imdb = []
    for cur_page, start_pos in get_start_positions(num_pages):
        print 'Downloading page', cur_page, 'of', num_pages
        url = args.ratings_url + '?start=' + str(start_pos) + '&view=compact&sort=ratings_date:desc&defaults=1'
        resp = requests.get(url)
        while resp.status_code != 200:
            print 'Failed to download page', cur_page, '(or private list)'
            print 'Retrying...'
            resp = requests.get(url)
        html = bs4.BeautifulSoup(resp.text)
        trs = html.find_all('tr', class_='list_item')
        del trs[0] # Skip header
        for tr in trs:
            position = unicode(len(imdb) + 1)
            html_title = tr.find('td', class_='title')
            imdb_url = html_title.a['href']
            imdb_id = re.search('(tt[0-9]{7})', imdb_url).group(1)
            if 'episode' in html_title['class']:
                # This ensures that there's a space between
                # series title and episode name
                html_title.a.append(' ')
            title = html_title.get_text()
            title_type = tr.find('td', class_='title_type').get_text().lstrip()
            user_rating = tr.find('td', class_='rater_ratings').a.get_text()
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
    end_time = time.time()
    print 'Downloaded', len(imdb), 'ratings in', pretty_seconds(end_time - start_time)
    # Write results to CSV file
    print 'Saving results to', args.outfile
    username = os.path.splitext(os.path.basename(args.outfile))[0]
    with codecs.open(args.outfile, 'wb') as outfile:
        w = UnicodeWriter(outfile)
        w.writerow(['position','const','created','modified','description','Title','Title type','Directors',
                    '{0} rated'.format(username),'IMDb Rating','Runtime (mins)','Year','Genres','Num. Votes',
                    'Release Date (month/day/year)','URL'])
        w.writerows(imdb)
