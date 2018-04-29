import requests
import re
from concurrent.futures import ProcessPoolExecutor, as_completed, Future
from tqdm import tqdm
from bs4 import BeautifulSoup
import bs4
import dateparser
import csv
import os

cities_path = '/Users/chris/PycharmProjects/defense_spending/src/defense_scraper/resources/us_cities_states_counties.csv'


class ArticleScraper:
    """
    Scrapes data from https://www.defense.gov/News/Contracts/Contract-View/Article/{ARTICLE ID}
    """

    header = ['date', 'branch', 'city', 'state', 'amount', 'error']
    _cities = set()
    _states = set()

    def __init__(self, url, cities_data_path=cities_path):
        self.bytes_processed = 0
        self._parse_cities(cities_data_path)
        self.url = url
        self.errors = []
        article = self.article_getter(url)
        self.date = self.parse_date(article)
        self.data = self.parse_graphs(article)



    def _parse_cities(self, filename: str):
        """
        Returns sets of city and state names for use when parsing.

        :param filename: Path to the CSV with city & state data.
        :return: a tuple of 2 sets.
        """
        f = csv.reader(open(filename, 'r'), delimiter='|')
        next(f)  # skip header
        for row in f:
            try:
                city, st_1, st_2, *rest = row
                self._cities.add(city.lower())
                # states.add(st_1.lower())
                self._states.add(st_2.lower())
            except:
                pass
        return

    def article_getter(self, url):
        pg = requests.get(url)
        self.bytes_processed += len(pg.text)
        return BeautifulSoup(pg.text, "html.parser")

    def parse_date(self, soup: BeautifulSoup):
        span = soup.find('span', class_='date')
        txt = span.text
        date_line = txt.splitlines()[3].strip()
        date = dateparser.parse(date_line)
        return date

    def parse_graphs(self, soup: BeautifulSoup):
        """
        Finds paragraphs with contract information.
        :param soup:
        :return:
        """

        text_span = soup.find(lambda tag: tag.name == 'span' and tag.get('class') == ['text'])
        graphs = text_span.find_all('p')  # {'style': ''}

        transactions = []
        for p in graphs:  #type: bs4.element.Tag
            if p.attrs.get('style') == 'text-align: center;':
                branch = p.get_text().replace("CONTRACTS", '').strip().upper()
            else:
                txt = p.get_text().strip()
                if txt and not txt.startswith('*'):  # todo: are there other exclusion criteria?
                    try:
                        data = self.parse_spending_paragraph(txt)
                        data['branch'] = branch
                        data['date'] = self.date
                        transactions.append(data)
                    except:
                        self.errors.append(txt)
        return transactions

    def parse_spending_paragraph(self, text):
        result = {
            'cities': [],
            'states': [],
            'amount': 0,
            'error': 0
        }

        pre_dollar, post_dollar, *rest = text.split('$')
        last_city = -10
        for i, s_dirty in enumerate(pre_dollar.split(',')):
            s = s_dirty.strip('*').strip()
            matcher = s.lower()
            #             print(matcher)
            if i - last_city != 1 and matcher in self._cities:
                result['cities'].append(s)
                last_city = i
            elif i - last_city == 1:
                print(matcher)
                if matcher in self._states:
                    result['states'].append(s)
                else:
                    for state in self._states:
                        if state in matcher:
                            result['states'].append(state)

        if len(result['cities']) != len(result['states']) or not result['cities']:
            raise ScrapingError

        amount_str_dirty, *rest = post_dollar.split(' ')
        amount_str, *rest = amount_str_dirty.split('.')  # for cents which are in some...
        result['amount'] = int(amount_str.replace(',', ''))

        return result

    @staticmethod
    def _unpack_dict(packed):
        results = []

        for c, s in zip(packed['cities'], packed['states']):
            r = dict()
            r['date'] = packed['date']
            r['branch'] = packed['branch']
            r['city'] = c
            r['state'] = s
            r['amount'] = packed['amount'] // len('cities')
            r['error'] = packed['error']
            results.append(r)
        return results

    def save(self, save_path):
        """saves data to CSV"""

        if not os.path.exists(save_path):
            with open(save_path, 'w') as f:
                c = csv.writer(f)
                c.writerow(self.header)

        with open(save_path, 'a') as f:
            c = csv.DictWriter(f, self.header)
            for d in self.data:
                rows = self._unpack_dict(d)
                for r in rows:
                    c.writerow(r)
        return

    def save_errors(self, savepath):
        if not self.errors:
            return
        if not os.path.exists(savepath):
            with open(savepath, 'w') as f:
                pass
        with open(savepath, 'a') as f:
            f.write('\n\n----------')
            f.writelines(self.url)
            f.write('\n')
            for er in self.errors:
                f.write(er)
        return





def load_article_numbers(filename):

    with open(filename) as f:
        articles = []
        s = f.readline()
        while s:
            for x in s.split(', '):
                if not x == '\n':
                    articles.append(x)
            s = f.readline()
    return articles


def make_urls(article_numbers):
    pattern = 'https://www.defense.gov/News/Contracts/Contract-View/Article/{}/'
    urls = [pattern.format(x) for x in article_numbers]
    return urls


def main(article_path, cities_path, save_path, ):
    errorpath = save_path + '.err.txt'

    if os.path.exists(save_path):
        os.remove(save_path)
    if os.path.exists(errorpath):
        os.remove(errorpath)
    tpe = ProcessPoolExecutor(5)
    urls = make_urls(load_article_numbers(article_path))
    futures = []
    for u in urls:
        fut = tpe.submit(ArticleScraper, u, cities_path)
        futures.append(fut)

    total_bytes = 0

    for fut in tqdm(as_completed(futures), total=len(urls)):
        r = fut.result()  #type: ArticleScraper
        r.save(save_path)
        r.save_errors(errorpath)
        total_bytes += r.bytes_processed

    print('Total bytes: {}'.format(total_bytes))


class ScrapingError(ValueError):
    pass


if __name__ == '__main__':
    main(
        '/Users/chris/PycharmProjects/defense_spending/data/scraping/article_numbers.txt',
        '/Users/chris/PycharmProjects/defense_spending/src/defense_scraper/resources/us_cities_states_counties.csv',
        '/Users/chris/PycharmProjects/defense_spending/data/scraping/articles.csv'
    )