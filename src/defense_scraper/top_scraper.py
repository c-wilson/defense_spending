import requests
import re
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from tqdm import tqdm


YEARS = range(2014, 2019)
MONTHS = range(1, 13)

class TopScraper:
    """Scrapes DoD contracts website (not archive) for contract article pages."""
    def __init__(self, url):
        """
        Gets and scrapes provided URL for links to articles.
        :param url:
        :return:
        """

        expression = r'https://www.defense.gov/News/Contracts/Contract-View/Article/([^/\">]*)'
        pg = requests.get(url)

        it = re.finditer(expression, pg.text)

        results = []

        for i in it:
            results.append(i.group(1))
        self.results = results

        return


    def save(self, fn):
        with open(fn, 'a') as f:
            for ln in self.results:
                f.write(ln)
                f.write(', ')
            f.write('\n')
        return


def main(filename):
    tpe = ThreadPoolExecutor(8)
    pattern = 'https://www.defense.gov/News/Contracts/Year/{}/Month/{}/'
    futures = []
    with open(filename, 'w') as _:
        pass

    for y in YEARS[:]:
        for m in MONTHS:
            pattern_ = pattern.format(y, m)
            future = tpe.submit(TopScraper, pattern_)
            futures.append(future)

    ex_count = 0
    for f in tqdm(as_completed(futures), total=len(futures)):  # type: Future
        try:
            result = f.result()
            # print(result)
            if result:
                result.save(filename)
        except Exception as e:
            print(e)
            ex_count += 1
    print('Exceptions: {}'.format(ex_count))


if __name__ == '__main__':
    main('/Users/chris/PycharmProjects/defense_spending/data/scraping/article_numbers.txt')
