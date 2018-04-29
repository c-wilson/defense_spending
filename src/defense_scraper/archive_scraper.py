from defense_scraper.article_scraper import *

#TODO: this needs work!


class ArchiveScraper(ArticleScraper):

    branches = {
        "ARMY",
        "NAVY",
        "MISSILE DEFENSE AGENCY",
        "AIR FORCE",
        "DEFENSE LOGISTICS AGENCY",
        "DEFENSE ADVANCED RESEARCH PROJECTS AGENCY",
        "WASHINGTON HEADQUARTERS SERVICES",
        "U.S. TRANSPORTATION COMMAND",
        "DEFENSE INFORMATION SYSTEMS AGENCY",
        "DEFENSE LOGISITICS AGENCY",
        "DEFENSE THREAT REDUCTION AGENCY",
        "JOINT IMPROVISED EXPLOSIVE DEVICE DEFEAT ORGANIZATION",
        "SPECIAL OPERATIONS COMMAND",
        "DEFENSE HEALTH AGENCY",
        "DEFENSE COMMISSARY AGENCY",
        "DEFENSE CONTRACT MANGEMENT AGENCY",
        "DEFENSE HUMAN RESOURCES ACTIVITY",
        "DEFENSE LOGISTIC AGENCY",
        "DEPARTMENT OF DEFENSE EDUCATION ACTIVITY",
        "DEFENSE HUMAN RESOURCES ACTIVITIES",
        "DEFENSE FINANCE AND ACCOUNTING SERVICE",
        "DEFENSE INTELLIGENCY AGENCY",
        "DEFENSE ADVANCE RESEARCH PROJECTS AGENCY",
        "SPECIAL OPERATIONS COMMMAND",
        "U.S TRANSPORTATION COMMAND",
        "U.S. SPECIAL OPERATIONS COMMAND",
        "UNIFORMED SERVICES UNIVERSITY OF THE HEALTH SCIENCES",
        "JOINT IMPROVISED-THREAT DEFEAT ORGANIZATION",
        "DEFENSE INFORMATION SYSTEMS AGENCY",
        "DEFENSE SECURITY SERVICE",
    }

    def parse_date(self, soup: BeautifulSoup):
        span = soup.find('title', )
        txt = span.text
        sp = txt.split(' for ')[1]
        date = dateparser.parse(sp)
        return date

    def parse_graphs(self, soup: BeautifulSoup):
        """
        Finds paragraphs with contract information.
        :param soup:
        :return:
        """

        text_span = soup.find_all('div', {'class': ['PressOpsContentBody']})[1]
        graphs = text_span.find_all('p')  # {'style': ''}
        branch = 'unk'
        transactions = []
        for p in graphs:  #type: bs4.element.Tag
            if p.text.strip() in self.branches:
                branch = p.text.strip()
            else:
                txt = p.text.replace('\r\n', ' ').strip()
                if txt and not txt.startswith('*'):  # todo: are there other exclusion criteria?
                    try:
                        data = self.parse_spending_paragraph(txt)
                        data['branch'] = branch
                        data['date'] = self.date
                        transactions.append(data)
                    except:
                        self.errors.append(txt)
        return transactions

    def parse_spending_paragraph(self, text: str):
        result = {
            'cities': [],
            'states': [],
            'amount': 0,
            'error': 0
        }

        text = text.replace('\r\n', ' ')

        pre_dollar, post_dollar, *rest = text.split('$')
        last_city = -10
        for i, s_dirty in enumerate(pre_dollar.split(',')):
            s = s_dirty.strip('*').strip()
            if i - last_city != 1 and s.lower() in self._cities:
                result['cities'].append(s)
                last_city = i
            elif i - last_city == 1:
                if s.lower() in self._states:
                    result['states'].append(s)
                else:
                    for state in self._states:
                        if state in s.lower():
                            result['states'].append(state)

        if len(result['cities']) != len(result['states']) or not result['cities']:
            raise ScrapingError

        amount_str_dirty, *rest = post_dollar.split(' ')
        amount_str, *rest = amount_str_dirty.split('.')  # for cents which are in some...
        result['amount'] = int(amount_str.replace(',', ''))

        return result

    def __init__(self, *args, **kwargs):
        otherstates = ('ariz.', 'calif.', 'conn.', 'ga.', 'wis.', 'mass.', 'n.j.', 'va.', 'md.', 'okla.',
                       'fla.', 'oklahoma.', 'pa.', 'n.m.', 'mo.', 'ill.', 'n.y.', 'mich.', 'wash.', 'tenn.',
                       'ala.', 'n.c.', 'neb.', 'mont.', 'miss.', 'del.', 'ind.', 'colo.', 'ore.', 'minn.',
                       'co.', 'ok.', 'kan.', 'la.', 's.c.', 'mich.', 'ky.')
        for o in otherstates:
            self._states.add(o)
        super(ArchiveScraper, self).__init__(*args, **kwargs)


def make_urls(article_numbers):
    pattern = 'http://archive.defense.gov/Contracts/Contract.aspx?ContractID={}'
    urls = [pattern.format(x) for x in article_numbers]
    return urls


def main(cities_path, save_path, ):
    errorpath = save_path + '.err.txt'

    if os.path.exists(save_path):
        os.remove(save_path)
    if os.path.exists(errorpath):
        os.remove(errorpath)


    tpe = ProcessPoolExecutor(5)
    urls = make_urls(range(449, 5319))
    # urls = make_urls(range(4400, 5319))
    # for u in urls:
    #     # print(u)
    #     a = ArchiveScraper(u, cities_path)


    futures = []
    for u in urls:
        fut = tpe.submit(ArchiveScraper, u, cities_path)
        futures.append(fut)

    total_bytes = 0

    for fut in tqdm(as_completed(futures), total=len(urls)):
        try:
            r = fut.result()  #type: ArchiveScraper
            r.save(save_path)
            r.save_errors(errorpath)
            total_bytes += r.bytes_processed
        except Exception as e:
            # raise e
            # print('EXCEPTION')
            pass


    print('Total bytes: {}'.format(total_bytes))





if __name__ == '__main__':
    main(
        '/Users/chris/PycharmProjects/defense_spending/src/defense_scraper/resources/us_cities_states_counties.csv',
        '/Users/chris/PycharmProjects/defense_spending/data/scraping/archives.csv'
    )