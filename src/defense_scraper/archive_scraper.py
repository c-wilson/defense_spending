from defense_scraper.article_scraper import *
from glob import glob
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
        # print(date)
        return date

    def parse_graphs(self, soup: BeautifulSoup):
        """
        Finds paragraphs with contract information.
        :param soup:
        :return:
        """

        text_span = soup.find_all('div', {'class': ['PressOpsContentBody']})[1]
        graphs = text_span.find_all('p')  # {'style': ''}

        if not graphs:
            spn = soup.find('span', {'id': 'ctl00_cphBody_ContentContents_lblArticleContent'})
            graphs = spn.find_all('div', {'style': 'MARGIN: 0in 0in 0pt; TEXT-INDENT: 0.5in'})

        if not graphs:
            graphs = text_span.find_all('div')

        branch = 'unk'
        transactions = []

        for p in graphs:  #type: bs4.element.Tag
            if p.text.strip() in self.branches:
                branch = p.text.strip()
            else:
                txt = p.text.replace('\r\n', ' ').strip()
                for el in txt.split('Â'):
                    # print(el)

                    if el and not el.startswith('*') and '$' in el and len(el) > 10:  # todo: are there other exclusion criteria?
                        try:
                            # print(el)
                            data = self.parse_spending_paragraph(el)
                            data['branch'] = branch
                            data['date'] = self.date
                            transactions.append(data)
                        except ScrapingError:
                            self.errors.append(el)
        return transactions

    def __init__(self, *args, **kwargs):
        super(ArchiveScraper, self).__init__(*args, **kwargs)


def make_urls(article_numbers):
    pattern = 'http://archive.defense.gov/Contracts/Contract.aspx?ContractID={}'
    urls = [pattern.format(x) for x in article_numbers]
    return urls


def main(urls_path, city_states_json_path, state_names_json_path, save_path, ):
    errorpath = save_path + '.err.txt'

    if os.path.exists(save_path):
        os.remove(save_path)
    if os.path.exists(errorpath):
        os.remove(errorpath)

    with open(urls_path, 'r') as f:
        u_text = f.read()
        urls = u_text.split(',')

    for u in tqdm(urls):
        try:
            a = ArchiveScraper(u, city_states_json_path, state_names_json_path)
            a.save(save_path)
            a.save_errors(errorpath)
        except Exception as e:
            print(u)
            print(e)
    # tpe = ProcessPoolExecutor(3)
    # futures = []
    # for u in urls:
    #     fut = tpe.submit(ArchiveScraper, u, city_states_json_path, state_names_json_path)
    #     futures.append(fut)
    #
    # total_bytes = 0
    #
    # for fut in tqdm(as_completed(futures), total=len(urls)):
    #
    #     r = fut.result()  #type: ArchiveScraper
    #     r.save(save_path)
    #     r.save_errors(errorpath)
    #     total_bytes += r.bytes_processed
    #
    #
    # print('Total bytes: {}'.format(total_bytes))


class ArchiveScraperLocal(ArchiveScraper):

    def article_getter(self, filename):
        with open(filename, 'r') as f:
            txt = f.read()
        return BeautifulSoup(txt, 'html5lib')

    def save_date(self, path):
        with open(path, 'a') as f:
            f.writelines(str(self.date))
        return


def main_local(directory, city_states_json_path, state_names_json_path, save_path):
    errorpath = save_path + '.err.txt'
    if os.path.exists(save_path):
        os.remove(save_path)
    if os.path.exists(errorpath):
        os.remove(errorpath)

    pattern = os.path.join(directory, '*.html')
    filenames = glob(pattern)
    filenames.sort()


    cities, states = parse_cities(city_states_json_path, state_names_json_path)

    for f in tqdm(filenames):
        try:
            a = ArchiveScraperLocal(f, cities, states)
            a.save(save_path)
            # a.save_date(save_path)
        except Exception as e:
            # print(u)
            print(e)



if __name__ == '__main__':
    # main(
    #     '/Users/chris/PycharmProjects/defense_spending/data/scraping/archive_urls.txt',
    #     '/Users/chris/PycharmProjects/defense_spending/src/defense_scraper/resources/city_states.json',
    #     '/Users/chris/PycharmProjects/defense_spending/src/defense_scraper/resources/state_names.json',
    #     '/Users/chris/PycharmProjects/defense_spending/data/scraping/archives.csv'
    # )

    main_local(
        '/Users/chris/PycharmProjects/defense_spending/data/scraping/archive',
        '/Users/chris/PycharmProjects/defense_spending/src/defense_scraper/resources/city_states.json',
        '/Users/chris/PycharmProjects/defense_spending/src/defense_scraper/resources/state_names.json',
        '/Users/chris/PycharmProjects/defense_spending/data/scraping/archive_.csv'
    )
    # cities, states = parse_cities(
    #     '/Users/chris/PycharmProjects/defense_spending/src/defense_scraper/resources/city_states.json',
    #     '/Users/chris/PycharmProjects/defense_spending/src/defense_scraper/resources/state_names.json'
    # )
    # a = ArchiveScraperLocal(
    #     '/Users/chris/PycharmProjects/defense_spending/data/scraping/archive/pg_00000.html',
    #     cities, states
    # )

    #
    # print(a.data)
    # print(a.errors)