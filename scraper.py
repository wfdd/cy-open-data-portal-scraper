
import asyncio
from datetime import datetime as dt
import sqlite3
from urllib.parse import urlparse, urljoin
from uuid import UUID

import aiohttp
from logbook import StderrHandler as StderrLogger, error, notice
from lxml.html import document_fromstring as _parse_html
import uvloop

base_url = 'http://www.data.gov.cy/'

labels = (('Πηγή Ενημέρωσης:', 'source'),
          ('Χρέωση:', 'fee'),
          ('Επίπεδο Επεξεργασίας:', 'degree_to_which_processed'),
          ('Προστέθηκε στο data.gov.cy:', 'date_first_added'),
          ('Άδεια Χρήσης:', 'license'),
          ('Συχνότητα Επικαιροποίησης:', 'update_frequency'),
          ('Περίοδος Αναφοράς:', 'reporting_period'),
          ('Γεωγραφική Κάλυψη:', 'geographic_coverage'),
          ('Σύνδεσμος Επικοινωνίας:', 'government_contact'),
          ('e-mail:', 'email'),)


def parse_html(text):
    html = _parse_html(text
                       .replace('<?xml version="1.0" encoding="UTF-8"?>', ''))
    html.make_links_absolute(base_url)
    return html


def extract_metadata(html):
    for label, _ in labels:
        yield html.xpath('string(//b[text() = "{}"]/..)'
                         .format(label)).replace(label, '').strip() or None


async def scrape_item(formats, category, item_url, list_url,
                      get):
    async with get(item_url) as item_resp:
        html = parse_html(await item_resp.text())
    return (UUID(hex=urlparse(item_url).path.rpartition('/')[-1],
                 version=4).hex,
            html.xpath('string(//*[@class = "datasethead"])').strip(),
            formats,
            category,
            *extract_metadata(html),
            item_url,
            list_url)


async def scrape_list(url, get):
    datasets = []
    while True:
        async with get(url) as list_resp:
            text = await list_resp.text()
        html = orig_html = parse_html(text)
        # '[Replication or Save Conflict]' warnings add an extra column,
        # complicating the parsing.  The 'Collapse' parameter gets rid of those
        # but it also messes up the pagination (because why wouldn't it),
        # so we're left with having to download the same page twice
        if '[Replication or Save Conflict]' in text:
            notice("'[Replication or Save Conflict]' in {}", url)
            async with get(url + '&Collapse=') as list_resp:
                html = parse_html(await list_resp.text())

        datasets.extend([
            (';'.join(filter(None,
                             (i.text_content().strip() for i in
                              r.xpath('.//*[starts-with(@class, "format-box")]'))
                             )) or None,
             r.xpath('string(.//*[@class = "datasetcat"])').strip(),
             r.xpath('string(.//a[@class = "datasethead"]/@href)'),
             url) for r in html.xpath('''\
//font[@class = "datasetresults"]
/following-sibling::table[1]/tr[position() > 1]''')])
        try:
            url, = orig_html.xpath('//a[contains(string(.), "Επόμενη")]/@href')
        except ValueError:
            return datasets


async def gather_datasets(get):
    async with get(base_url) as index_resp:
        html = parse_html(await index_resp.text())
    sections = (urljoin(index_resp.url,
                        l.replace('location.href=', '').strip("'")) for l in
                html.xpath('//div[@class = "AccordionPanelTab"]/a/@onclick'))
    datasets = await get.gather(scrape_list(s, get) for s in sections)
    datasets = await get.gather(scrape_item(*i, get)
                                for l in datasets for i in l)
    return int(html.xpath('string(//span[contains(string(.), "datasets")])')
               .replace('datasets', '').strip()), \
           datasets


def prepare_getter(loop, session):
    class Get:
        event = asyncio.Event(loop=loop)
        event.set()  # Flip the inital state to True
        semaphore = asyncio.Semaphore(8, loop=loop)

        def __init__(self, url):
            self.url = url

        async def __aenter__(self):
            for i in range(3):
                await self.event.wait()
                try:
                    async with self.semaphore:
                        self.resp = await session.get(self.url)
                        return self.resp
                except aiohttp.errors.ClientResponseError as e:
                    if i == 2:
                        raise       # Giving up after the third attempt
                    # Pausing all requests since they're all going to
                    # the same server and are (probably) gonna be
                    # similarly rejected
                    await self._pause(e)

        async def __aexit__(self, *a):
            self.resp.close()

        async def _pause(self, e):
            if self.event.is_set():  # Debounce repeated failures
                self.event.clear()
                error('Received {!r} on {}.  Retrying in 5s', e, self.url)
                await asyncio.sleep(5, loop=loop)
                self.event.set()

        @staticmethod
        async def gather(iterable):
            return await asyncio.gather(*iterable, loop=loop)

    return Get


def main(loop):
    with StderrLogger(), \
            aiohttp.ClientSession(loop=loop) as session, \
            sqlite3.connect('data.sqlite') as conn:
        dataset_count, datasets = loop.run_until_complete(
            gather_datasets(prepare_getter(loop, session)))

        now = dt.now().isoformat()
        conn.execute('''\
CREATE TABLE IF NOT EXISTS data
(id, title, formats, category, source, fee, degree_to_which_processed,
 date_first_added, license, update_frequency, reporting_period,
 geographic_coverage, government_contact, email, item_url, list_url,
 meta__last_updated, UNIQUE (id))''')
        conn.executemany('''\
INSERT OR REPLACE INTO data VALUES
(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (d + (now,) for d in datasets))
        datasets_in_db, = conn.execute('SELECT count(*) FROM data').fetchone()
        if datasets_in_db != dataset_count:
            notice('Scraped {} datasets in total'
                   ' though {} are reported to exist',
                   datasets_in_db, dataset_count)

if __name__ == '__main__':
    main(uvloop.new_event_loop())
