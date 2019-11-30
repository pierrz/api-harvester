# =================================================
# =================================================
# SETTINGS

# Dependencies
import requests, pprint, json, os, ssl
from datetime import datetime
from time import time
from pathlib import Path #, PurePath
from lxml import etree
from io import StringIO, BytesIO
import aiohttp, asyncio
# from aiohttp import web

# Functions
pp = pprint.PrettyPrinter(indent=4)

# Main parameters
from config import Config
output_dir = Config.output_dir

# main parameters
n_pagin = Config.n_pagin
pagin = Config.pagin
baseurl = Config.baseurl
trail_url = Config.trail_url
auth = Config.auth


# python updater.py --table_name 'schema.update_table'
# file_path = sys.argv[1]

# =================================================
# NYPL API Settings

# custom parameters
# s_terms = 'Farm Security Administration Photographs'
# s_terms = 'still image manhattan street portrait 1960 photograph '
s_terms = "Photographs"
adv_s_terms = '&field=genre'
# coll_id = 'e5462600-c5d9-012f-a6a3-58d385a7bc34'  # Farm Security Administration Photographs
# coll_id = 'a301da20-c52e-012f-cc55-58d385a7bc34'  # Photographic views of New York City, 1870's-1970's
coll_id = '439afdd0-c62b-012f-66d1-58d385a7bc34'  # Detroit Publishing Company postcards

# /!\ URLs
coll_url = baseurl + 'collections/' + coll_id + '?' + pagin
full_url = baseurl + 'items' + trail_url + s_terms + '&' + pagin + adv_s_terms
item_url = baseurl + 'items/mods_captures/'  # item_details


# =================================================
# =================================================
# CODE

# ---------------
# Utilities
def timestamp():
    return datetime.utcnow().isoformat()


def create_logfile(log_path):
    """
    Creates a (txt) log file
    """
    if os.path.isfile(log_path):
        os.remove(log_path)
    log_stream = open(log_path, 'w+')
    return log_stream


def write_to_log(log_path, message):
    """
    Writes to (txt) log file
    """
    with open(log_path, 'a') as log:

        if isinstance(message, str):
            mes_str = '\n' + '/!\\ ' + message
            log.write(mes_str)

        elif isinstance(message, list):
            mes_main = '\n' + '/!\\ ' + message[0]
            log.write(mes_main)
            for p in message[1:]:
                mes_p = '\n' + '\t' + ' - ' + p
                log.write(mes_p)


# ---------------
# File output
def write_json_f(data, filename, output_dir):
    json_f = Path(output_dir, filename + '.json')
    with open(json_f, 'w') as f:
        json.dump(data['response'], f)
        log_res = data['response']['headers']['message'] #['$']
        # pp.pprint(data['response']['headers']['message'])
        json_mes = "- json file created '" + filename + "' --> " + str(log_res)
        print(json_mes)
        write_to_log(log_path, json_mes)


def write_xml_f(data, filename, output_dir):
    xml_f = Path(output_dir, filename + '.xml')
    print(data)
    # tree = etree.fromstring(bytes(data, encoding='utf-8'))
    # tree = etree.parse(data)
    # tree = ElementTree.fromstring(bytes(data, encoding='utf-8'))
    tree = etree.parse(BytesIO(data))
    meta_xml = tree.findall('response')[0]
    xml_data = etree.tostring(meta_xml).decode('UTF-8')
    with open(xml_f, "w") as f:
        f.write(xml_data)
        log_res = tree.findall('response/headers/message')[0].text
        xml_mes = '- xml file created --> ' + log_res
        print(xml_mes)
        write_to_log(log_path, xml_mes)


# ---------------
# Main functions
async def fetch(session, url, mode):
    async with session.get(url, ssl=ssl.SSLContext(), headers={'Authorization': auth}) as response:
        # print(response)
        if mode == 'json':
            return await response.json()
        else:
            # return await response.content     # tryouts for xml data
            return await response


async def fetch_all(urls, loop, mode):
    async with aiohttp.ClientSession(loop=loop) as session:
        results = await asyncio.gather(*[fetch(session, url, mode) for url in urls], return_exceptions=True)
        return results


def fetch_pages():
    page = 1
    n_pages = 1
    list_urls = list()
    while page <= n_pages:

        # page urls
        if page > 1:
            page_url = '{}&page={}'.format(full_url, str(page))
        else:
            page_url = full_url
        # print(page_url)

        # prepare page numbers
        if page == 1:
            data = requests.get(page_url, headers={'Authorization': auth}).json()['nyplAPI']
            total_results = int(data['response']['numResults'])
            n_pages = int(data['request']['totalPages'])

        # iterate to next page
        list_urls.append(page_url)
        page += 1

    return list_urls, n_pages, total_results


def get_items(page_dir, loop):
    # fetch all items details per page
    n_page = 0
    with os.scandir(page_dir) as it:
        for page in it:
            if not page.name.startswith('.') and page.is_file():
                with open(os.path.realpath(page), "r") as read_file:

                    # xml/json urls list
                    n_page += 1
                    page_data = json.load(read_file)
                    item_urls = list()
                    item_urls_xml = list()
                    print('==> GET ITEMS FOR PAGE ' + str(n_page))
                    for el in page_data['result']:
                        item_urls.append(item_url + el['uuid'])
                        item_urls_xml.append(item_url + el['uuid'] + '.xml')

                    # get json data
                    json_data = loop.run_until_complete(fetch_all(item_urls, loop, 'json'))
                    for p in range(len(json_data)):
                        pos = p + (n_page - 1) * n_pagin
                        filename = 'item_' + str(pos) + '_page_' + str(n_page)
                        try:
                            write_json_f(json_data[p]['nyplAPI'], filename, json_dir)
                        except Exception as e:
                            print(e.message)

                    # # get xml data
                    # data = loop.run_until_complete(fetch_all(item_urls_xml[:20], loop, 'xml'))
                    # for p in range(len(data)):
                    #     pos = p + (n_page - 1) * n_pagin
                    #     print('==> WRITE XML ITEM ' + str(pos))
                    #     filename = 'item_' + str(pos) + '_page_' + str(n_page)
                    #     write_xml_f(data[p], filename, xml_dir)


# ---------------
# Init
if __name__ == '__main__':

    # init asyncio
    start = time()
    loop = asyncio.get_event_loop()

    # prepare folders
    run_dir = Path(output_dir, timestamp())
    page_dir = Path(run_dir, 'data', 'pages')
    xml_dir = Path(run_dir, 'data', 'xml')
    json_dir = Path(run_dir, 'data', 'json')
    log_dir = Path(run_dir, 'log')
    log_path = Path(log_dir, 'log.txt')
    for f in [run_dir, page_dir, xml_dir, json_dir, log_dir]:
        Path(f).mkdir(parents=True, exist_ok=True)
    log_stream = create_logfile(log_path)

    # get page urls
    print('\n==> GET PAGES URL')
    res_1 = fetch_pages()
    page_urls = res_1[0]
    n_pages = res_1[1]
    total = res_1[2]

    # logs
    mes_res = '\n{:,}'.format(total) + " items retrieved from the search '" + s_terms + "'"
    mes_pages = '\n{:,}'.format(n_pages) + " pages to fetch ..."
    print(mes_res, mes_pages)
    write_to_log(log_path, [mes_res, mes_pages])

    # fetch/write page data
    print('==> GET PAGES DATA')
    data = loop.run_until_complete(fetch_all(page_urls, loop, 'json'))
    for p in range(len(data)):
        filename_page = 'page_' + str(p+1)
        write_json_f(data[p]['nyplAPI'], filename_page, page_dir)

    # fetch/write all items details per page
    get_items(page_dir, loop)

    # end program/log
    log_stream.close()
    end = time()
    duration = end - start
    print('\n--> Process took ' + str(int(duration / 60)) + ' min. ' + '{:10.2f}'.format(duration % 60) + ' sec.')
