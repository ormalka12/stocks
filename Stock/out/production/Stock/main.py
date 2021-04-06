#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
"""
save the output in some kind of RDBMS  and visualize the data with matplotlib/seaborn/bokhe or JS lib like D3/VisJS/GoogleCharts

Execute:
$ OUT='/tmp/csv1'; ./main.py > $OUT && sort -k4 -n -t, $OUT

'sort' flags exaplined:
    -k1 sorts by column 1
    -n sorts numerically instead of lexicographically (so "11" will not come before "2,3...")
    -t, sets the delimiter


Output example:
IL001103852393.,124.5,9,30.599999999999994
IL0011731457,4178.0,-1,4179.0

"""
from os import write, close, remove
from subprocess import Popen, PIPE, STDOUT
from undetected_chromedriver import ChromeOptions, Chrome
from selenium.common.exceptions import NoSuchElementException
from tempfile import mkstemp
import json, re
from random import shuffle
import psycopg2 as pg2

# connecting postgreDQL DB
connection = pg2.connect(database="stock",user="postgres",
                              password="password")

stocks = {}  # map of <ISIN, StockName>
chrome_options = ChromeOptions()
chrome_options.add_argument('--headless')
browser = Chrome(options=chrome_options)
browser.get('https://finance.sponser.co.il/finance/indices/all.php')
elems = browser.find_elements_by_xpath("//a[@href]")
for elem in elems:
    url = elem.get_attribute("href")
    r = re.search(r"(?<=https://www.sponser.co.il/Tag.aspx\?id=).*(?=&from=finance)", url)
    if r:
        stocks[r.group(0)] = elem.find_element_by_tag_name('span').text
browser.quit()
stock_ids = list(stocks.keys())
shuffle(stock_ids)


# proxy extension to print responses from specific URLs
tmp_fd, mitm_hook = mkstemp()
write(tmp_fd, f"""
import mitmproxy

def response(flow):
    _path = flow.request.path
    if _path.startswith('/api/statistics/tradeguidet1') or _path.startswith('/api/company/securitydata'):
        print(flow.response.content)
""".encode())
close(tmp_fd)

# start the proxy with our extension (download from "https://mitmproxy.org/downloads" and add to PATH)
pp = Popen(f'mitmdump -s {mitm_hook}'.split(' '), stdout=PIPE, stderr=STDOUT, close_fds=True)


def get_url(i):
    return f'https://www.tase.co.il/he/market_data/security/{i}/statistics'


chrome_options = ChromeOptions()
# chrome_options.add_argument('--headless')
chrome_options.add_argument('--proxy-server=localhost:8080')
chrome_options.add_argument('--ignore-ssl-errors=yes')
chrome_options.add_argument('--ignore-certificate-errors')
browser = Chrome(options=chrome_options)

resp_set = set()
i = 0
# iterator
cur = connection.cursor()
insert_query = """ 
    INSERT INTO stock (isin, last_rate, lowest_price, distance)
    VALUES(%s, %s, %s, %s) 
    ON CONFLICT (isin)
    DO 
    UPDATE SET last_rate = EXCLUDED.last_rate,
    lowest_price = EXCLUDED.lowest_price,
    distance = EXCLUDED.distance;
    """

while True: ############ why to infinity?
    try:  # read incoming traffic from proxy, 'unicode-escape' prevents from json deserialize to break
        line = pp.stdout.readline().decode('unicode-escape')
        if len(line) > 0:
            # print(line)
            cur_stock_id = stock_ids[i]
            if line.startswith('Error'):
                print(line)
            # assert proxy is on and start browsing
            elif line.startswith('Proxy server listening'):
                browser.get(get_url(cur_stock_id))
            elif line.startswith("b'{\"BaseRate"):  # start of the response from /api/company/securitydata
                d = json.loads(line[2:-2])
                LastRate, ISIN, SecurityLongName = d['LastRate'], d['ISIN'], d['SecurityLongName']
            # check for our delimiter & start of json
            elif line.startswith('b\'{"Items'):  # start of the response from /api/statistics/tradeguidet1
                # for some reason (bug?).. they make the statistics request twice
                # to overcome this issue we need check if we received the same response before moving to the next stock
                if line in resp_set:
                    i += 1
                    try:
                        browser.get(get_url(cur_stock_id))
                    except IndexError as no_more_stocks:
                        break
                else:
                    resp_set.add(line)
                    try:
                        LowestPrice = json.loads(line[2:-2])['Items'][0]['LowestPrice']
                    except IndexError:  # somtimes happens
                        LowestPrice = -1
                    distance_from_low = abs(LastRate - LowestPrice)
                    # print(f'{ISIN},{stocks[cur_stock_id]},{LastRate},{LowestPrice},{distance_from_low}')
                    print(f'{ISIN},{LastRate},{LowestPrice},{distance_from_low}')
                    record = (ISIN, LastRate, LowestPrice, distance_from_low)
                    cur.execute(insert_query, record)
    except KeyboardInterrupt:
        break
# cleanups
browser.quit()
pp.terminate()
remove(mitm_hook)
