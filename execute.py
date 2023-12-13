import pandas as pd
import numpy
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import bs4
import datetime
import re
import json
import time
import cloudscraper
from dateutil.relativedelta import relativedelta

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def newssite_metadata_mining(SOURCE='detik', SECTION='finance',
                            PAGE_SLEEP=0.25,
                            DATE_SLEEP=1,
                            START_DATE=(2005,1,1)):
    '''Perform detik.com data mining operations.
    Get all articles metadata from 1 January 2005
    to come. Saved metadata : `title`, `url`, `timestamp`.
    
    Capable to automatically resume progress from last
    known timestamp (daily accuracy).
    
    All of the metadata saved in file named :
    `detik_{SECTION}_results.json`, which section is
    kwargs from this function.
    
    Saved file format is *.json, with structure:
    - timestamp (daily-resolution)
        - lists
            - title (string)
            - url (string)
            - timestamp (minute-resolution) (int)
            
    Print daily current progress.
    
    Kwargs:
        SECTION: string, optional.
            detik.com indexed section. Valid argumens is:
            `news`: Detik News
            `edu`: Detik Edu
            `finance`: Detik Finance
            `hot`: Detik Hot
            `inet`: Detik Inet
            `sport`: Detik Sport
            `oto`: Detik Oto
            `travel`: Detik Travel
            `sepakbola`: Detik Sepakbola
            `food`: Detik Food
            `health`: Detik Health
            `wolipop`: Wolipop
            
        REQUEST_URL: format string with placeholder.
            Optional. URL used to get indexed articles data
            
        PAGE_SLEEP: float, optional.
            How much time intevral (in seconds) put between
            page in particular date.
            
        DATE_SLEEP: float, optional.
            How much time interval (in seconds) put between
            date.
            
    Returns:
        None.
    '''
    article_metadata_file = f'{SOURCE}_{SECTION}_results.json'

    # Read current progress from article_metadata_file
    # or create a new blank dictionary if not exist
    try :
        # Try to read if the particular file exists
        with open(article_metadata_file, 'r') as f:
            article_metadata_dict = json.load(f)

        # Check last mining progress
        all_date_list = list(article_metadata_dict.keys())
        all_date_list.sort(reverse=True)
        start_date = datetime.datetime.fromtimestamp(float(all_date_list[0]))

    except (FileNotFoundError, IndexError):
        # If file not found, create a new blank file
        article_metadata_dict = {}

        # Configure start date
        # This is hard-coded, based on initial findings
        # that most of the indexed content started from Jan 1, 2005
        start_date = datetime.datetime(*START_DATE)

    end_date = datetime.datetime.now()
    day_range = end_date - start_date

    for i in range(day_range.days):
        # Track time to complete all articles in the current date
        tick = datetime.datetime.now()
        
        # Assign variables
        current_date = start_date + datetime.timedelta(days=i)
        page_crawl = True
        page_number = 1
        
        total_articles = 0
        while(page_crawl):
            try:
                # Perform data mining
                contents = process_switcher(SOURCE, SECTION, page_number, current_date)

                # Check contents validity in this particular page number
                if len(contents) == 0:
                    page_crawl = False
                    break
                    
                # Add date record to dictionary (in timestamp format)
                article_metadata_dict[current_date.timestamp()] = []

                # Loop through all available contents in particular page
                for content in contents:
                    # Assign result(s) into dictionary
                    temp_article_result = {}
                    for key in content.keys():
                        temp_article_result[key] = content[key]
                    article_metadata_dict[current_date.timestamp()].append(temp_article_result)

                    # Calculate total articles for this particular date
                    total_articles+=1

            except ValueError:
                time.sleep(5)
                continue

            except TypeError:
                time.sleep(5)
                continue

            page_number+=1

            # Sleep during interval between page
            time.sleep(PAGE_SLEEP)

        # Sleep during interval between date
        time.sleep(DATE_SLEEP)

        # Store current progress into file
        with open(article_metadata_file, 'w') as f:
            json.dump(article_metadata_dict, f)
            
        # Track time to complete all articles in the current date
        tock = datetime.datetime.now()

        # Print current progress
        print(f'{SOURCE} {SECTION} {i+1}/{day_range.days} : {current_date} - {total_articles} articles - time {tock-tick}')

def month_formatter(month_text):
    '''Convert short month spell from `id` to `en`.
    '''
    if month_text == 'Mei':
        return 'May'
    elif month_text == 'Agu':
        return 'Aug'
    elif month_text == 'Okt':
        return 'Oct'
    elif month_text == 'Des':
        return 'Dec'
    else:
        return month_text

def process_switcher(SOURCE, SECTION, page_number, current_date):
    '''
    Switch between various mining processor and
    return crawled contents in dictionary format
    '''
    identifier = f'{SOURCE}_{SECTION}'
    if ((identifier == 'detik_news') or
        (identifier == 'detik_finance') or
        (identifier == 'detik_hot') or
        (identifier == 'detik_sport') or
        (identifier == 'detik_oto')):
        contents = detik_general_processor(SECTION, page_number, current_date)

    elif identifier == 'detik_edu':
        contents = detik_edu_processor(SECTION, page_number, current_date)
        
    elif identifier == 'detik_inet':
        contents = detik_inet_processor(SECTION, page_number, current_date)
        
    elif identifier == 'detik_travel':
        contents = detik_travel_processor(SECTION, page_number, current_date)
    
    elif ((identifier == 'detik_food') or
          (identifier == 'detik_health')):
        contents = detik_food_health_processor(SECTION, page_number, current_date)
    
    elif identifier == 'detik_wolipop':
        contents = detik_wolipop_processor(SECTION, page_number, current_date)
        
    elif SOURCE == 'kompas':
        contents = kompasdotcom_processor(SECTION, page_number, current_date)
        
    elif SOURCE == 'bisnis':
        contents = bisnisdotcom_processor(SECTION, page_number, current_date)
        
    elif SOURCE == 'kontan':
        contents = kontan_processor(SECTION, page_number, current_date)
        
    elif SOURCE == 'cnbc':
        # Try desktop version of the web
        try:
            contents = cnbcindonesia_processor(SECTION, page_number, current_date)
        # If the response is mobile version,
        # use this version of processor
        except IndexError:
            contents = cnbcindonesia_alt_processor(SECTION, page_number, current_date)
        
    return contents

def detik_general_processor(section, 
                            page, 
                            current_date, 
                            DATE_FORMAT='%m/%d/%Y',
                            REQUEST_URL='https://{0}.detik.com/indeks/{1}?date={2}'):
    '''
    Valid for finance, hot, news, sport, oto
    '''
    date_compatible_format = current_date.strftime(DATE_FORMAT)
    formatted_request_url = REQUEST_URL.format(section, page, date_compatible_format)
    res = requests.get(formatted_request_url, verify=False)

    # Create BeautifulSoup object for HTML
    # structure handling
    soup = bs4.BeautifulSoup(res.text)

    # Find all content tag : list-content__item
    contents = soup.find_all(attrs={'class':'list-content__item'})

    results = []
    for content in contents:
        temp_result = {'title':content.img['title'],
                       'url':content.a['href'],
                       'timestamp':int(content.find_all('span')[1]['d-time'])}
        results.append(temp_result)
    return results

def detik_edu_processor(section,
                        page,
                        current_date,
                        DATE_FORMAT='%m/%d/%Y',
                        REQUEST_URL = 'https://www.detik.com/{0}/indeks/{1}?date={2}'):
    return detik_general_processor(section, page, current_date, DATE_FORMAT, REQUEST_URL)

def detik_inet_processor(section,
                         page,
                         current_date,
                         DATE_FORMAT='%d-%m-%Y',
                         REQUEST_URL='https://{0}.detik.com/indeks/{1}?date={2}'):
    date_compatible_format = current_date.strftime(DATE_FORMAT)
    formatted_request_url = REQUEST_URL.format(section, page, date_compatible_format)
    res = requests.get(formatted_request_url, verify=False)

    # Create BeautifulSoup object for HTML
    # structure handling
    soup = bs4.BeautifulSoup(res.text)

    # Find all content tag : list-content__item
    contents = soup.find_all(attrs={'class':'list-content__item'})

    results = []
    for content in contents:
        temp_result = {'title':content.a['dtr-ttl'],
                       'url':content['i-link'],
                       'timestamp':int(bs4.BeautifulSoup(content['i-info']).span['d-time'])}
        results.append(temp_result)
    return results

def detik_travel_processor(section,
                           page,
                           current_date,
                           DATE_FORMAT='%m/%d/%Y',
                           REQUEST_URL='https://{0}.detik.com/indeks/{1}?date={2}'):
    date_compatible_format = current_date.strftime(DATE_FORMAT)
    formatted_request_url = REQUEST_URL.format(section, page, date_compatible_format)
    res = requests.get(formatted_request_url, verify=False)

    # Create BeautifulSoup object for HTML
    # structure handling
    soup = bs4.BeautifulSoup(res.text)

    # Find all content tag : list-content__item
    contents = soup.find_all(attrs={'class':'list__news--trigger'})

    results = []
    for content in contents:
        date_list_slice = (content.div.div.text).split(' ')[1:5]
        date_list_slice[1] = month_formatter(date_list_slice[1])
        datetime_formatted = datetime.datetime.strptime(' '.join(date_list_slice), '%d %b %Y %H:%M')

        temp_result = {'title':content.a['dtr-ttl'],
                       'url':content.a['href'],
                       'timestamp':int(datetime_formatted.timestamp())}
        results.append(temp_result)
    return results

def detik_food_health_processor(section,
                                page,
                                current_date,
                                DATE_FORMAT='%m/%d/%Y',
                                REQUEST_URL='https://{0}.detik.com/indeks/{1}?date={2}'):
    date_compatible_format = current_date.strftime(DATE_FORMAT)
    formatted_request_url = REQUEST_URL.format(section, page, date_compatible_format)
    res = requests.get(formatted_request_url, verify=False)

    # Create BeautifulSoup object for HTML
    # structure handling
    soup = bs4.BeautifulSoup(res.text)

    # Find all content tag : list-content__item
    contents = soup.find_all('article')

    results = []
    for content in contents:
        date_list_slice = (content.span.text).split(' ')[1:5]
        date_list_slice[1] = month_formatter(date_list_slice[1])
        datetime_formatted = datetime.datetime.strptime(' '.join(date_list_slice), '%d %b %Y %H:%M')

        temp_result = {'title':content.h2.text,
                       'url':content.a['href'],
                       'timestamp':int(datetime_formatted.timestamp())}
        results.append(temp_result)
    return results

def detik_wolipop_processor(section,
                            page,
                            current_date,
                            DATE_FORMAT='%m/%d/%Y',
                            REQUEST_URL='https://{0}.detik.com/indeks/{1}?date={2}'):
    date_compatible_format = current_date.strftime(DATE_FORMAT)
    formatted_request_url = REQUEST_URL.format(section, page, date_compatible_format)
    res = requests.get(formatted_request_url, verify=False)

    # Create BeautifulSoup object for HTML
    # structure handling
    soup = bs4.BeautifulSoup(res.text)

    # Find all content tag : list-content__item
    contents = soup.find_all(attrs={'class':'text'})

    results = []
    for content in contents:
        date_list_slice = (content.span.text).split(' ')[1:5]
        date_list_slice[1] = month_formatter(date_list_slice[1])
        datetime_formatted = datetime.datetime.strptime(' '.join(date_list_slice), '%d %b %Y %H:%M')

        temp_result = {'title':content.h3.a.text.replace('\n', '').strip(),
                       'url':content.a['href'],
                       'timestamp':int(datetime_formatted.timestamp())}
        results.append(temp_result)
    return results   

def kompasdotcom_processor(section, 
                           page, 
                           current_date, 
                           DATE_FORMAT='%Y-%m-%d'):
    '''
    Valid for `all`, `news`, `megapolitan`,
    `nasional`, `regional`, `global`, `tren`,
    `health`, `food`, `edukasi`, `money`,
    `tekno`, `lifestyle`, `homey`, `properti`,
    `bola`, `travel`, `otomotif`, `sains`,
    `hype`, `jeo`, `health`, `skola`, `stori`,
    `konsultasihukum`, `headline`, `terpopuler`,
    `sorotan`, `topik-pilihan`
    
    '''
    date_compatible_format = current_date.strftime(DATE_FORMAT)
    REQUEST_URL = f'https://indeks.kompas.com/?site={section}&date={date_compatible_format}&page={page}'

    res = requests.get(REQUEST_URL, verify=False)

    # Create BeautifulSoup object for HTML
    # structure handling
    soup = bs4.BeautifulSoup(res.text)

    # Find all content tag : list-content__item
    contents = soup.find_all(attrs={'class':'article__list'})

    results = []
    for content in contents:
        raw_date = content.find(attrs={'class':'article__date'}).text
        raw_date = raw_date.replace(',','').replace(' WIB', '')
        on_datetime = datetime.datetime.strptime(raw_date, '%d/%m/%Y %H:%M')
        temp_result = {'section':content.find(attrs={'class':'article__subtitle--inline'}).text,
                       'title':content.div.div.a.img['alt'],
                       'url':content.div.div.a['href'],
                       'timestamp':int(on_datetime.timestamp())}
        results.append(temp_result)
    return results

def bisnisdotcom_processor(section, 
                           page, 
                           current_date, 
                           DATE_FORMAT='%Y-%m-%d'):
    '''
    Section value : (section-label) -> (section-value)
    `Semua Kanal` : `0`
    `Market` : `194`
    `Finansial` : `5`
    `Ekonomi` : `43`
    `Kabar24` : `186`
    `Teknologi` : `277`
    `Lifestyle` : `197`
    `Entrepreneur` : `258`
    `Travel` : `222`
    `Sport` : `57`
    `Bola` : `392`
    `Otomotif` : `272`
    `Jakarta` : `382`
    `Bandung` : `548`
    `Banten` : `420`
    `Semarang` : `528`
    `Surabaya` : `526`
    `Bali` : `529`
    `Sumatra` : `527`
    `Kalimantan` : `406`
    `Sulawesi` : `530`
    `Papua` : `413`
    `Koran` : `242`
    `Infografik` : `547`
    `Ramadan` : `390`
    `Bisnis TV` : `551`
    '''
    date_compatible_format = current_date.strftime(DATE_FORMAT)
    REQUEST_URL = f'https://www.bisnis.com/index?c={section}&d={date_compatible_format}&per_page={page}'

    scraper = cloudscraper.create_scraper()
    res = scraper.get(REQUEST_URL)

    # Create BeautifulSoup object for HTML
    # structure handling
    soup = bs4.BeautifulSoup(res.text)

    # Contents length check
    content_length = soup.find_all(attrs={'class':'list-news', 'class':'indeks-new'})
    if len(content_length) > 0:
        # In empty page, the article placeholder may
        # filled with "Tidak ada berita", that send
        # false alarm into the system
        try:
            contents = soup.find_all(attrs={'class':'list-news', 'class':'indeks-new'})[0].find_all('li')
            results = []
            for content in contents:
                raw_date = (content.find(attrs={'class':'date'}).text).strip().replace(' WIB', '')
                on_datetime = datetime.datetime.strptime(raw_date, '%d %b %Y | %H:%M')
                temp_result = {'section':content.find(attrs={'class':'wrapper-description'}).div.a['href'],
                               'title':content.div.a['title'],
                               'url':content.div.a['href'],
                               'timestamp':int(on_datetime.timestamp())}
                results.append(temp_result)
            return results
        except AttributeError:
            return content_length
    else:
        return content_length

def kontan_processor(section, 
                     page, 
                     current_date, 
                     DATE_FORMAT='&tanggal=%d&bulan=%m&tahun=%Y',
                     ARTICLES_PER_PAGE=20):
    '''
    Section value : (section-label) -> (section-value)
    `Semua Artikel` : ``
    `Nasional` : `nasional`
    `Keuangan` : `keuangan`
    `Investasi` : `investasi`
    `Industri` : `industri`
    `Internasional` : `internasional`
    `Peluang Usaha` : `peluangusaha`
    `Personal Finance` : `personalfinance`
    `English` : `english`
    `Lifestyle` : `lifestyle`
    `Fokus` : `fokus`
    `Piala Eropa` : `pialaeropa`
    `Regional` : `regional`
    `Yangter` : `yangter`
    `Kesehatan` : `kesehatan`
    `Cari Tahu` : `caritahu`
    `Analisis` : `analisis`
    `Executive` : `executive`
    `Kolom` : `kolom`
    `Kilas Kementerian` : `kilaskementerian`
    `Infografik` : `infografik`
    `Insight` : `insight`
    `Cek Fakta` : `cekfakta`
    `Ads` : `ads`
    `seremonia` : `seremonia`
    `Native` : `native`
    `Adv` : `adv`
    `Export Expert` : `exportexpert`
    `Tabloid` : `tabloid`
    `Kilas Korporasi` : `kilaskorporasi`
    `Edsus` : `edsus`
    `Kontan TV` : `tv`
    `Stock Setup` : `stocksetup`
    `BelanjaOn` : `belanjaon`
    `News Setup` : `newssetup`
    `Film On` : `filmon`
    `Kiat On` : `kiaton`
    `Sport Setup` : `sportsetup`
    `momsmoney.id` : `momsmoneyid`
    
    '''
    date_compatible_format = current_date.strftime(DATE_FORMAT)
    REQUEST_URL = f'https://www.kontan.co.id/search/indeks?kanal={section}{date_compatible_format}&pos=indeks&per_page={(page - 1) * ARTICLES_PER_PAGE}'

    scraper = cloudscraper.create_scraper()
    res = scraper.get(REQUEST_URL)
    
    # Check if the status OK or NOT OK
    if not res.ok:
        print('Server error 500. Try 5 times with 10 sec interval to make sure its not a fluke.')
        res_ok = False
        for i in range(5):
            res = scraper.get(REQUEST_URL)
            if res.ok:
                res_ok = True
                break
            time.sleep(10)
        # If still `Server Error 500`, 
        # return blank lists.
        if not res_ok:
            return []

    # Create BeautifulSoup object for HTML
    # structure handling
    soup = bs4.BeautifulSoup(res.text)

    # Find all content tag : list-content__item
    contents = soup.find_all(attrs={'data-offset':'20'})

    results = []
    for content in contents:
        # Isolate datetime constructor from web
        date_list_slice = []
        date_list_slice_temp = (content.find(attrs={'class':'fs14','class':'ff-opensans'}).find(attrs={'class':'font-gray'}).text).split(' ')
        
        # Construct timedelta params
        search_keys = ('Tahun', 'Bulan', 'Hari', 'Jam', 'Menit')
        timedelta_constructor = []
        for search_key in search_keys:
            try:
                index = date_list_slice_temp.index(search_key)
                timedelta_constructor.append(int(date_list_slice_temp[index-1]))
            except ValueError:
                timedelta_constructor.append(0)
        
        # Calculate article publish date relative to current datetime
        date_now = datetime.datetime.now()
        news_date = date_now - relativedelta(years=timedelta_constructor[0], months=timedelta_constructor[1], days=timedelta_constructor[2], hours=timedelta_constructor[3], minutes=timedelta_constructor[4])
        
        temp_result = {'section':content.find(attrs={'class':'linkto-orange', 'class':'hrf-gede', 'class':'mar-r-5'}).a.text,
                       'title':content.find('h1').a.text,
                       'url':'https:' + content.a['href'],
                       'timestamp':int(news_date.timestamp())}
        results.append(temp_result)
    return results

def cnbcindonesia_processor(section, 
                            page, 
                            current_date, 
                            DATE_FORMAT='%Y/%m/%d'):
    '''
    Valid for all articles index only.    
    '''
    date_compatible_format = current_date.strftime(DATE_FORMAT)
    REQUEST_URL = f'https://www.cnbcindonesia.com/indeks/{page}?date={date_compatible_format}'

    scraper = cloudscraper.create_scraper()
    res = scraper.get(REQUEST_URL)

    # Create BeautifulSoup object for HTML
    # structure handling
    soup = bs4.BeautifulSoup(res.text)

    # Find all content tag : list-content__item
    contents = soup.find_all(attrs={'class':'gtm_indeks_feed'})
    contents = contents[0].find_all('li')

    results = []
    for content in contents:
        # Fetch URL, remove "https://"
        time_from_url = (content.article.a['href'])
        time_from_url = time_from_url.replace('https://', '')

        # Split by folder "/"
        time_from_url = time_from_url.split('/')

        # Get 3rd content in the folder (time indices)
        time_from_url = time_from_url[2]

        # Split by "-", to get year-month-day-hour-minute-seconds
        # published time of that article
        time_from_url = time_from_url.split('-')
        time_from_url = time_from_url[0]
        time_from_url = datetime.datetime.strptime(time_from_url, '%Y%m%d%H%M%S')       
        
        temp_result = {'section':content.find(attrs={'class':'label'}).text,
                       'title':content.find('h2').text,
                       'url':content.article.a['href'],
                       'timestamp':int(time_from_url.timestamp())}
        results.append(temp_result)
    return results

def cnbcindonesia_alt_processor(section, 
                                page, 
                                current_date, 
                                DATE_FORMAT='%Y/%m/%d'):
    '''
    *ALTERNATIVE VERSION*
    Fetch mobile version of the web
    that have different structure compared
    to the desktop version.
    
    Valid for all articles index only.    
    '''
    date_compatible_format = current_date.strftime(DATE_FORMAT)
    REQUEST_URL = f'https://www.cnbcindonesia.com/indeks/{page}?date={date_compatible_format}'

    scraper = cloudscraper.create_scraper()
    res = scraper.get(REQUEST_URL)

    # Create BeautifulSoup object for HTML
    # structure handling
    soup = bs4.BeautifulSoup(res.text)

    # Find all content tag : list-content__item
    contents = soup.find_all(attrs={'class':'list__item'})

    results = []
    for content in contents:
        # Fetch URL, remove "https://"
        time_from_url = content.a['href']
        time_from_url = time_from_url.replace('https://', '')

        # Split by folder "/"
        time_from_url = time_from_url.split('/')

        # Get 3rd content in the folder (time indices)
        time_from_url = time_from_url[2]

        # Split by "-", to get year-month-day-hour-minute-seconds
        # published time of that article
        time_from_url = time_from_url.split('-')
        time_from_url = time_from_url[0]
        time_from_url = datetime.datetime.strptime(time_from_url, '%Y%m%d%H%M%S')       
        
        temp_result = {'section':content.find(attrs={'class':'sub'}).text,
                       'title':content.find('h4').a.text,
                       'url':content.a['href'],
                       'timestamp':int(time_from_url.timestamp())}
        results.append(temp_result)
    return results


SITE_SOURCE = input('Enter news site source : ')
SITE_SECTION = input('Enter new site section : ')

if SITE_SOURCE == 'kompas':
    PAGE_SLEEP = 4
    DATE_SLEEP = 8
    START_DATE = (2013,5,1)

elif SITE_SOURCE == 'detik':
    PAGE_SLEEP = 10
    DATE_SLEEP = 15
    START_DATE = (2005,1,1)

elif SITE_SOURCE == 'bisnis':
    PAGE_SLEEP = 10
    DATE_SLEEP = 15
    START_DATE = (2010,12,1)

elif SITE_SOURCE == 'kontan':
    PAGE_SLEEP = 4
    DATE_SLEEP = 8
    START_DATE = (2011,1,2)

elif SITE_SOURCE == 'cnbc':
    PAGE_SLEEP = 4
    DATE_SLEEP = 8
    START_DATE = (2018,1,8)

newssite_metadata_mining(SOURCE=SITE_SOURCE, SECTION=SITE_SECTION,
                         PAGE_SLEEP=PAGE_SLEEP,
                         DATE_SLEEP=DATE_SLEEP,
                         START_DATE=START_DATE)
