"""Configuration for Common Crawl AWS pipeline."""

# AWS settings
AWS_REGION = "us-east-1"  # Same region as CC data on S3 = free access
ATHENA_DATABASE = "ccindex"
ATHENA_TABLE = "ccindex"
ATHENA_OUTPUT_BUCKET = "s3://kyivnotkiev-cc-results/"  # Create this bucket

# Common Crawl crawl selection: 2 per year for dense coverage
# Picked to spread across each year (early + late crawl)
CRAWL_IDS = [
    # 2013
    "CC-MAIN-2013-20", "CC-MAIN-2013-48",
    # 2014
    "CC-MAIN-2014-15", "CC-MAIN-2014-42",
    # 2015
    "CC-MAIN-2015-14", "CC-MAIN-2015-40",
    # 2016
    "CC-MAIN-2016-18", "CC-MAIN-2016-44",
    # 2017
    "CC-MAIN-2017-13", "CC-MAIN-2017-43",
    # 2018
    "CC-MAIN-2018-09", "CC-MAIN-2018-43",
    # 2019
    "CC-MAIN-2019-09", "CC-MAIN-2019-43",
    # 2020
    "CC-MAIN-2020-10", "CC-MAIN-2020-45",
    # 2021
    "CC-MAIN-2021-10", "CC-MAIN-2021-43",
    # 2022
    "CC-MAIN-2022-05", "CC-MAIN-2022-40",
    # 2023
    "CC-MAIN-2023-06", "CC-MAIN-2023-40",
    # 2024
    "CC-MAIN-2024-10", "CC-MAIN-2024-42",
    # 2025
    "CC-MAIN-2025-05", "CC-MAIN-2025-26",
    # 2026
    "CC-MAIN-2026-04", "CC-MAIN-2026-12",
]

# News and media domains to scan — English-language sites that cover Ukraine
# These are pre-filtered via the CC index to avoid scanning all 3B+ pages
NEWS_DOMAINS = [
    # Major English news
    "bbc.com", "bbc.co.uk", "cnn.com", "nytimes.com", "washingtonpost.com",
    "theguardian.com", "reuters.com", "apnews.com", "aljazeera.com",
    "france24.com", "dw.com", "euronews.com", "politico.com", "politico.eu",
    "theatlantic.com", "newyorker.com", "economist.com", "ft.com",
    "bloomberg.com", "wsj.com", "usatoday.com", "nbcnews.com", "abcnews.go.com",
    "cbsnews.com", "foxnews.com", "npr.org", "pbs.org", "vox.com",
    "time.com", "newsweek.com", "huffpost.com", "dailymail.co.uk",
    "independent.co.uk", "telegraph.co.uk", "mirror.co.uk", "express.co.uk",
    "sky.com", "itv.com", "channel4.com", "rte.ie",
    "cbc.ca", "globalnews.ca", "abc.net.au", "sbs.com.au", "rnz.co.nz",
    "thehill.com", "axios.com", "thedailybeast.com", "slate.com",
    "foreignpolicy.com", "foreignaffairs.com", "carnegieendowment.org",
    "brookings.edu", "cfr.org", "chathamhouse.org",
    # Wire services
    "afp.com", "ansa.it", "efe.com", "dpa-international.com",
    # International
    "scmp.com", "japantimes.co.jp", "straitstimes.com", "hindustantimes.com",
    "timesofindia.indiatimes.com", "ndtv.com",
    # Ukrainian English media
    "kyivindependent.com", "ukrinform.net", "ukrinform.ua",
    "pravda.com.ua", "unian.net", "unian.info",
    "interfax.com.ua", "liga.net", "hromadske.ua",
    # Eastern Europe / Russia coverage
    "rferl.org", "meduza.io", "themoscowtimes.com",
    "balkaninsight.com", "emerging-europe.com",
    # Travel & food (for food/landmark pairs)
    "lonelyplanet.com", "tripadvisor.com", "atlasobscura.com",
    "cntraveler.com", "theculturetrip.com", "tasteatlas.com",
    "seriouseats.com", "bonappetit.com", "saveur.com", "eater.com",
    "foodandwine.com", "epicurious.com",
    # Sports (for sports pairs)
    "espn.com", "goal.com", "transfermarkt.com", "soccerway.com",
    "fifa.com", "uefa.com", "bbc.com", "skysports.com",
    "sportingnews.com", "bleacherreport.com",
    # Reference / encyclopedic
    "britannica.com", "newworldencyclopedia.org",
]

# TLDs to also scan (catches smaller outlets)
NEWS_TLDS = ["ua", "uk"]
