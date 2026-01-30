import argparse
import json
import random
import time
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from pytrends.request import TrendReq

# 请求头，避免被服务屏蔽
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (MCM_Research_Bot; mailto:your_email@example.com)"
}
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# 1. 完整明星名单整理 (去重处理)
raw_list = [
    "John O'Hurley", "Kelly Monaco", "Evander Holyfield", "Rachel Hunter", "Joey McIntyre", "Trista Sutter", 
    "Tatum O'Neal", "Tia Carrere", "George Hamilton", "Lisa Rinna", "Stacy Keibler", "Jerry Rice", 
    "Giselle Fernandez", "Master P", "Drew Lachey", "Kenny Mayne", "Harry Hamlin", "Vivica A. Fox", 
    "Monique Coleman", "Joey Lawrence", "Mario Lopez", "Emmitt Smith", "Shanna Moakler", "Willa Ford", 
    "Sara Evans", "Jerry Springer", "Tucker Carlson", "John Ratzenberger", "Ian Ziering", "Clyde Drexler", 
    "Laila Ali", "Apolo Anton Ohno", "Shandi Finnessey", "Paulina Porizkova", "Heather Mills", "Billy Ray Cyrus", 
    "Joey Fatone", "Leeza Gibbons", "Cameron Mathison", "Jane Seymour", "Sabrina Bryan", "Jennie Garth", 
    "Floyd Mayweather Jr.", "Josie Maran", "Albert Reed", "Helio Castroneves", "Mel B", "Wayne Newton", 
    "Marie Osmond", "Mark Cuban", "Cristian de la Fuente", "Steve Guttenberg", "Priscilla Presley", 
    "Marlee Matlin", "Shannon Elizabeth", "Marissa Jaret Winokur", "Jason Taylor", "Kristi Yamaguchi", 
    "Monica Seles", "Penn Jillette", "Adam Carolla", "Mario", "Ted McGinley", "Cloris Leachman", 
    "Susan Lucci", "Cody Linley", "Misty May-Treanor", "Maurice Greene", "Warren Sapp", "Jeffrey Ross", 
    "Toni Braxton", "Lance Bass", "Kim Kardashian", "Rocco DiSpirito", "Brooke Burke", "Gilles Marini", 
    "Denise Richards", "David Alan Grier", "Lawrence Taylor", "Ty Murray", "Shawn Johnson", "Steve Wozniak", 
    "Belinda Carlisle", "Chuck Wicks", "Lil' Kim", "Steve-O", "Holly Madison", "Melissa Rycroft", 
    "Ashley Hamiliton", "Debi Mazar", "Melissa Joan Hart", "Mark Dacascos", "Chuck Liddell", "Natalie Coughlin", 
    "Louie Vito", "Michael Irvin", "Joanna Krupa", "Kathy Ireland", "Tom DeLay", "Macy Gray", "Aaron Carter", 
    "Mya", "Donny Osmond", "Kelly Osbourne", "Aiden Turner", "Shannen Doherty", "Niecy Nash", "Buzz Aldrin", 
    "Chad Ochocinco", "Evan Lysacek", "Pamela Anderson", "Nicole Scherzinger", "Erin Andrews", "Kate Gosselin", 
    "Jake Pavelka", "David Hasselhoff", "Florence Henderson", "Kyle Massey", "Jennifer Grey", "Rick Fox", 
    "Kurt Warner", "Margaret Cho", "Michael Bolton", "Brandy", "The Situation", "Audrina Patridge", 
    "Bristol Palin", "Ralph Macchio", "Chelsea Kane", "Kirstie Alley", "Hines Ward", "Sugar Ray Leonard", 
    "Chris Jericho", "Petra Nemcova", "Mike Catherwood", "Romeo", "Wendy Williams", "Kendra Wilkinson", 
    "Elisabetta Canalis", "David Arquette", "Ricki Lake", "J.R. Martinez", "Metta World Peace", "Hope Solo", 
    "Chynna Phillips", "Kristin Cavallari", "Carson Kressley", "Chaz Bono", "Nancy Grace", "Rob Kardashian", 
    "William Levy", "Jack Wagner", "Sherri Sheperd", "Jaleel White", "Roshon Fegan", "Melissa Gilbert", 
    "Martina Navratilova", "Donald Driver", "Gavin DeGraw", "Gladys Knight", "Katherine Jenkins", "Maria Menounos", 
    "Zendaya", "Dorothy Hamill", "Victor Ortiz", "Alexandra Raisman", "Jacoby Jones", "D. L. Hughley", 
    "Andy Dick", "Wynonna Judd", "Kellie Pickler", "Lisa Vanderpump", "Sean Lowe", "Valerie Harper", 
    "Brant Daugherty", "Elizabeth Berkley Lauren", "Leah Remini", "Corbin Bleu", "Amber Riley", "Keyshawn Johnson", 
    "Bill Engvall", "Christina Milian", "Nicole \"Snooki\" Polizzi", "Jack Osbourne", "Bill Nye", 
    "Billy Dee Williams", "Danica McKellar", "James Maslow", "Candace Cameron Bure", "Sean Avery", "Diana Nyad", 
    "Charlie White", "Amy Purdy", "Meryl Davis", "Drew Carey", "Cody Simpson", "NeNe Leakes", "Antonio Sabato Jr.", 
    "Jonathan Bennett", "Lea Thompson", "Janel Parrish", "Alfonso Ribeiro", "Lolo Jones", "Randy Couture", 
    "Tommy Chong", "Betsey Johnson", "Michael Waltrip", "Tavis Smiley", "Bethany Mota", "Sadie Robertson", 
    "Suzanne Somers", "Willow Shields", "Riker Lynch", "Rumer Willis", "Nastia Liukin", "Michael Sam", 
    "Robert Herjavec", "Charlotte McKinney", "Noah Galloway", "Redfoo", "Patti LaBelle", "Chris Soules", 
    "Gary Busey", "Alexa PenaVega", "Carlos PenaVega", "Victor Espinoza", "Alek Skarlatos", "Chaka Khan", 
    "Andy Grammer", "Tamar Braxton", "Nick Carter", "Hayes Grier", "Bindi Irwin", "Kim Zolciak-Biermann", 
    "Paula Deen", "Mischa Barton", "Kim Fields", "Jodie Sweetin", "Doug Flutie", "Von Miller", "Antonio Brown", 
    "Paige VanZant", "Geraldo Rivera", "Nyle DiMarco", "Ginger Zee", "Wanya Morris", "Marla Maples", 
    "Jake T. Austin", "Maureen McCormick", "Marilu Henner", "Ryan Lochte", "Calvin Johnson Jr.", "Laurie Hernandez", 
    "Amber Rose", "Rick Perry", "James Hinchcliffe", "Babyface", "Vanilla Ice", "Jana Kramer", "Terra Jole", 
    "Charlo", "Mr. T", "Heather Morris", "Nancy Kerrigan", "Bonner Bolton", "Simone Biles", "David Ross", 
    "Rashad Jennings", "Chris Kattan", "Erika Jayne", "Normani", "Nick Viall", "Sasha Pieterse", "Frankie Muniz", 
    "Jordan Fisher", "Derek Fisher", "Nikki Bella", "Terrell Owens", "Victoria Arlen", "Barbara Corcoran", 
    "Lindsey Stirling", "Debbie Gibson", "Nick Lachey", "Drew Scott", "Vanessa Lachey", "Jamie Anderson", 
    "Johnny Damon", "Kareem Abdul-Jabbar", "Arike Ogunbowale", "Jennie Finch Daigle", "Chris Mazdzer", 
    "Mirai Nagasu", "Tonya Harding", "Josh Norman", "Adam Rippon", "Juan Pablo Di Pace", "Evanna Lynch", 
    "Nancy McKeon", "John Schneider", "Milo Manheim", "Danelle Umstead", "Mary Lou Retton", "DeMarcus Ware", 
    "Nikki Glaser", "Bobby Bones", "Tinashe", "Alexis Ren", "Joe Amabile", "Kate Flannery", "James Van Der Beek", 
    "Kel Mitchell", "Ray Lewis", "Lamar Odom", "Sailor Brinkley-Cook", "Sean Spicer", "Mary Wilson", 
    "Lauren Alaina", "Ally Brooke", "Karamo Brown", "Hannah Brown", "Anne Heche", "Jesse Metcalfe", 
    "Chrishell Stause", "Skai Jackson", "Justina Machado", "Charles Oakley", "Veron Davis", "Johnny Weir", 
    "Nev Schulman", "AJ McLean", "Nelly", "Kaitlyn Bristowe", "Carole Baskin", "Monica Aldama", "Jeannie Mai", 
    "Martin Kove", "Brian Austin Green", "Melora Hardin", "Matt James", "Mike \"The Miz\" Mizanin", "Suni Lee", 
    "Iman Shumpert", "Cody Rigsby", "Melanie C", "Jimmie Allen", "Olivia Jade", "JoJo Siwa", "Christine Chiu", 
    "Kenya Moore", "Amanda Kloots", "Jason Lewis", "Cheryl Ladd", "Selma Blair", "Joseph Baena", "Trevor Donovan", 
    "Daniel Durant", "Wayne Brady", "Sam Champion", "Jessie James Decker", "Jordin Sparks", "Heidi D'Amelio", 
    "Charli D'Amelio", "Teresa Giudice", "Vinny Guadagnino", "Shangela", "Gabby Windey", "Jamie Lynn Spears", 
    "Mira Sorvino", "Barry Williams", "Alyson Hannigan", "Xochitl Gomez", "Adrian Peterson", "Matt Walsh", 
    "Tyson Beckford", "Jason Mraz", "Lele Pons", "Harry Jowsey", "Mauricio Umansky", "Charity Lawson", 
    "Ariana Madix", "Tori Spelling", "Eric Roberts", "Reginald VelJohnson", "Chandler Kinney", "Dwight Howard", 
    "Danny Amendola", "Stephen Nedoroscik", "Ilona Maher", "Anna Delvey", "Brooks Nader", "Phaedra Parks", 
    "Jenn Tran", "Joey Graziadei", "Corey Feldman", "Danielle Fishel", "Elaine Hendrix", "Baron Davis", 
    "Jordan Chiles", "Andy Richter", "Robert Irwin", "Hilaria Baldwin", "Lauren Jauregui", "Scott Hoying", 
    "Alix Earle", "Jen Affleck", "Whitney Leavitt", "Dylan Efron"
]

# 明星去重
def load_all_stars(data_path: Path):
    if data_path.exists():
        try:
            df = pd.read_csv(data_path)
            names = df.get("celebrity_name", pd.Series(dtype=str)).dropna().astype(str)
            unique_names = sorted(set(name.strip() for name in names if name.strip()))
            if unique_names:
                return unique_names
        except Exception as exc:
            print(f"读取 {data_path} 失败，使用默认名单：{exc}")
    else:
        print(f"未找到 {data_path}，使用默认名单。")
    return sorted(list(set(raw_list)))


REPO_ROOT = Path(__file__).resolve().parent
DATA_PATH = REPO_ROOT / "2026_MCM_Problem_C_Data.csv"
ALL_STARS = load_all_stars(DATA_PATH)

def fetch_all_trends(star_list, output_file="dwts_historical_trends.csv"):
    """
    抓取 2004 至今的所有热度数据，通过 Long-term 趋势定位明星的 Popularity
    """
    pytrends = TrendReq(hl='en-US', tz=360)
    all_data = pd.DataFrame()
    
    # Google Trends 限制一次 5 个关键词
    batch_size = 5
    
    for i in range(0, len(star_list), batch_size):
        batch = star_list[i:i + batch_size]
        print(f"正在处理第 {i//batch_size + 1} 组: {batch}")
        
        # 针对潜在歧义词的处理
        search_terms = []
        for name in batch:
            if name in ["Mario", "The Situation", "Prince", "Romeo"]:
                search_terms.append(f"{name} DWTS")
            else:
                search_terms.append(name)
        
        try:
            # 采用 'all' 时间范围 (2004-present) 以确保覆盖所有赛季
            pytrends.build_payload(search_terms, cat=0, timeframe='all', geo='US', gprop='')
            df = pytrends.interest_over_time()
            
            if not df.empty:
                df = df.drop(columns=['isPartial'])
                # 转为长表格式
                df_reset = df.reset_index()
                melted = df_reset.melt(id_vars='date', var_name='celebrity_name', value_name='search_index')
                all_data = pd.concat([all_data, melted])
                
            # 随机休眠防止封禁 (很重要！)
            wait = random.uniform(10, 20)
            time.sleep(wait)
            
        except Exception as e:
            print(f"抓取 {batch} 时发生错误: {e}")
            time.sleep(60) # 出错则长休
            continue
            
    # 保存结果
    all_data.to_csv(output_file, index=False)
    print(f"✅ 完成！数据已保存至 {output_file}")


def _request_with_retry(url, params=None, headers=None, retries=3, timeout=20):
    headers = headers or DEFAULT_HEADERS
    response = None
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=timeout)
        except requests.RequestException:
            response = None
        if response and response.status_code == 200:
            return response
        if response and response.status_code not in RETRY_STATUS_CODES:
            return response
        time.sleep(2 ** attempt)
    return response


def _parse_abbrev_number(value):
    if value is None:
        return None
    text = str(value).strip().lower().replace(",", "")
    if not text:
        return None
    multiplier = 1
    if text.endswith("k"):
        multiplier = 1_000
        text = text[:-1]
    elif text.endswith("m"):
        multiplier = 1_000_000
        text = text[:-1]
    elif text.endswith("b"):
        multiplier = 1_000_000_000
        text = text[:-1]
    try:
        return int(float(text) * multiplier)
    except ValueError:
        return None


def _extract_handle(url):
    if not url:
        return ""
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    if not path:
        return ""
    return path.split("/")[0].lstrip("@")


def _find_wikipedia_title(name):
    params = {
        "action": "query",
        "list": "search",
        "srsearch": name,
        "format": "json",
    }
    response = _request_with_retry("https://en.wikipedia.org/w/api.php", params=params)
    if not response or response.status_code != 200:
        return ""
    try:
        data = response.json()
    except ValueError:
        return ""
    results = data.get("query", {}).get("search", [])
    if not results:
        return ""
    return results[0].get("title", "")


def _fetch_wikipedia_html(title):
    params = {
        "action": "parse",
        "page": title,
        "prop": "text",
        "format": "json",
    }
    response = _request_with_retry("https://en.wikipedia.org/w/api.php", params=params)
    if not response or response.status_code != 200:
        return ""
    try:
        data = response.json()
    except ValueError:
        return ""
    return data.get("parse", {}).get("text", {}).get("*", "")


def _fetch_wikidata_id(title):
    params = {
        "action": "query",
        "prop": "pageprops",
        "titles": title,
        "format": "json",
    }
    response = _request_with_retry("https://en.wikipedia.org/w/api.php", params=params)
    if not response or response.status_code != 200:
        return ""
    try:
        data = response.json()
    except ValueError:
        return ""
    pages = data.get("query", {}).get("pages", {})
    for page in pages.values():
        return page.get("pageprops", {}).get("wikibase_item", "")
    return ""


def _fetch_wikidata_social_links(wikidata_id):
    if not wikidata_id:
        return {}
    url = "https://www.wikidata.org/wiki/Special:EntityData/{}.json".format(wikidata_id)
    response = _request_with_retry(url)
    if not response or response.status_code != 200:
        return {}
    try:
        data = response.json()
    except ValueError:
        return {}
    entity = data.get("entities", {}).get(wikidata_id, {})
    claims = entity.get("claims", {})

    def _get_claim_value(pid):
        claim_list = claims.get(pid, [])
        if not claim_list:
            return ""
        mainsnak = claim_list[0].get("mainsnak", {})
        return mainsnak.get("datavalue", {}).get("value", "")

    instagram_handle = _get_claim_value("P2003")  # Instagram username
    twitter_handle = _get_claim_value("P2002")  # X/Twitter username
    tiktok_handle = _get_claim_value("P7085")  # TikTok username
    youtube_channel = _get_claim_value("P2397")  # YouTube channel ID
    facebook_id = _get_claim_value("P2013")  # Facebook ID

    links = {}
    if instagram_handle:
        links["instagram"] = f"https://www.instagram.com/{instagram_handle}/"
    if twitter_handle:
        links["twitter"] = f"https://twitter.com/{twitter_handle}"
    if tiktok_handle:
        links["tiktok"] = f"https://www.tiktok.com/@{tiktok_handle}"
    if youtube_channel:
        links["youtube"] = f"https://www.youtube.com/channel/{youtube_channel}"
    if facebook_id:
        links["facebook"] = f"https://www.facebook.com/{facebook_id}"
    return links

def _match_domain(url, domain):
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if not host:
        return False
    return host == domain or host.endswith(f".{domain}")


def _extract_social_links(html):
    soup = BeautifulSoup(html, "html.parser")
    infobox = soup.find("table", class_=lambda x: x and "infobox" in x)
    if not infobox:
        return {}
    links = {}
    for link in infobox.find_all("a", href=True):
        href = link["href"]
        if _match_domain(href, "instagram.com"):
            links.setdefault("instagram", href)
        elif _match_domain(href, "twitter.com") or _match_domain(href, "x.com"):
            links.setdefault("twitter", href)
        elif _match_domain(href, "tiktok.com"):
            links.setdefault("tiktok", href)
        elif _match_domain(href, "youtube.com") or _match_domain(href, "youtu.be"):
            links.setdefault("youtube", href)
        elif _match_domain(href, "facebook.com"):
            links.setdefault("facebook", href)
    return links


def _fetch_instagram_followers(handle):
    if not handle:
        return None
    url = f"https://www.instagram.com/{handle}/?__a=1&__d=dis"
    response = _request_with_retry(url)
    if not response or response.status_code != 200:
        return _fetch_instagram_followers_fallback(handle)
    try:
        data = response.json()
    except ValueError:
        return None
    count = (
        data.get("graphql", {})
        .get("user", {})
        .get("edge_followed_by", {})
        .get("count")
    )
    return _parse_abbrev_number(count)


def _fetch_instagram_followers_fallback(handle):
    if not handle:
        return None
    url = f"https://www.instagram.com/{handle}/"
    response = _request_with_retry(url)
    if not response or response.status_code != 200:
        return None
    match = re.search(r'"edge_followed_by":\{"count":(\d+)\}', response.text)
    if not match:
        return None
    return _parse_abbrev_number(match.group(1))

def _fetch_twitter_followers(handle):
    if not handle:
        return None
    url = "https://cdn.syndication.twimg.com/widgets/followbutton/info.json"
    response = _request_with_retry(url, params={"screen_names": handle})
    if not response or response.status_code != 200:
        return None
    data = response.json()
    if not data:
        return None
    return _parse_abbrev_number(data[0].get("followers_count"))


def _fetch_tiktok_followers(handle):
    if not handle:
        return None
    url = f"https://www.tiktok.com/@{handle}"
    response = _request_with_retry(url)
    if not response or response.status_code != 200:
        return None
    match = re.search(r'"followerCount":(\d+)', response.text)
    if not match:
        return None
    return _parse_abbrev_number(match.group(1))


def _fetch_youtube_followers(url):
    if not url:
        return None
    response = _request_with_retry(url)
    if not response or response.status_code != 200:
        return None
    match = re.search(
        r'"subscriberCountText":\{"simpleText":"([^"]+)"', response.text
    )
    if not match:
        return None
    count_text = match.group(1).split()[0]
    return _parse_abbrev_number(count_text)


def _parse_dance_style(raw_text):
    text = str(raw_text).strip()
    if not text:
        return ""
    split_match = re.split(r"\s[-–—]\s| / ", text, maxsplit=1)
    return split_match[0].strip()


def fetch_social_followers(star_list, output_file="dwts_social_followers.csv"):
    """
    抓取维基百科/维基数据中的社交媒体账号，并提取粉丝量。
    输出字段包含：
    - celebrity_name, wikipedia_title
    - instagram_handle, twitter_handle, tiktok_handle, youtube_url, facebook_url
    - instagram_followers, twitter_followers, tiktok_followers, youtube_subscribers
    注意：社交媒体粉丝量为公开页面抓取结果，接口可能限制或失效，返回空值不代表为零。
    """
    results = []
    for name in star_list:
        print(f"正在抓取 {name} 的社交媒体粉丝量...")
        try:
            title = _find_wikipedia_title(name)
            if not title:
                results.append({"celebrity_name": name})
                continue
            html = _fetch_wikipedia_html(title)
            links = _extract_social_links(html)
            wikidata_id = _fetch_wikidata_id(title)
            wikidata_links = _fetch_wikidata_social_links(wikidata_id)
            for key, value in wikidata_links.items():
                links.setdefault(key, value)
            instagram_handle = _extract_handle(links.get("instagram", ""))
            twitter_handle = _extract_handle(links.get("twitter", ""))
            tiktok_handle = _extract_handle(links.get("tiktok", ""))
            youtube_url = links.get("youtube", "")

            results.append({
                "celebrity_name": name,
                "wikipedia_title": title,
                "instagram_handle": instagram_handle,
                "twitter_handle": twitter_handle,
                "tiktok_handle": tiktok_handle,
                "youtube_url": youtube_url,
                "instagram_followers": _fetch_instagram_followers(instagram_handle),
                "twitter_followers": _fetch_twitter_followers(twitter_handle),
                "tiktok_followers": _fetch_tiktok_followers(tiktok_handle),
                "youtube_subscribers": _fetch_youtube_followers(youtube_url),
                "facebook_url": links.get("facebook", ""),
            })
        except Exception as exc:
            print(f"抓取 {name} 时发生错误: {exc}")
            results.append({"celebrity_name": name})
        time.sleep(random.uniform(1.5, 3.0))

    pd.DataFrame(results).to_csv(output_file, index=False)
    print(f"✅ 社交媒体粉丝量已保存至 {output_file}")


def add_running_order_and_dance_style(
    order_file="dwts_weekly_details.csv",
    output_file="dwts_weekly_details_enriched.csv",
):
    """
    补充舞蹈种类字段清洗，统一输出列名。
    """
    order_path = Path(order_file)
    if not order_path.exists():
        print(f"未找到周次明细文件：{order_file}")
        return
    try:
        df = pd.read_csv(order_path)
    except Exception as exc:
        print(f"读取周次明细失败：{exc}")
        return
    if "Dance_Style" in df.columns:
        df["Dance_Style"] = df["Dance_Style"].apply(_parse_dance_style)
    df.to_csv(output_file, index=False)
    print(f"✅ 周次明细已保存至 {output_file}")


def _format_gdelt_datetime(dt_value):
    if not hasattr(dt_value, "strftime"):
        raise ValueError("start_dt/end_dt must be datetime-like values")
    return dt_value.strftime("%Y%m%d%H%M%S")


def fetch_negative_news_ratio(name, start_dt, end_dt, max_records=250):
    """
    使用 GDELT 2.1 获取指定时间段负面新闻占比，返回 tone < 0 的文章比例。
    """
    try:
        params = {
            "query": f'"{name}"',
            "format": "json",
            "mode": "ArtList",
            "maxrecords": max_records,
            "sort": "DateDesc",
            "startdatetime": _format_gdelt_datetime(start_dt),
            "enddatetime": _format_gdelt_datetime(end_dt),
        }
    except ValueError as exc:
        print(f"GDELT 时间格式错误：{exc}")
        return 0.0
    response = _request_with_retry("https://api.gdeltproject.org/api/v2/doc/doc", params=params)
    if not response or response.status_code != 200:
        return 0.0
    try:
        data = response.json()
    except ValueError:
        return 0.0
    articles = data.get("articles") or []
    if not articles:
        return 0.0
    negative = 0
    total = 0
    for article in articles:
        tone_value = article.get("tone")
        try:
            tone = float(tone_value)
        except (TypeError, ValueError):
            continue
        total += 1
        if tone < 0:
            negative += 1
    if total == 0:
        return 0.0
    return negative / total


def adjust_heat_with_negative_news(
    trends_path="dwts_historical_trends.csv",
    output_file="dwts_heat_adjusted.csv",
    granularity="M",
    cache_path="dwts_heat_cache.json",
    max_pairs=None,
    sleep_range=(1.0, 2.0),
):
    """
    结合 Google Trends 与 GDELT 负面新闻比例拆分热度。
    performance_heat = search_index * (1 - negative_news_ratio)
    black_red_heat = search_index * negative_news_ratio
    """
    df = pd.read_csv(trends_path)
    required_columns = {"date", "search_index", "celebrity_name"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"趋势数据缺少列：{', '.join(sorted(missing))}")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["search_index"] = pd.to_numeric(df["search_index"], errors="coerce")
    df = df.dropna(subset=["search_index"])
    df["period_key"] = df["date"].dt.to_period(granularity).astype(str)

    cache_file = Path(cache_path)
    cache = {}
    if cache_file.exists():
        try:
            cache = json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            cache = {}

    def _cache_key(name, period_key):
        return f"{name}||{period_key}"

    unique_pairs = (
        df[["celebrity_name", "period_key"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    processed = 0
    interrupted = False
    for name, period_key in unique_pairs:
        key = _cache_key(name, period_key)
        if key in cache:
            continue
        if max_pairs is not None and processed >= max_pairs:
            break
        period_start = pd.Period(period_key).start_time
        period_end = pd.Period(period_key).end_time
        try:
            cache[key] = fetch_negative_news_ratio(name, period_start, period_end)
        except KeyboardInterrupt:
            interrupted = True
            break
        processed += 1
        cache_file.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
        time.sleep(random.uniform(*sleep_range))

    cache_file.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
    df["negative_news_ratio"] = pd.to_numeric(
        df.apply(
            lambda row: cache.get(_cache_key(row["celebrity_name"], row["period_key"])),
            axis=1,
        ),
        errors="coerce",
    )
    df["negative_news_ratio"] = df["negative_news_ratio"].fillna(0)
    missing_ratios = df["negative_news_ratio"].isna().sum()
    if missing_ratios:
        print(f"⚠️ 仍有 {missing_ratios} 条记录缺少负面新闻比例，可继续运行补全。")
    df["performance_heat"] = df["search_index"] * (1 - df["negative_news_ratio"])
    df["black_red_heat"] = df["search_index"] * df["negative_news_ratio"]
    df.to_csv(output_file, index=False)
    if interrupted:
        print(f"⚠️ 中断保存缓存与部分结果至 {output_file}")
        return
    print(f"✅ 热度拆分结果已保存至 {output_file}")

# 3. 运行抓取
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DWTS 数据抓取工具")
    parser.add_argument(
        "--mode",
        choices=["trends", "social", "heat", "all", "order"],
        default="trends",
        help="选择抓取模式：trends=Google Trends，social=社交媒体粉丝量，heat=负面新闻热度拆分，order=整理周次明细",
    )
    parser.add_argument(
        "--order-file",
        default="dwts_weekly_details.csv",
        help="周次明细数据路径",
    )
    parser.add_argument(
        "--order-output",
        default="dwts_weekly_details_enriched.csv",
        help="清洗后的周次明细输出路径",
    )
    parser.add_argument(
        "--trends-file",
        default="dwts_historical_trends.csv",
        help="Google Trends 输出路径",
    )
    parser.add_argument(
        "--heat-output",
        default="dwts_heat_adjusted.csv",
        help="热度拆分输出路径",
    )
    parser.add_argument(
        "--heat-cache",
        default="dwts_heat_cache.json",
        help="负面新闻缓存文件路径（可用于断点续跑）",
    )
    parser.add_argument(
        "--heat-limit",
        type=int,
        default=None,
        help="限制负面新闻抓取的最大组合数（调试用）",
    )
    parser.add_argument(
        "--heat-sleep-min",
        type=float,
        default=1.0,
        help="负面新闻抓取间隔最小秒数",
    )
    parser.add_argument(
        "--heat-sleep-max",
        type=float,
        default=2.0,
        help="负面新闻抓取间隔最大秒数",
    )
    args = parser.parse_args()

    if args.mode in {"trends", "all"}:
        fetch_all_trends(ALL_STARS)
    if args.mode in {"social", "all"}:
        fetch_social_followers(ALL_STARS)
    if args.mode in {"heat", "all"}:
        adjust_heat_with_negative_news(
            args.trends_file,
            args.heat_output,
            cache_path=args.heat_cache,
            max_pairs=args.heat_limit,
            sleep_range=(args.heat_sleep_min, args.heat_sleep_max),
        )
    if args.mode in {"order", "all"}:
        add_running_order_and_dance_style(args.order_file, args.order_output)