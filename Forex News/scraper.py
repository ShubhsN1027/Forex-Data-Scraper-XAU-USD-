import time
import math
import os
import requests
from datetime import datetime
import pandas as pd
import pytz
import pyarrow as pa
import pyarrow.parquet as pq

from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from config import ALLOWED_ELEMENT_TYPES, ICON_COLOR_MAP, ALLOWED_CURRENCY_CODES, ALLOWED_IMPACT_COLORS
from utils import reformat_scraped_data

from nltk.sentiment.vader import SentimentIntensityAnalyzer

NEWSAPI_ENDPOINT = "https://newsapi.org/v2/everything"
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "YOUR_NEWSAPI_KEY_HERE")

# Severity map from impact colors
COLOR_SEVERITY_MAP = {
    "red": ("High", 3),
    "orange": ("Medium", 2),
    "yellow": ("Low", 1),
    "gray": ("Low", 1)
}

EVENT_CATEGORIES = {
    "Non-Farm Employment": "NFP",
    "GDP": "GDP",
    "Interest Rate Decision": "RateDecision",
    "CPI": "Inflation",
    "FOMC": "MonetaryPolicy"
}

# Keywords for geopolitical and commodity factors
GEOPOLITICAL_KEYWORDS = ["war", "sanctions", "unrest", "disaster", "terror", "conflict"]
COMMODITY_KEYWORDS = ["mining output", "mining", "reserves", "IMF", "gold supply"]

def infer_category(headline):
    for k, v in EVENT_CATEGORIES.items():
        if k.lower() in headline.lower():
            return v
    return "Other"

def contains_keyword(text, keywords):
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)

def fetch_news_coverage(keyword, event_time_ms):
    event_time = datetime.utcfromtimestamp(event_time_ms / 1000.0)
    from_time = (event_time - pd.Timedelta("1 day")).isoformat() + "Z"
    to_time = (event_time + pd.Timedelta("1 day")).isoformat() + "Z"

    params = {
        "q": keyword,
        "from": from_time,
        "to": to_time,
        "sortBy": "relevancy",
        "apiKey": NEWSAPI_KEY,
        "language": "en"
    }

    try:
        r = requests.get(NEWSAPI_ENDPOINT, params=params)
        r.raise_for_status()
        data = r.json()
        coverage_count = data.get("totalResults", 0)
    except:
        coverage_count = 0
    return coverage_count

def compute_coverage_factor(coverage_count):
    if coverage_count == 0:
        return 1.0
    elif 1 <= coverage_count <= 5:
        return 1.1
    else:
        return 1.2

def convert_to_utc_ms(date_str, time_str):
    # Example: date="Wed Oct 2", time="8:30am"
    # We'll assume year=2024 (as per user’s instructions)
    parts = date_str.split()
    # parts like ['Wed', 'Oct', '2']
    if len(parts) >= 3:
        month_str = parts[1]
        day_str = parts[2]
    else:
        return None
    full_str = f"{month_str} {day_str}, 2024 {time_str}"
    dt = datetime.strptime(full_str, "%b %d, %Y %I:%M%p")
    dt_utc = pytz.UTC.localize(dt)
    return int(dt_utc.timestamp() * 1000)

def enrich_data(df):
    # Filter by allowed currencies
    df = df[df['currency'].isin(ALLOWED_CURRENCY_CODES)]
    df = df[df['impact'].isin(ALLOWED_IMPACT_COLORS)]

    # Convert time to ms
    df['time(ms)'] = df.apply(lambda row: convert_to_utc_ms(row['date'], row['time']), axis=1)
    df = df.dropna(subset=['time(ms)'])

    # Map severity
    df['severity_category'] = df['impact'].apply(lambda c: COLOR_SEVERITY_MAP[c][0])
    df['severity_level'] = df['impact'].apply(lambda c: COLOR_SEVERITY_MAP[c][1])

    # Category from headline
    df['category'] = df['event'].apply(infer_category)

    # Sentiment score
    sia = SentimentIntensityAnalyzer()
    df['sentiment_score'] = df['event'].apply(lambda text: sia.polarity_scores(text)['compound'])

    # Historical volatility placeholder
    df['historical_volatility'] = 0.0

    # Future event
    now_ms = int(time.time() * 1000)
    df['future_event'] = df['time(ms)'] > now_ms

    # Domain-specific multipliers for severity:
    # Base severity from official severity_level.
    # We'll form a final_severity as:
    # final_severity = official_severity * (1 + log(1+vol)) * coverage_factor * sentiment_factor * domain_factors
    # anticipated_severity = same formula but for future, no coverage known, set coverage_factor=1.0

    # Domain factors:
    # - MonetaryPolicy/RateDecision => +20% => factor 1.2
    # - Inflation => +15% => factor 1.15
    # - NFP => +20% => factor 1.2
    # - Geopolitical => if keyword in headline => +25% => factor 1.25
    # - Commodity => if keyword in headline => +10% => factor 1.1
    # - If USD involved => +10% => factor 1.1 (since DXY related)
    # sentiment_factor: if sentiment is negative (compound < -0.05), gold often rises as safe-haven => increase factor. 
    # Let's do sentiment_factor = 1 + (|sentiment| * 0.1) if sentiment < 0 else 1 for positive. 
    # Actually gold as safe-haven on negative sentiment: if sentiment < 0 => factor 1+(abs(neg)*0.1), else factor=1.
    # (This is arbitrary, but shows complexity.)
    
    def domain_factor(row):
        factor = 1.0
        if row['category'] in ["RateDecision", "MonetaryPolicy"]:
            factor *= 1.2
        if row['category'] == "Inflation":
            factor *= 1.15
        if row['category'] == "NFP":
            factor *= 1.2
        if contains_keyword(row['headline'], GEOPOLITICAL_KEYWORDS):
            factor *= 1.25
        if contains_keyword(row['headline'], COMMODITY_KEYWORDS):
            factor *= 1.1
        # If USD is in currency, reflect DXY indirectly
        if row['headline'] and 'USD' in df['currency'].unique():
            factor *= 1.1
        
        return factor

    df['domain_factor'] = df.apply(domain_factor, axis=1)

    def sentiment_factor(s):
        if s < -0.05:
            return 1 + (abs(s)*0.1)
        else:
            return 1.0

    df['sentiment_factor'] = df['sentiment_score'].apply(sentiment_factor)

    # For past events, compute coverage:
    # final_severity = severity_level * (1+log(1+vol)) * coverage_factor * sentiment_factor * domain_factor
    # For future events, coverage_factor=1.0 (unknown), 
    # anticipated_severity = same formula but coverage_factor=1.0

    final_severities = []
    anticipated_severities = []
    for i, row in df.iterrows():
        official_sev = row['severity_level']
        vol_factor = 1 + math.log1p(row['historical_volatility'])
        sentiment_f = row['sentiment_factor']
        domain_f = row['domain_factor']

        if row['future_event']:
            # Future event: coverage unknown = 1.0
            # anticipated_sev
            anticipated_sev = official_sev * vol_factor * 1.0 * sentiment_f * domain_f
            final_sev = None
        else:
            # Past event: get coverage
            coverage_count = fetch_news_coverage(row['category'], row['time(ms)'])
            coverage_factor = compute_coverage_factor(coverage_count)
            final_sev = official_sev * vol_factor * coverage_factor * sentiment_f * domain_f
            anticipated_sev = official_sev * vol_factor * 1.0 * sentiment_f * domain_f
        
        final_severities.append(final_sev)
        anticipated_severities.append(anticipated_sev)

    df['final_severity'] = final_severities
    df['anticipated_severity'] = anticipated_severities

    # Select final columns
    final_cols = [
        "time(ms)",
        "severity_level",
        "severity_category",
        "event",
        "category",
        "sentiment_score",
        "historical_volatility",
        "future_event",
        "final_severity",
        "anticipated_severity"
    ]
    df = df[final_cols].rename(columns={"event": "headline"})
    return df

def store_as_parquet(df, year=2024, month=10):
    os.makedirs("news_parquet", exist_ok=True)
    path = f"news_parquet/year={year}/month={month}/forex_news.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)
    print(f"Data stored at: {path}")

if __name__ == "__main__":
    # Run scraper
    try:
        driver = webdriver.Chrome()
    except:
        driver = webdriver.Chrome(ChromeDriverManager().install())

    driver.get("https://www.forexfactory.com/calendar?month=Oct.2024")
    month = "October"  # Hardcoded month/year

    table = driver.find_element(By.CLASS_NAME, "calendar__table")

    data = []
    while True:
        before_scroll = driver.execute_script("return window.pageYOffset;")
        driver.execute_script("window.scrollTo(0, window.pageYOffset + 500);")
        time.sleep(2)
        after_scroll = driver.execute_script("return window.pageYOffset;")
        if before_scroll == after_scroll:
            break

    for row in table.find_elements(By.TAG_NAME, "tr"):
        row_data = []
        for element in row.find_elements(By.TAG_NAME, "td"):
            class_name = element.get_attribute('class')
            if class_name in ALLOWED_ELEMENT_TYPES:
                if element.text:
                    row_data.append(element.text)
                elif "calendar__impact" in class_name:
                    impact_elements = element.find_elements(By.TAG_NAME, "span")
                    color = None
                    for impact in impact_elements:
                        impact_class = impact.get_attribute("class")
                        color = ICON_COLOR_MAP.get(impact_class, None)
                    if color:
                        row_data.append(color)
                    else:
                        row_data.append("impact")
        if len(row_data):
            data.append(row_data)

    driver.quit()

    df_raw = reformat_scraped_data(data, month)
    csv_path = f"news/{month}_news.csv"
    df = pd.read_csv(csv_path)
    df_enriched = enrich_data(df)
    store_as_parquet(df_enriched, 2024, 10)
    print("Final Data:")
    print(df_enriched.head())
    print("Done.")