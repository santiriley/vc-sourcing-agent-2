# ============================================================================
# IMPORTS AND SETUP
# ============================================================================

import os
import re
from datetime import datetime, timedelta
from dateutil import tz
from typing import Dict, List, Set, Optional, Any
from urllib.parse import quote_plus

import pandas as pd
import feedparser
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration settings for the VC Sourcing Agent"""

    # Google Sheets Configuration
    SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
    SHEET_NAME = "Leads"
    SCOPE = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    # Timezone
    TIMEZONE = os.environ.get("TZ", "America/Costa_Rica")

    # Data Collection Settings
    TIME_WINDOW_DAYS = 14
    MAX_ITEMS_PER_FEED = 60

    # Geographic Coverage
    COUNTRIES = [
        "Costa Rica", "Guatemala", "El Salvador", "Honduras", "Nicaragua",
        "Panama", "Belize", "Colombia", "Venezuela", "Ecuador", "Peru",
        "Bolivia", "Chile", "Argentina", "Uruguay", "Paraguay", "Brazil"
    ]

    COUNTRY_ALIASES = {
        "Costa Rica": ["Costa Rica", "CR"],
        "El Salvador": ["El Salvador", "SV"],
        "Honduras": ["Honduras", "HN"],
        "Nicaragua": ["Nicaragua", "NI"],
        "Guatemala": ["Guatemala", "GT"],
        "Panama": ["Panamá", "Panama", "PA"],
        "Belize": ["Belize"],
        "Colombia": ["Colombia", "CO"],
        "Venezuela": ["Venezuela", "VE"],
        "Ecuador": ["Ecuador", "EC"],
        "Peru": ["Perú", "Peru", "PE"],
        "Bolivia": ["Bolivia", "BO"],
        "Chile": ["Chile", "CL"],
        "Argentina": ["Argentina", "AR"],
        "Uruguay": ["Uruguay", "UY", "ROU"],
        "Paraguay": ["Paraguay", "PY"],
        "Brazil": ["Brasil", "Brazil", "BR"],
    }

    # Signal Detection Terms
    SECTOR_BLACKLIST = [
        "fintech", "payments", "lending", "buy now pay later",
        "wallet", "neobank", "crypto exchange", "remittance"
    ]

    POST_REVENUE_TERMS = [
        "post-revenue", "revenue", "facturación", "ingresos", "ARR", "MRR",
        "paying customers", "clientes de pago", "contratos", "invoices",
        "compras recurrentes"
    ]

    ENTERPRISE_SIGNALS = [
        "enterprise", "B2B", "contract", "pilot", "paid pilot",
        "cliente corporativo", "empresa"
    ]

    FEMALE_NAMES = [
        "ana", "maría", "maria", "camila", "daniela", "gabriela", "valentina",
        "isabella", "sofia", "sofía", "fernanda", "luisa", "laura", "andrea",
        "carla", "carolina", "paula", "juliana", "claudia", "patricia",
        "mariana", "bianca", "bruna", "aline", "renata", "talita", "carol",
        "alejandra", "ximena", "pauline", "ines", "inés", "beatriz", "raquel",
        "cecilia", "catalina", "silvia", "verónica", "veronica"
    ]

    # Feed URLs
    LATAM_FEEDS = [
        "https://contxto.com/feed/",
        "https://latamlist.com/feed/"
    ]


# ============================================================================
# GOOGLE SHEETS INTEGRATION
# ============================================================================

class GoogleSheetsManager:
    """Handles Google Sheets authentication and operations"""

    def __init__(self, service_account_path: str):
        """Initialize with service account credentials"""
        self.service_account_path = service_account_path
        self.client = None

    def authenticate(self) -> gspread.Client:
        """Authenticate with Google Sheets API"""
        if not self.client:
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                self.service_account_path,
                Config.SCOPE
            )
            self.client = gspread.authorize(creds)
        return self.client

    def open_sheet(self, sheet_id: str) -> gspread.Spreadsheet:
        """Open a Google Sheet by ID"""
        client = self.authenticate()
        return client.open_by_key(sheet_id)

    def get_or_create_worksheet(
        self,
        sheet: gspread.Spreadsheet,
        worksheet_name: str
    ) -> gspread.Worksheet:
        """Get existing worksheet or create new one"""
        try:
            return sheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            return sheet.add_worksheet(
                title=worksheet_name,
                rows=1000,
                cols=20
            )

    def read_existing_urls(self, worksheet: gspread.Worksheet) -> Set[str]:
        """Read existing URLs from worksheet to avoid duplicates"""
        try:
            records = worksheet.get_all_records()
            return {r.get("URL", "") for r in records if r.get("URL")}
        except Exception:
            return set()

    def append_dataframe(
        self,
        worksheet: gspread.Worksheet,
        df: pd.DataFrame
    ) -> None:
        """Append DataFrame to worksheet"""
        all_values = worksheet.get_all_values()

        if not all_values:
            # Empty sheet - write with headers
            set_with_dataframe(worksheet, df)
        else:
            # Append without headers
            set_with_dataframe(
                worksheet,
                df,
                row=len(all_values) + 1,
                include_column_header=False
            )


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

class DateTimeUtils:
    """Date and time utility functions"""

    @staticmethod
    def get_local_tz():
        """Get local timezone object"""
        return tz.gettz(Config.TIMEZONE)

    @staticmethod
    def now() -> datetime:
        """Get current time in local timezone"""
        return datetime.now(tz.tzutc()).astimezone(DateTimeUtils.get_local_tz())

    @staticmethod
    def is_within_window(dt: datetime) -> bool:
        """Check if datetime is within configured time window"""
        age = DateTimeUtils.now() - dt
        return age <= timedelta(days=Config.TIME_WINDOW_DAYS)

    @staticmethod
    def parse_feed_date(entry) -> datetime:
        """Parse date from feed entry"""
        for attr in ("published_parsed", "updated_parsed"):
            if hasattr(entry, attr) and getattr(entry, attr):
                parsed = getattr(entry, attr)
                dt = datetime(*parsed[:6], tzinfo=tz.tzutc())
                return dt.astimezone(DateTimeUtils.get_local_tz())
        return DateTimeUtils.now()


# ============================================================================
# TEXT ANALYSIS
# ============================================================================

class TextAnalyzer:
    """Analyze text for signals and entities"""

    # Regex for extracting names
    NAME_PATTERN = re.compile(
        r"\b([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)\s([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)\b"
    )

    @staticmethod
    def find_country(text: str) -> str:
        """Find country mentioned in text"""
        text_lower = text.lower()

        for country, aliases in Config.COUNTRY_ALIASES.items():
            for alias in aliases:
                if alias.lower() in text_lower:
                    return country
        return ""

    @staticmethod
    def contains_terms(text: str, terms: List[str]) -> bool:
        """Check if text contains any of the specified terms"""
        text_lower = text.lower()
        return any(term.lower() in text_lower for term in terms)

    @staticmethod
    def detect_female_founder(text: str) -> bool:
        """Detect if text mentions a female founder"""
        text_lower = text.lower()

        founder_indicators = [
            "founded by", "co-founded by", "cofundada por", "fundada por",
            "fundado por", "cofounder", "co-founder", "fundadora",
            "fundador", "CEO", "CTO"
        ]

        if not any(indicator in text_lower for indicator in founder_indicators):
            return False

        # Extract names and check against female names list
        names = TextAnalyzer.NAME_PATTERN.findall(text)
        for first_name, _ in names:
            if first_name.lower() in Config.FEMALE_NAMES:
                return True

        return False

    @staticmethod
    def extract_company_name(text: str) -> str:
        """Attempt to extract company name from text"""
        names = TextAnalyzer.NAME_PATTERN.findall(text)
        if not names:
            return ""

        # Find most frequent name pair
        name_pairs = [" ".join(n) for n in names]
        if name_pairs:
            return max(set(name_pairs), key=name_pairs.count)
        return ""

    @staticmethod
    def calculate_score(country: str, text: str) -> int:
        """Calculate lead score based on signals"""
        score = 0

        # Geographic relevance
        if country:
            score += 3

        # Revenue signals
        if TextAnalyzer.contains_terms(text, Config.POST_REVENUE_TERMS):
            score += 3

        # Female founder bonus
        if TextAnalyzer.detect_female_founder(text):
            score += 2

        # Enterprise signals
        if TextAnalyzer.contains_terms(text, Config.ENTERPRISE_SIGNALS):
            score += 1

        # Sector penalty
        if TextAnalyzer.contains_terms(text, Config.SECTOR_BLACKLIST):
            score -= 2

        return max(0, min(10, score))

# ============================================================================
# FEED PROCESSING
# ============================================================================

class FeedProcessor:
    """Process RSS feeds and extract startup information"""

    @staticmethod
    def build_google_news_urls(country: str) -> List[str]:
        """Build Google News RSS URLs for a country"""
        query = (
            '(startup OR raised OR funding OR seed OR "Series A" '
            'OR clients OR customers OR revenue OR facturación OR ingresos)'
        )
        full_query = f'{query} {country}'
        encoded_query = quote_plus(full_query)

        return [
            f'https://news.google.com/rss/search?q={encoded_query}&hl=es-419&gl=LA&ceid=LA:es-419',
            f'https://news.google.com/rss/search?q={encoded_query}&hl=en&gl=US&ceid=US:en'
        ]

    @staticmethod
    def fetch_feed_items() -> List[Dict[str, Any]]:
        """Fetch all feed items from configured sources"""
        items = []

        # Process Google News feeds for each country
        for country in Config.COUNTRIES:
            urls = FeedProcessor.build_google_news_urls(country)

            for url in urls:
                try:
                    feed = feedparser.parse(url)
                    items.extend(
                        FeedProcessor._process_feed_entries(
                            feed.entries[:Config.MAX_ITEMS_PER_FEED],
                            "GoogleNews",
                            country
                        )
                    )
                except Exception as e:
                    print(f"Error processing feed {url}: {e}")

        # Process LatAm-specific feeds
        for url in Config.LATAM_FEEDS:
            try:
                feed = feedparser.parse(url)
                items.extend(
                    FeedProcessor._process_feed_entries(
                        feed.entries[:Config.MAX_ITEMS_PER_FEED],
                        url,
                        None
                    )
                )
            except Exception as e:
                print(f"Error processing feed {url}: {e}")

        return items

    @staticmethod
    def _process_feed_entries(
        entries: List,
        source: str,
        default_country: Optional[str]
    ) -> List[Dict]:
        """Process individual feed entries"""
        items = []

        for entry in entries:
            dt = DateTimeUtils.parse_feed_date(entry)

            if not DateTimeUtils.is_within_window(dt):
                continue

            title = entry.get("title", "")
            summary = entry.get("summary", "")
            link = entry.get("link", "")

            # Determine country
            full_text = f"{title}\n{summary}"
            country = TextAnalyzer.find_country(full_text) or default_country or ""

            items.append({
                "title": title,
                "summary": summary,
                "url": link,
                "published": dt,
                "source": source,
                "country_guess": country
            })

        return items


# ============================================================================
# DATA TRANSFORMATION
# ============================================================================

class DataTransformer:
    """Transform raw feed items into structured lead data"""

    @staticmethod
    def clean_html(text: str) -> str:
        """Remove HTML tags from text"""
        return re.sub(r"<[^<]+?>", "", text)

    @staticmethod
    def truncate_text(text: str, max_length: int = 220) -> str:
        """Truncate text with ellipsis if needed"""
        if len(text) > max_length:
            return text[:max_length - 3] + "…"
        return text

    @staticmethod
    def transform_items(items: List[Dict]) -> pd.DataFrame:
        """Transform feed items into DataFrame with scoring"""
        rows = []

        for item in items:
            title = item["title"]
            summary = DataTransformer.clean_html(item.get("summary", ""))
            url = item["url"]
            country = item["country_guess"]

            full_text = f"{title}. {summary}"

            # Detect signals
            signals = []
            if TextAnalyzer.contains_terms(full_text, Config.POST_REVENUE_TERMS):
                signals.append("post-revenue")
            if TextAnalyzer.detect_female_founder(full_text):
                signals.append("female-founder")
            if TextAnalyzer.contains_terms(full_text, Config.ENTERPRISE_SIGNALS):
                signals.append("enterprise")
            if TextAnalyzer.contains_terms(full_text, Config.SECTOR_BLACKLIST):
                signals.append("fintech-ish")

            # Extract company name
            company = (
                TextAnalyzer.extract_company_name(title) or
                TextAnalyzer.extract_company_name(summary) or
                ""
            )

            rows.append({
                "DateFound": DateTimeUtils.now().strftime("%Y-%m-%d"),
                "Company": company,
                "URL": url,
                "Country": country,
                "Title": title,
                "Snippet": DataTransformer.truncate_text(summary),
                "Signals": ", ".join(signals),
                "Score": TextAnalyzer.calculate_score(country, full_text),
                "Source": item["source"],
                "Published": item["published"].strftime("%Y-%m-%d %H:%M")
            })

        df = pd.DataFrame(rows)

        if not df.empty:
            df = df.sort_values(
                ["Score", "Published"],
                ascending=[False, False]
            ).reset_index(drop=True)

        return df

# ============================================================================
# MAIN PIPELINE
# ============================================================================

class VCSourcingPipeline:
    """Main pipeline orchestrator"""

    def __init__(self, service_account_path: str = "service_account.json"):
        """Initialize pipeline with service account"""
        self.sheets_manager = GoogleSheetsManager(service_account_path)

    def run(self) -> Optional[pd.DataFrame]:
        """Run the complete sourcing pipeline"""
        print("⏳ Starting VC Sourcing Pipeline...")

        # Step 1: Collect feed items
        print("📡 Collecting feed items...")
        items = FeedProcessor.fetch_feed_items()
        print(f"✓ Collected {len(items)} raw items")

        if not items:
            print("⚠️ No items found within time window")
            return None

        # Step 2: Transform data
        print("🔄 Transforming data...")
        df = DataTransformer.transform_items(items)

        if df.empty:
            print("⚠️ No candidate leads after transformation")
            return None

        print(f"✓ Found {len(df)} candidate leads")

        # Step 3: Update Google Sheets
        print("📊 Updating Google Sheets...")
        try:
            sheet = self.sheets_manager.open_sheet(Config.SHEET_ID)
            worksheet = self.sheets_manager.get_or_create_worksheet(
                sheet,
                Config.SHEET_NAME
            )

            # Check for duplicates
            existing_urls = self.sheets_manager.read_existing_urls(worksheet)
            new_df = df[~df["URL"].isin(existing_urls)].copy()

            if new_df.empty:
                print("ℹ️ No new leads to add (all URLs already exist)")
                return df

            # Append new leads
            self.sheets_manager.append_dataframe(worksheet, new_df)

            print(f"✅ Added {len(new_df)} new leads to '{Config.SHEET_NAME}'")
            print(f"🔗 Sheet: https://docs.google.com/spreadsheets/d/{Config.SHEET_ID}/edit")

        except Exception as e:
            print(f"❌ Error updating Google Sheets: {e}")
            return df

        return df


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    pipeline = VCSourcingPipeline("service_account.json")
    pipeline.run()
