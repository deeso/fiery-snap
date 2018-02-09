from .twitter_scraper_processor import TwitterScraper
from .pastebin_scraper_processor import PastebinScraper

PROCESSOR_CLASS_MAPS = {
    TwitterScraper.class_map_key(): TwitterScraper,
    PastebinScraper.class_map_key(): PastebinScraper,
}
