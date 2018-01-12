from .twitter_scraper_processor import TwitterScraper
from .mongo_filter_processor import MongoFilterProcessor

PROCESSOR_CLASS_MAPS = {
    TwitterScraper.class_map_key(): TwitterScraper,
    MongoFilterProcessor.class_map_key(): MongoFilterProcessor
}
