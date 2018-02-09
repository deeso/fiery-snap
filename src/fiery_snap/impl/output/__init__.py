from .flatfile_sink import SimpleFlatfile
from .mongo_sink import MongoStore
from .twitterscraper_emailer_sink import TwitterScraperEmailUpdates
from .pastebinscraper_emailer_sink import PastebinScraperEmailUpdates

OUTPUT_CLASS_MAPS = {
    SimpleFlatfile.class_map_key(): SimpleFlatfile,
    MongoStore.class_map_key(): MongoStore,
    TwitterScraperEmailUpdates.class_map_key(): TwitterScraperEmailUpdates,
    PastebinScraperEmailUpdates.class_map_key(): PastebinScraperEmailUpdates,
}
