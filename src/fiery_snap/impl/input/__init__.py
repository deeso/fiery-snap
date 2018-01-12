
from fiery_snap.impl.input.twitter_source import TwitterSource
from fiery_snap.impl.input.kombu_source import KombuClientProducer
from fiery_snap.impl.input.iocs_source import *

INPUT_CLASS_MAPS = {
    TwitterSource.class_map_key(): TwitterSource,
    KombuClientProducer.class_map_key(): KombuClientProducer,
    DomainSource.class_map_key(): DomainSource,
    HostSource.class_map_key(): HostSource,
    IpSource.class_map_key(): IpSource,
    HashSource.class_map_key(): HashSource,
    HtmlSource.class_map_key(): HtmlSource,
}

