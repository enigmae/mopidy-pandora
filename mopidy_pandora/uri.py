import logging
import urllib

from pandora.models.pandora import AdItem, PlaylistItem


logger = logging.getLogger(__name__)


class _PandoraUriMeta(type):
    def __init__(cls, name, bases, clsdict):  # noqa N805
        super(_PandoraUriMeta, cls).__init__(name, bases, clsdict)
        if hasattr(cls, 'uri_type'):
            cls.TYPES[cls.uri_type] = cls


class PandoraUri(object):
    __metaclass__ = _PandoraUriMeta
    TYPES = {}
    SCHEME = 'pandora'

    def __init__(self, uri_type=None):
        self.uri_type = uri_type

    def __repr__(self):
        return '{}:{uri_type}'.format(self.SCHEME, **self.__dict__)

    @property
    def encoded_attributes(self):
        encoded_dict = {}
        for k, v in self.__dict__.items():
            encoded_dict[k] = PandoraUri.encode(v)

        return encoded_dict

    @property
    def uri(self):
        return repr(self)

    @classmethod
    def encode(cls, value):
        if value is None:
            value = ''

        if not isinstance(value, basestring):
            value = str(value)
        return urllib.quote(value.encode('utf8'))

    @classmethod
    def decode(cls, value):
        return urllib.unquote(value).decode('utf8')

    @classmethod
    def parse(cls, uri):
        parts = [cls.decode(p) for p in uri.split(':')]
        if not parts or parts[0] != PandoraUri.SCHEME or len(parts) < 2:
            raise NotImplementedError('Not a Pandora URI: {}'.format(uri))
        uri_cls = cls.TYPES.get(parts[1])
        if uri_cls:
            return uri_cls(*parts[2:])
        else:
            raise NotImplementedError("Unsupported Pandora URI type '{}'".format(uri))


class GenreUri(PandoraUri):
    uri_type = 'genre'

    def __init__(self, category_name):
        super(GenreUri, self).__init__(self.uri_type)
        self.category_name = category_name

    def __repr__(self):
        return '{}:{category_name}'.format(
            super(GenreUri, self).__repr__(),
            **self.encoded_attributes
        )


class StationUri(PandoraUri):
    uri_type = 'station'

    def __init__(self, station_id, token):
        super(StationUri, self).__init__(self.uri_type)
        self.station_id = station_id
        self.token = token

    def __repr__(self):
        return '{}:{station_id}:{token}'.format(
            super(StationUri, self).__repr__(),
            **self.encoded_attributes
        )

    @classmethod
    def from_station(cls, station):
        if station.id.startswith('G') and station.id == station.token:
            return GenreStationUri(station.id, station.token)
        return StationUri(station.id, station.token)


class GenreStationUri(StationUri):
    uri_type = 'genre_station'

    @classmethod
    def from_station(cls, station):
        if not (station.id.startswith('G') and station.id == station.token):
            return StationUri(station.id, station.token)
        return GenreStationUri(station.id, station.token)


class TrackUri(PandoraUri):
    uri_type = 'track'

    @classmethod
    def from_track(cls, track):
        if isinstance(track, PlaylistItem):
            return PlaylistItemUri(track.station_id, track.track_token)
        elif isinstance(track, AdItem):
            return AdItemUri(track.station_id)
        else:
            raise NotImplementedError("Unsupported playlist item type '{}'".format(track))


class PlaylistItemUri(TrackUri):

    def __init__(self, station_id, token):
        super(PlaylistItemUri, self).__init__(self.uri_type)
        self.station_id = station_id
        self.token = token

    def __repr__(self):
        return '{}:{station_id}:{token}'.format(
            super(PlaylistItemUri, self).__repr__(),
            **self.encoded_attributes
        )


class AdItemUri(TrackUri):
    uri_type = 'ad'

    def __init__(self, station_id):
        super(AdItemUri, self).__init__(self.uri_type)
        self.station_id = station_id

    def __repr__(self):
        return '{}:{station_id}'.format(
            super(AdItemUri, self).__repr__(),
            **self.encoded_attributes
        )
