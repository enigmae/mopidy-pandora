from __future__ import absolute_import, unicode_literals

import unittest

from mock import mock

from mopidy import core

from mopidy.models import Track

import pykka

from mopidy_pandora import frontend

from mopidy_pandora.frontend import EventHandlingPandoraFrontend, PandoraFrontend, PandoraFrontendFactory

from tests import conftest, dummy_backend
from tests.dummy_backend import DummyBackend, DummyPandoraBackend


class TestPandoraFrontendFactory(unittest.TestCase):

    def test_events_supported_returns_event_handler_frontend(self):
        frontend = PandoraFrontendFactory(conftest.config(), mock.PropertyMock())

        assert type(frontend) is EventHandlingPandoraFrontend

    def test_events_not_supported_returns_regular_frontend(self):
        config = conftest.config()
        config['pandora']['event_support_enabled'] = False
        frontend = PandoraFrontendFactory(config, mock.PropertyMock())

        assert type(frontend) is PandoraFrontend


class BaseTestFrontend(unittest.TestCase):

    def setUp(self):
        config = {
            'core': {
                'max_tracklist_length': 10000,
            }
        }

        self.backend = dummy_backend.create_proxy(DummyPandoraBackend)
        self.non_pandora_backend = dummy_backend.create_proxy(DummyBackend)

        self.core = core.Core.start(
            config, backends=[self.backend, self.non_pandora_backend]).proxy()

        self.tracks = [
            Track(uri='pandora:track:mock_id1:mock_token1', length=40000),          # Regular track
            Track(uri='pandora:ad:mock_id2', length=40000),                         # Advertisement
            Track(uri='mock:track:mock_id3:mock_token3', length=40000),            # Not a pandora track
            Track(uri='pandora:track:mock_id4:mock_token4', length=40000),
            Track(uri='pandora:track:mock_id5:mock_token5', length=None),           # No duration
        ]

        self.uris = [
            'pandora:track:mock_id1:mock_token1', 'pandora:ad:mock_id2',
            'mock:track:mock_id3:mock_token3', 'pandora:track:mock_id4:mock_token4',
            'pandora:track:mock_id5:mock_token5']

        def lookup(uris):
            result = {uri: [] for uri in uris}
            for track in self.tracks:
                if track.uri in result:
                    result[track.uri].append(track)
            return result

        self.core.library.lookup = lookup
        self.tl_tracks = self.core.tracklist.add(uris=self.uris).get()

    def tearDown(self):
        pykka.ActorRegistry.stop_all()


class TestFrontend(BaseTestFrontend):

    def setUp(self):  # noqa: N802
        return super(TestFrontend, self).setUp()

    def tearDown(self):  # noqa: N802
        super(TestFrontend, self).tearDown()

    def test_only_execute_for_pandora_executes_for_pandora_uri(self):
        func_mock = mock.PropertyMock()
        func_mock.__name__ = str('func_mock')
        func_mock.return_value = True

        self.core.playback.play(tlid=self.tl_tracks[0].tlid).get()
        frontend.only_execute_for_pandora_uris(func_mock)(self)

        assert func_mock.called

    def test_only_execute_for_pandora_does_not_execute_for_non_pandora_uri(self):
        func_mock = mock.PropertyMock()
        func_mock.__name__ = str('func_mock')
        func_mock.return_value = True

        self.core.playback.play(tlid=self.tl_tracks[2].tlid).get()
        frontend.only_execute_for_pandora_uris(func_mock)(self)

        assert not func_mock.called

    def test_set_options_performs_auto_setup(self):
        self.core.tracklist.set_repeat(False).get()
        self.core.tracklist.set_consume(True).get()
        self.core.tracklist.set_random(True).get()
        self.core.tracklist.set_single(True).get()
        self.core.playback.play(tlid=self.tl_tracks[0].tlid).get()

        frontend = PandoraFrontend.start(conftest.config(), self.core).proxy()
        frontend.track_playback_started(self.tracks[0]).get()
        assert self.core.tracklist.get_repeat().get() is False
        assert self.core.tracklist.get_consume().get() is True
        assert self.core.tracklist.get_random().get() is False
        assert self.core.tracklist.get_single().get() is False


class TestEventHandlingFrontend(BaseTestFrontend):

    def setUp(self):  # noqa: N802
        super(TestEventHandlingFrontend, self).setUp()

    def tearDown(self):  # noqa: N802
        super(TestEventHandlingFrontend, self).tearDown()

    def test_process_events_ignores_ads(self):
        self.core.playback.play(tlid=self.tl_tracks[1].tlid).get()

        frontend = EventHandlingPandoraFrontend.start(conftest.config(), self.core).proxy()
        frontend._trigger_event_triggered = mock.PropertyMock()
        frontend.event_processed_event.get().clear()
        frontend.track_playback_resumed(self.tl_tracks[1], 100).get()

        assert frontend.event_processed_event.get().isSet()
        assert not frontend._trigger_event_triggered.called

    def test_process_events_handles_exception(self):
        self.core.playback.play(tlid=self.tl_tracks[0].tlid).get()

        frontend = EventHandlingPandoraFrontend.start(conftest.config(), self.core).proxy()
        frontend._trigger_event_triggered = mock.PropertyMock()
        frontend._get_event = mock.PropertyMock(side_effect=ValueError('mock_error'))
        frontend.event_processed_event.get().clear()
        frontend.track_playback_resumed(self.tl_tracks[0], 100).get()

        assert frontend.event_processed_event.get().isSet()
        assert not frontend._trigger_event_triggered.called

    def test_process_events_does_nothing_if_no_events_are_queued(self):
        self.core.playback.play(tlid=self.tl_tracks[0].tlid).get()

        frontend = EventHandlingPandoraFrontend.start(conftest.config(), self.core).proxy()
        frontend._trigger_event_triggered = mock.PropertyMock()
        frontend.event_processed_event.get().set()
        frontend.track_playback_resumed(self.tl_tracks[0], 100).get()

        assert frontend.event_processed_event.get().isSet()
        assert not frontend._trigger_event_triggered.called

    def test_tracklist_changed_blocks_if_events_are_queued(self):
        self.core.playback.play(tlid=self.tl_tracks[0].tlid).get()

        frontend = EventHandlingPandoraFrontend.start(conftest.config(), self.core).proxy()
        frontend.event_processed_event.get().clear()
        frontend.last_played_track_uri = 'mock_uri'
        frontend.upcoming_track_uri = 'mock_ury'

        frontend.tracklist_changed().get()
        assert not frontend.tracklist_changed_event.get().isSet()
        assert frontend.last_played_track_uri.get() == 'mock_uri'
        assert frontend.upcoming_track_uri.get() == 'mock_ury'

    def test_tracklist_changed_updates_uris_after_event_is_processed(self):
        self.core.playback.play(tlid=self.tl_tracks[0].tlid).get()

        frontend = EventHandlingPandoraFrontend.start(conftest.config(), self.core).proxy()
        frontend.event_processed_event.get().set()
        frontend.last_played_track_uri = 'mock_uri'
        frontend.upcoming_track_uri = 'mock_ury'

        frontend.tracklist_changed().get()
        assert frontend.tracklist_changed_event.get().isSet()
        current_track_uri = self.core.playback.get_current_tl_track().get()
        assert frontend.last_played_track_uri.get() == current_track_uri.track.uri
        assert frontend.upcoming_track_uri.get() == self.core.tracklist.next_track(current_track_uri).get().track.uri
