from __future__ import absolute_import, unicode_literals

from mopidy import backend, listener


class PandoraFrontendListener(listener.Listener):

    """
    Marker interface for recipients of events sent by the frontend actor.

    """

    @staticmethod
    def send(event, **kwargs):
        listener.send_async(PandoraFrontendListener, event, **kwargs)

    def end_of_tracklist_reached(self, auto_play=False):
        """
        Called whenever the tracklist contains only one track, or the last track in the tracklist is being played.
        :param auto_play: specifies if the next track should be played as soon as it is added to the tracklist.
        :type auto_play: boolean
        """
        pass


class PandoraEventHandlingFrontendListener(listener.Listener):

    """
    Marker interface for recipients of events sent by the event handling frontend actor.

    """

    @staticmethod
    def send(event, **kwargs):
        listener.send_async(PandoraEventHandlingFrontendListener, event, **kwargs)

    def event_triggered(self, track_uri, pandora_event):
        """
        Called when one of the Pandora events have been triggered (e.g. thumbs_up, thumbs_down, sleep, etc.).

        :param track_uri: the URI of the track that the event should be applied to.
        :type track_uri: string
        :param pandora_event: the Pandora event that should be called. Needs to correspond with the name of one of
                              the event handling methods defined in `:class:mopidy_pandora.backend.PandoraBackend`
        :type pandora_event: string
        """
        pass


class PandoraBackendListener(backend.BackendListener):

    """
    Marker interface for recipients of events sent by the backend actor.

    """

    @staticmethod
    def send(event, **kwargs):
        listener.send_async(PandoraBackendListener, event, **kwargs)

    def next_track_available(self, track, auto_play=False):
        """
        Called when the backend has the next Pandora track available to be added to the tracklist.

        :param track: the Pandora track that was fetched
        :type track: :class:`mopidy.models.Ref`
        :param auto_play: specifies if the track should be played as soon as it is added to the tracklist.
        :type auto_play: boolean
        """
        pass

    def event_processed(self):
        """
        Called when the backend has successfully processed the event for the track. This lets the frontend know
        that it can process any tracklist changed events that were queued while the Pandora event was being processed.

        """
        pass


class PandoraPlaybackListener(listener.Listener):

    """
    Marker interface for recipients of events sent by the playback provider.

    """

    @staticmethod
    def send(event, **kwargs):
        listener.send_async(PandoraPlaybackListener, event, **kwargs)

    def track_changed(self, track):
        """
        Called when the track has been changed successfully. Let's the frontend know that it should probably
        expand the tracklist by fetching and adding another track to the tracklist, and removing tracks that have
        already been played.

        :param track: the Pandora track that was just changed to.
        :type track: :class:`mopidy.models.Ref`
        """
        pass

    def track_unplayable(self, track):
        """
        Called when the track is not playable. Let's the frontend know that it should probably remove this track
        from the tracklist and try to replace it with the next track that Pandora provides.

        :param track: the unplayable Pandora track.
        :type track: :class:`mopidy.models.Ref`
        """
        pass

    def skip_limit_exceeded(self):
        """
        Called when the playback provider  has skipped over the maximum number of permissible unplayable tracks using
        :func:`~mopidy_pandora.pandora.PandoraPlaybackProvider.change_track`. This lets the frontend know that the
        player should probably be stopped in order to avoid an infinite loop on the tracklist (which should still be
        in 'repeat' mode.

        """
        pass


class PandoraEventHandlingPlaybackListener(listener.Listener):

    """
    Marker interface for recipients of events sent by the playback provider.

    """

    @staticmethod
    def send(event, **kwargs):
        listener.send_async(PandoraEventHandlingPlaybackListener, event, **kwargs)

    def doubleclicked(self):
        """
        Called when the user performed a doubleclick action (i.e. pause/back, pause/resume, pause, next).

        """
        pass
