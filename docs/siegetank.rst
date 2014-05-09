Siegetank Python API
====================

.. autosummary::

    siegetank.base.generate_token
    siegetank.base.login
    siegetank.base.Stream
    siegetank.base.Target
    siegetank.base.generate_token
    siegetank.base.refresh_scvs
    siegetank.base.add_target
    siegetank.base.get_targets

.. automodule:: siegetank.base

    .. automethod:: siegetank.base.login
    .. automethod:: siegetank.base.generate_token
    .. automethod:: siegetank.base.refresh_scvs
    .. automethod:: siegetank.base.add_target
    .. automethod:: siegetank.base.get_targets

    .. autoclass:: Stream

        .. automethod:: Stream.start
        .. automethod:: Stream.stop
        .. automethod:: Stream.delete
        .. automethod:: Stream.download
        .. automethod:: Stream.upload
        .. automethod:: Stream.reload_info
        .. attribute:: Stream.id

            The id of the stream.

        .. attribute:: Stream.active

            A bool denoting if the stream is active or not.

        .. attribute:: Stream.frames

            Number of frames completed on the stream.

        .. attribute:: Stream.status

            Status of the stream.

        .. attribute:: Stream.error_count

            Number of times the stream has errored.

    .. autoclass:: Target

        .. automethod:: Target.delete
        .. automethod:: Target.add_stream
        .. automethod:: Target.reload_streams
        .. automethod:: Target.reload_info
        .. attribute:: Target.id

            The id of the target.

        .. attribute:: Target.options

            A dictionary of options defined for the target.

        .. attribute:: Target.creation_date

            Time for when the target was created, defined in seconds since epoch.

        .. attribute:: Target.shards

            The shards used by the target

        .. attribute:: Target.engines

            Engines used by the target

        .. attribute:: Target.weight

            Weight of the target relative to other targets owned by you.