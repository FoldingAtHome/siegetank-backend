Siegetank Python API
====================

.. autosummary::

    siegetank.base.login
    siegetank.base.Stream
    siegetank.base.Target
    siegetank.base.refresh_scvs
    siegetank.base.add_target
    siegetank.base.list_targets

.. automodule:: siegetank.base

    .. automethod:: siegetank.base.login
    .. automethod:: siegetank.base.refresh_scvs
    .. automethod:: siegetank.base.add_target
    .. automethod:: siegetank.base.list_targets

    .. autoclass:: Stream

        .. automethod:: Stream.start
        .. automethod:: Stream.stop
        .. automethod:: Stream.delete
        .. automethod:: Stream.download
        .. automethod:: Stream.upload
        .. automethod:: Stream.reload_info
        .. automethod:: Stream.sync
        .. attribute:: Stream.id

            id of the stream. Returns ``string``.

        .. attribute:: Stream.active

            If a core is currently processing the stream. Returns ``bool``.

        .. attribute:: Stream.frames

            Number of frames completed on the stream. Returns ``int``.

        .. attribute:: Stream.status

            Status of the stream. Returns ``str``.

        .. attribute:: Stream.error_count

            Number of times the stream has errored. Returns ``int``.

    .. autoclass:: Target

        .. automethod:: Target.delete
        .. automethod:: Target.add_stream
        .. automethod:: Target.reload_info
        .. attribute:: Target.id

            id of the target. Returns ``str``.

        .. attribute:: Target.owner

            Owner of the target the target. Returns ``str``.

        .. attribute:: Target.options

            Options defined for the target. Returns ``dict``.

        .. attribute:: Target.creation_date

            When the target was created, defined in seconds since epoch. Returns ``float``.

        .. attribute:: Target.shards

            The shards used by the target. Returns ``list`` of ``str``.

        .. attribute:: Target.streams

            List of streams in the target

        .. attribute:: Target.engines

            Engines used by the target. Returns ``list`` of ``str``.

        .. attribute:: Target.weight

            Weight of the target relative to other targets owned by you. Returns ``int``.
