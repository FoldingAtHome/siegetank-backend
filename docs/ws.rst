Work Server REST API
================================

.. automodule:: server.ws

Command Center Methods
----------------------

These methods on the workserver can only be issued by IPs matching that of known command centers.

.. autosimple:: TargetStreamsHandler.get
.. autosimple:: PostStreamHandler.post
.. autosimple:: ActivateStreamHandler.post
.. autosimple:: DeleteTargetHandler.put

Core Methods
------------

These methods must be authenticated using a core token

.. autosimple:: CoreStartHandler.get
.. autosimple:: CoreFrameHandler.put
.. autosimple:: CoreCheckpointHandler.put
.. autosimple:: CoreStopHandler.put
.. autosimple:: CoreHeartbeatHandler.post

Manager Methods
---------------

.. autosimple:: StreamInfoHandler.get
.. autosimple:: StreamDownloadHandler.get
.. autosimple:: StreamStartHandler.put
.. autosimple:: StreamStopHandler.put
.. autosimple:: StreamDeleteHandler.put

Misc Methods
------------

.. autosimple:: AliveHandler.get
.. autosimple:: ActiveStreamsHandler.get