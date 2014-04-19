SCV REST API
================================

.. automodule:: server.scv

Command Center Methods
----------------------

These methods on the workserver can only be issued by IPs matching that of known command centers.


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