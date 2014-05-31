SCV REST API
================================

.. automodule:: server.scv

Manager Methods
---------------

.. autosimple:: StreamInfoHandler.get
.. autosimple:: StreamDownloadHandler.get
.. autosimple:: StreamUploadHandler.get
.. autosimple:: StreamStartHandler.put
.. autosimple:: StreamStopHandler.put
.. autosimple:: StreamDeleteHandler.put
.. autosimple:: StreamsHandler.post
.. autosimple:: TargetStreamsHandler.get

Misc Methods
------------

.. autosimple:: AliveHandler.get
.. autosimple:: ActiveStreamsHandler.get

Core Methods
------------
These methods must be authenticated using a core token

.. autosimple:: CoreStartHandler.get
.. autosimple:: CoreFrameHandler.put
.. autosimple:: CoreCheckpointHandler.put
.. autosimple:: CoreStopHandler.put
.. autosimple:: CoreHeartbeatHandler.post
