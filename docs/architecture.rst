Siegetank Architecture
----------------------

There are four major components to the stack:

1. Command Centers (CCs)
2. SCVs
3. MongoDB
4. ELB

CCs are a more advanced type of an assignment server. They provide REST APIs for many services such as adding managers, adding donors, adding targets, managing core assignments, etc. Typically in production environments, CCs are deployed behind AWS EC2 instances with requests routed via ELB. 

SCVs are the main workhorses. They typically demand large amounts of ram, disk space, and espcially bandwidth. Targets typically striate their streams over a particular range of SCVs based on the needs of the groups and the load of the SCVs.

MongoDB is the persistent storage layer that stores information about the target as a whole. Typical production environments deploy a replica set to provide high availability and redundancy. Both the CCs and SCVs talk to MongoDB.

ELB is an elastic load balancer on AWS that allows for a single point of contact for all requests for CCs. In addition to load balancing, it also allows us to provide a single point of contact.

Core Assignments
----------------

A core begins by making an assignment request to a CC. The CC uses an assignment algorithm detailed in the source code to pick the appropriate target. The CC then looks over the list of sharded SCVs for this target, and activates a particular stream on the chosen SCV. The CC then returns back to the core a token and a URI to start the stream (where by stream files are subsequently fetched). The core then communicates with the SCV as it works on the stream by submitting frames, checkpoints, and heartbeats. When the core disengages from the stream, the SCV deactivates the stream, and statistics are written to Mongo.