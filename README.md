<h1> Backend </h1>
![build status](https://circleci.com/gh/proteneer/backend.png?circle-token=7768b2d45d7b388b2da07c80616b6ed068c1eb34)

<h1> Components </h1>

ST - Siege Tank  
CC - Command Center  
WS - Work Server  

<h1> Key goals </h1>

The major goals in the development of Siege Tank are:

1. Striated workservers for load-balancing  
2. Significantly improved ease of use in creating, storing, and accessing jobs  
3. Adopt modern technologies and libraries  
4. Common RESTful Web API with Python bindings  
5. Scalability on both generic servers and AWS  
6. Emphasize convention over configuration  

<h2> Server Dependencies </h2>

Everything aside from apollo can be pip-installed. 

1. python 3.3
2. redis
3. bcrypt
4. pymongo
5. requests

<h2> Core Dependencies </h2>

1. [POCO Complete Edition](http://pocoproject.org/)
2. [OpenMM 6.0](https://github.com/SimTk/openmm)
3. [cmake](http://www.cmake.org/cmake/resources/software.html)

<h1> Tests </h1>

Tests for the server can be ran using:

python -W ignore -m unittest

<h1> API </h1>

Currently, the allowed RESTful requests are GET, PUT, and POST. GETs and PUTs are guaranteed idempotent, that is, sending the same request 2+ times has the same effect as sending a single request. . Detailed request parameters are available in the source code. In general, with the exception of downloading the final trajectory, PG users work with the Command Center methods. 

<h2> CC Methods </h2>

[P] Unauthenticated requests  
[A] Authenticated requests  
[C] Core requests  

- [P] POST x.com/auth - Authenticate the user, returning an authorization token
- [A] POST x.com/targets - add a target
- [P] GET x.com/targets - if Authenticated, retrieves User's targets, if Public, retrieves list of all targets on server
- [P] GET x.com/targets/info/:target_id - get info about a specific target
- [A] PUT x.com/targets/stage/:target_id - change stage from beta->adv->full
- [A] PUT x.com/targets/delete/:target_id - delete target and its streams
- [A] PUT x.com/targets/stop/:target_id - stop the target and its streams
- [A] GET x.com/targets/streams/:target_id - get the streams for the target
- [A] POST x.com/streams - add a stream
- [P] GET x.com/streams/info/:stream_id - get information about specific stream
- [A] PUT x.com/streams/delete/:stream_id - delete a stream
- [A] PUT x.com/streams/stop/:stream_id - stop a stream
- [C] POST x.com/core/assign - assign a stream to a core

<h2> WS Methods </h2>

[R] IP restricted to requests made by CC  
[C] Used by the core

- [A] GET x.com/streams/stream_id     - download a stream
- [R] PUT x.com/streams/delete        - delete a stream
- [R] POST x.com/streams              - add a new stream
- [C] GET x.com/core/start            - start a stream (given an auth token)
- [C] PUT x.com/core/frame            - add a frame to a stream (idempotent)
- [C] PUT x.com/core/stop             - stop a stream
- [C] PUT x.com/core/checkpoint       - send a checkpoint file corresponding to the last frame received
- [C] POST x.com/core/heartbeat       - send a heartbeat
