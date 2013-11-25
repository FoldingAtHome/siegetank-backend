<h1> Siege Tank </h1>

F@h re-exposed using a web-based RESTful API with Python bindings.

Standard Draft v0.1 

<h1> Key goals </h1>

The major goals in the development of Siege Tank are:

1. Striated workservers for load-balancing
2. Significantly improved ease of use in creating, storing, and accessing jobs
3. Adopt modern technologies
4. Common RESTful Web API with Python bindings
5. Scalability on both generic servers and AWS
6. Emphasize convention over configuration

<h1> Dependencies </h1>

1. Redis - super fast in-memory database.
2. Protocol Buffers - for communicating frames back to WS.
3. Tornado - WS base
4. POCO - C++ HTTPSession library

<h1> API </h1>

Currently, the allowed RESTful requests are: 

1. read-only GET  
2. add POST  
3. delete DELETE  
4. move PUT

A stream is a conceptualized instance of an MD-run. A project consists of a collection of streams.

<h2> Authentication </h2>

PUT x.com/st/auth  
__REQ__
``` json
{
  "username": "username",
  "password": "password"
}
```  
__REP__
``` json
{
  "token": "random_token"
}
```
The token can be used by other APIs as well.

<h2> POST,DELETE,PUT </h2>
<h3> POST x.com/st/projects </h3>  
Create a new, empty, project. All streams within a project have the same system and integrator. frame\_format, precision, steps\_per\_frame are optional, with default values listed below.  
__REQ__
``` json
{
  "description" : "kinase project",
  "system" : "system.xml",
  "integrator" : "integrator.xml",
  "frame_format" : "xtc",
  "precision" : 3,
  "steps_per_frame" : 50000
}
```
__REP__
``` json
{
  "project_id": "sha1sum",
}
```
<h3> POST x.com/st/projects/{project-id} </h3>
Add stream(s) to a pre-existing project by giving it initial states. The states must be consistent with project's system and integrator.  
__REQ__
``` json
{
  "states" : ["state0.xml","state3.xml","state2349.xml"]
}
```
__REP__
``` json
{
  "stream_ids": ["sha1sum3", "sha1sum4", "sha1sum5"]
}
```
<h3> DELETE x.com/st/projects/{project-id}/{stream-id} </h3>
Delete a stream from a project, replies with HTTP 200 if successful
<h3> DELETE x.com/st/projects/{project-id} </h3>
Delete a project and its streams, replies with HTTP 200 if successful
<h3> PUT x.com/st/projects/{source-project-id}/{stream-id} </h3>
Move a stream from {source-project-id}/{stream-id} to {destination-project-id}/{stream-id}, replies with HTTP 200 if successful
``` json
{
  "destination" : "destination_project_sha1sum",
}
```
<h2> GET </h2>
<h3> GET x.com/st/projects </h3>
List projects ids available to authenticated user along with their descriptions  
__REP__  
``` json
{
  "project_ids" : [ 
                     "project_id_0" : 
                     { 
                       "description" : "kinase project",
                       "creation date" : "date",
                       "streams" : 59834
                     }
                     "project_id_1" : 
                     { 
                       "description" : "gpcrs",
                       "creation date" : "date",
                       "streams" : 29341
                     }
                  ]
}
```
<h3> GET x.com/st/projects/{project-id} </h3>
List streams in the project along with the number of frames  
__REP__ 
``` json
{
  "project_ids" : [ 
                     {"sha1sum0": 5234},
                     {"sha1sum1": 6234}
                  ]
}
```
