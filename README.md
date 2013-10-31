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
6. Allow multiple users to manage a single server with authentication
7. Emphasize Convention over Configuration

<h1> API </h1>

Currently, the allowed RESTful requests are: 

1. read-only GET  
2. add POST  
3. delete DELETE  
4. move PUT

A stream is a conceptualized instance of an MD-run. A project consists of a collection of streams.

<h2> Authentication </h2>

OAuth or Google Auth. Authentication should show allocation available and disk space available.

<h2> POST,DELETE,PUT </h2>
<h3> POST x.com/st/projects </h3>  
Create a new project using a set of OpenMM generated XMLs. The user can choose to select additional options, whose default values are listed below. Each streams within a project has its own system, integrator, and XML files. The user may choose to add new ones as needed. Options are defined on a project basis, that is, all streams share the same properties. The project id is an sha1sum of the union of the system and the integrator xmls.  
__REQ__
``` json
{
  "description" : "kinase project",
  "system" : "system.xml",
  "integrator" : "integrator.xml",
  "states" : ["state0.xml","state1.xml","state2.xml"],
  "options" : {
    "frame-format" : "xtc",
    "precision" : 3,
    "steps-per-frame" : 50000
  }
}
```
__REP__
``` json
{
  "project_id": "sha1sump",
  "stream_ids": ["sha1sum0", "sha1sum1", "sha1sum2"]
}
```
<h3> POST x.com/st/projects/{project-sha1sum} </h3>
Add stream(s) to a pre-existing project
__REQ__
``` json
{
  "system" : "system.xml",
  "integrator" : "integrator.xml",
  "states" : ["state3.xml","state4.xml","state5.xml"]
}
```
__REP__
``` json
{
  "stream_ids": ["sha1sum3", "sha1sum4", "sha1sum5"]
}
```
<h3> DELETE x.com/st/projects/{project-sha1sum}/{stream-sha1sum} </h3>
Delete a stream from a project, replies with HTTP 200 if successful
<h3> DELETE x.com/st/projects/{project-sha1sum} </h3>
Delete a project and its streams, replies with HTTP 200 if successful
<h3> PUT x.com/st/projects/{project-sha1sum}/{stream-sha1sum} </h3>
Move a stream from {project-sha1sum}/{stream-sha1sum} to {destination-project-sha1sum}/{stream-sha1sum}, replies with HTTP 200 if successful
``` json
{
  "destination" : "destination_project_sha1sum",
}
```
<h2> GET </h2>
<h3> GET x.com/st/projects </h3>
List projects sha1 ids available to authenticated user along with their descriptions, __REP__ content:
``` json
{
  "project_ids" : [ 
                     {"sha1sum0": "description0"},
                     {"sha1sum1": "description1"}
                  ]
}
```
<h3> GET x.com/st/projects/{project-sha1sum} </h3>
List streams in the project along with the number of frames, __REP__ content:
``` json
{
  "project_ids" : [ 
                     {"sha1sum0": 5234},
                     {"sha1sum1": 6234}
                  ]
}
```
