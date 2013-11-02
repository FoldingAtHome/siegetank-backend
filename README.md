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
Create a new project using a set of OpenMM generated XMLs. All streams in a project share identical system and integrator xml files, but varying in state. This constraints provides a useful guarantee for apps such as MSM Accelerator, where it can start adaptive sampling simply by a pre-existing project (since all streams of the project are guaranteed to be from the same project!). All streams share the same options. The REP project_id is proposed to the SHA256 checksum of the input json object.  
__REQ__
``` json
{
  "description" : "kinase project",
  "system" : "system.xml",
  "integrator" : "integrator.xml",
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
  "project_id": "sha1sum",
}
```
<h3> POST x.com/st/projects/{project-id} </h3>
Add stream(s) to a pre-existing project by giving it initial states.  
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
                     {"sha1sum0": "description0"},
                     {"sha1sum1": "description1"}
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
