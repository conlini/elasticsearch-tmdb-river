TMDB River Plugin for ElasticSearch 
==================================

The TMDB river plugin pulls data from [TheMovieDB](https://www.themoviedb.org) to [elasticsearch](http://www.elasticsearch.org).


Getting Started
===============

The TMDB River uses the TMDB discover api to fetch metadata for movies or tv serials.

The tmdb river requires a valid [api key](https://www.themoviedb.org/account) from TMDB in order to work. 


Installation
------------

Here is how you can easily create the river and index data from tmdb:

```sh
curl -XPUT localhost:9200/_river/tmdb/_meta -d '
{
    "type" : "tmdb",
    "api_key" : "<api_key>",
    "discovery_type" : "<tv/movie>",
    "max_pages" : 50 ,
    "content_mapping" : "properties" : {"title" : {"index" : "not_analyzed" , "type" : "string"}}
}'
```

NOTE: The 'api_key' parameter is mandatory. Other parameters have reasonable defaults.

The index is created when not already existing, otherwise the documents are added to the existing one with the configured name. 

All documents are stored under a single type("contents"). 

The documents are indexed using the [bulk api](http://www.elasticsearch.org/guide/reference/java-api/bulk.html) at 20 documents per request.

The river goes thru all pages of the response and indexes all documents. This can be controlled using the "max_pages" parameter

A user defined mapping can be provided for the metadata, using the "content_mapping" field. The structure is mapped as follows
{"content" : {"properties" : {"title" : {"index" : "not_analyzed" , "type" : "string"}}}}



License
=======

```
This software is licensed under the Apache 2 license, quoted below.


Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
```
