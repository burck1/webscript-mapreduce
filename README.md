# webscript-mapreduce
A http://webscript.io module for running a map reduce job

To use this module:

1. Import the repo as a local variable:
```lua
local mapreduce = require('burck1/webscript-mapreduce/mapreduce.lua')
```

2. Create 4 endpoints within the same subdomain:

    1. /mapreduce - this will be used to initialize the mapreduce job
    2. /map - this will perform the "map" operation
    3. /reduce - this will perform the "reduce" operation
    4. /result - this will collect the results

3. Write your webscripts: See the sample.*.lua files for an example.
