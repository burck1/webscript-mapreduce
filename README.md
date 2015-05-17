# webscript-mapreduce
A http://webscript.io module for running a map reduce job

## Instructions

1. Import the repo as a local variable:
```lua
local mapreduce = require('burck1/webscript-mapreduce/mapreduce.lua')
```

2. Create 4 endpoints within the same subdomain:

    1. `/mapreduce` - this will be used to initialize the mapreduce job
    2. `/map` - this will perform the "map" operation
    3. `/reduce` - this will perform the "reduce" operation
    4. `/result` - this will collect the results

3. Write your webscripts: See the sample.*.lua files for an example.


## API Reference

```lua
mapreduce.setup(map_url, reduce_url, result_url)
```

```lua
mapreduce.map(data)
```

```lua
local key = mapreduce.key(request)
```

```lua
local value = mapreduce.value(request)
```

```lua
mapreduce.emit(request, key2, value2)
```

```lua
mapreduce.continue(request)
```

```lua
local result = mapreduce.result(request)
```
