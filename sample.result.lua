local mapreduce = require('burck1/webscript-mapreduce/mapreduce.lua')

local result = mapreduce.value(request)
local result_str = json.stringify(result)

log(result_str)
return result_str, {["Content-Type"]="application/json"}
