local mapreduce = require('burck1/webscript-mapreduce/mapreduce.lua')

local sum = function (l)
    local sum = 0
    for k,v in pairs(l) do
        sum = sum + v
    end
    return sum
end

key = mapreduce.key(request)
value = mapreduce.value(request)

mapreduce.emit(request, key, sum(value))
mapreduce.continue(request)
