local mapreduce = require('burck1/webscript-mapreduce/mapreduce.lua')

local get_words_count = function (line)
    local counts = {}
    for w in line:gmatch("%S+") do
        counts[string.lower(w)] = (counts[string.lower(w)] or 0) + 1
    end
    return counts
end

key = mapreduce.key(request)
value = mapreduce.value(request)

counts = get_words_count(value)
for word,count in pairs(counts) do
    mapreduce.emit(request, word, count)
end

mapreduce.continue(request)
