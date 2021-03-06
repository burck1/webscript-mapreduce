local mapreduce = require('burck1/webscript-mapreduce/mapreduce.lua')

local get_words_count = function (line)
    line = string.gsub(line, "[.,;:]", "")
    line = string.lower(line)
    local counts = {}
    for w in line:gmatch("%S+") do
        counts[w] = (counts[w] or 0) + 1
    end
    return counts
end

local key = mapreduce.key(request)
local value = mapreduce.value(request)

local counts = get_words_count(value)
for word,count in pairs(counts) do
    mapreduce.emit(request, word, count)
end
