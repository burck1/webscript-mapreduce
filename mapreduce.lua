function tablelength(T)
    local count = 0
    for _ in pairs(T) do count = count + 1 end
    return count
end

local mapreduce = {}

mapreduce.setup = function (map_url, reduce_url, result_url)
    storage["mapreduce:config:urls:map"] = map_url
    storage["mapreduce:config:urls:reduce"] = reduce_url
    storage["mapreduce:config:urls:result"] = result_url
    storage["mapreduce:data:maptotal"] = 0
    storage["mapreduce:data:reducetotal"] = 0
    storage["mapreduce:data:groups"] = json.stringify({})
    storage["mapreduce:data:results"] = json.stringify({})
end

mapreduce.map = function (data)
    storage["mapreduce:data:mapcount"] = tablelength(data)
    for k,v in pairs(data) do
        local response = http.request {
            method = "POST",
            url = storage["mapreduce:config:urls:map"],
            params = { key = k },
            data = v
        }
        if response.statuscode == 200 then
            log(k.." mapped")
        else
            log(k.." failed to map")
        end
    end
end

mapreduce.reduce = function (data)
    storage["mapreduce:data:reducecount"] = tablelength(data)
    for k,v in pairs(data) do
        local response = http.request {
            method = "POST",
            url = storage["mapreduce:config:urls:reduce"],
            params = { key = k },
            data = v
        }
        if response.statuscode == 200 then
            log("reduce done")
        else
            log("reduce done: failed to send result")
        end
    end
end

mapreduce.send_result = function (data)
    local response = http.request {
        method = "POST",
        url = storage["mapreduce:config:urls:result"],
        data = data
    }
    if response.statuscode == 200 then
        log(k.." done")
    else
        log(k.." failed to send result")
    end
end

mapreduce.emit = function (request, key, value)
    if string.find(storage["mapreduce:config:urls:map"], request.path) then
        lease.acquire("mapreduce:data:groups")
        local current_groups = json.parse(storage["mapreduce:data:groups"])
        local values = {}
        if current_groups[key] then values = current_groups[key] end
        table.insert(values, value)
        current_groups[key] = values
        storage["mapreduce:data:groups"] = json.stringify(current_groups)
        lease.release("mapreduce:data:groups")
    elseif string.find(storage["mapreduce:config:urls:reduce"], request.path) then
        lease.acquire("mapreduce:data:results")
        local current_results = json.parse(storage["mapreduce:data:results"])
        current_results[key] = value
        storage["mapreduce:data:results"] = json.stringify(current_results)
        lease.release("mapreduce:data:results")
    end
end

mapreduce.continue = function (request)
    if string.find(storage["mapreduce:config:urls:map"], request.path) then
        lease.acquire("mapreduce:data:maptotal")
        local c = tonumber(storage["mapreduce:data:maptotal"]) + 1
        storage["mapreduce:data:maptotal"] = c
        lease.release("mapreduce:data:maptotal")
        if c == tonumber(storage["mapreduce:data:mapcount"]) then
            local groups = json.parse(storage["mapreduce:data:groups"])
            local ser_groups = {}
            for k,v in pairs(groups) do
                ser_groups[k] = json.stringify(v)
            end
            mapreduce.reduce(ser_groups)
        end
    elseif string.find(storage["mapreduce:config:urls:reduce"], request.path) then
        lease.acquire("mapreduce:data:reducetotal")
        local c = tonumber(storage["mapreduce:data:reducetotal"]) + 1
        storage["mapreduce:data:reducetotal"] = c
        lease.release("mapreduce:data:reducetotal")
        if c == tonumber(storage["mapreduce:data:reducecount"]) then
            mapreduce.send_result(storage["mapreduce:data:results"])
        end
    end
end

mapreduce.key = function (request)
    return request.query.key
end

mapreduce.value = function (request)
    if string.find(storage["mapreduce:config:urls:reduce"], request.path) then
        return json.parse(request.body)
    else
        return request.body
    end
end

mapreduce.result = function (request)
    return json.parse(request.body)
end


return mapreduce
