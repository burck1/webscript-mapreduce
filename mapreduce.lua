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

mapreduce.initiate = function (data)
    storage["mapreduce:data:mapcount"] = tablelength(data)

    local data_html = ''
    for k,v in pairs(data) do
        local data_location = "mapreduce:data:map:"..k
        storage[data_location] = v
        data_html = data_html..'"'..k..'":"'..data_location..'",'
    end

    local html = [[<!DOCTYPE html>
<html>
  <head>
    <link rel="stylesheet" type="text/css" href="http://yui.yahooapis.com/pure/0.6.0/buttons-min.css">
  </head>
  <body>
    <button id="go" type="button" class="pure-button pure-button-primary">GO</button>
    <h2 id="result"></h2>
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.4/jquery.min.js"></script>
    <script type="text/javascript">
    var map_url = "]]..storage["mapreduce:config:urls:map"]..[[";
    var reduce_url = "]]..storage["mapreduce:config:urls:reduce"]..[[";
    var result_url = "]]..storage["mapreduce:config:urls:result"]..[[";
    var map_data = {]]..data_html..[[};
    $(function() {
      $("#go").click(function() {
        $.each(map_data, function(key, value) {
          $.ajax({
            method: "POST",
            url: map_url + "?key=" + key,
            data: value,
            contentType: "text/plain",
            processData: false,
            dataType: "json",
            timeout: 10000
          }).done(function(reduce_data) {
            console.log("map: " + key);
            if (!$.isEmptyObject(reduce_data)) {
              $.each(reduce_data, function(key2, value2) {
                $.ajax({
                  method: "POST",
                  url: reduce_url + "?key=" + key2,
                  data: value2,
                  contentType: "text/plain",
                  processData: false,
                  dataType: "text",
                  timeout: 10000
                }).done(function(result_data) {
                  console.log("reduce: " + key2);
                  if (result_data) {
                    $.ajax({
                      method: "POST",
                      url: result_url,
                      data: result_data,
                      contentType: "text/plain",
                      processData: false,
                      timeout: 10000
                    }).done(function(){
                      console.log("results sent");
                      $("#result").html("DONE");
                    });
                  }
                }).fail(function(jqXHR, textStatus){
                  if(textStatus === 'timeout') {
                    console.log("Timeout: reduce: " + key2);
                  }
                });
              });
            }
          }).fail(function(jqXHR, textStatus){
            if(textStatus === 'timeout') {
              console.log("Timeout: map: " + key);
            }
          });
        });
      });
    });
    </script>
  </body>
</html>]]
    return html, {["Content-Type"]="text/html"}
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
            storage["mapreduce:data:reducecount"] = tablelength(groups)
            local groups_locations = {}
            for k,v in pairs(groups) do
                local data_location = "mapreduce:data:reduce:"..k
                storage[data_location] = json.stringify(v)
                groups_locations[k] = data_location
            end
            return json.stringify(groups_locations), {["Content-Type"]="application/json"}
        else
            return "{}", {["Content-Type"]="application/json"}
        end
    elseif string.find(storage["mapreduce:config:urls:reduce"], request.path) then
        lease.acquire("mapreduce:data:reducetotal")
        local c = tonumber(storage["mapreduce:data:reducetotal"]) + 1
        storage["mapreduce:data:reducetotal"] = c
        lease.release("mapreduce:data:reducetotal")
        if c == tonumber(storage["mapreduce:data:reducecount"]) then
            return "mapreduce:data:results", {["Content-Type"]="text/plain"}
        else
            return "", {["Content-Type"]="text/plain"}
        end
    end
end

mapreduce.key = function (request)
    return request.query.key
end

mapreduce.value = function (request)
    if string.find(storage["mapreduce:config:urls:map"], request.path) then
        return storage[request.body]
    elseif string.find(storage["mapreduce:config:urls:reduce"], request.path) then
        return json.parse(storage[request.body])
    else
        return request.body
    end
end

mapreduce.result = function (request)
    return json.parse(storage[request.body])
end


return mapreduce
