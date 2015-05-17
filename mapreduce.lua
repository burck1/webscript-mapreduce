function tablelength(T)
    local count = 0
    for _ in pairs(T) do count = count + 1 end
    return count
end

local mapreduce = {}

mapreduce.setup = function (map_url, mapresults_url, reduce_url, reduceresults_url, result_url)
    storage["mapreduce:config:urls:map"] = map_url
    storage["mapreduce:config:urls:mapresults"] = mapresults_url
    storage["mapreduce:config:urls:reduce"] = reduce_url
    storage["mapreduce:config:urls:reduceresults"] = reduceresults_url
    storage["mapreduce:config:urls:result"] = result_url
    storage["mapreduce:data:maptotal"] = 0
    storage["mapreduce:data:reducetotal"] = 0
    storage["mapreduce:data:groups"] = json.stringify({})
    storage["mapreduce:data:results"] = json.stringify({})
end

mapreduce.html = function (initiate_url)
    local html = [[<!DOCTYPE html>
<html>
  <head>
    <link rel="stylesheet" type="text/css" href="http://yui.yahooapis.com/pure/0.6.0/buttons-min.css">
  </head>
  <body>
    <button id="go" type="button" class="pure-button pure-button-primary">GO</button>
    <pre id="result"></pre>
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.4/jquery.min.js"></script>
    <script type="text/javascript">
    function initiate(url, done, fail, always) {
      fail = typeof fail !== "undefined" ? fail : function(jqXHR, textStatus){console.log("Error: initiate: "+textStatus);};
      always = typeof always !== "undefined" ? always : function(){};
      return $.ajax({
        method: "GET",
        url: url,
        dataType: "json"
      }).done(done).fail(fail).always(always);
    }
    function map(url, key, value, done, fail, always) {
      fail = typeof fail !== "undefined" ? fail : function(jqXHR, textStatus){console.log("Error: map: "+key+": "+textStatus);};
      always = typeof always !== "undefined" ? always : function(){};
      return $.ajax({
        method: "POST",
        url: url + "?key=" + key,
        data: value,
        contentType: "text/plain",
        processData: false
      }).done(done).fail(fail).always(always);
    }
    function mapresults(url, done, fail, always) {
      fail = typeof fail !== "undefined" ? fail : function(jqXHR, textStatus){console.log("Error: mapresults: "+textStatus);};
      always = typeof always !== "undefined" ? always : function(){};
      return $.ajax({
        method: "GET",
        url: url,
        dataType: "json"
      }).done(done).fail(fail).always(always);
    }
    function reduce(url, key, value, done, fail, always) {
      fail = typeof fail !== "undefined" ? fail : function(jqXHR, textStatus){console.log("Error: reduce: "+key+": "+textStatus);};
      always = typeof always !== "undefined" ? always : function(){};
      return $.ajax({
        method: "POST",
        url: url + "?key=" + key,
        data: value,
        contentType: "text/plain",
        processData: false
      }).done(done).fail(fail).always(always);
    }
    function reduceresults(url, done, fail, always) {
      fail = typeof fail !== "undefined" ? fail : function(jqXHR, textStatus){console.log("Error: reduceresults: "+textStatus);};
      always = typeof always !== "undefined" ? always : function(){};
      return $.ajax({
        method: "GET",
        url: url,
        dataType: "text"
      }).done(done).fail(fail).always(always);
    }
    function result(url, data, done, fail, always) {
      fail = typeof fail !== "undefined" ? fail : function(jqXHR, textStatus){console.log("Error: result: "+textStatus);};
      always = typeof always !== "undefined" ? always : function(){};
      return $.ajax({
        method: "POST",
        url: url,
        data: data,
        contentType: "text/plain",
        processData: false,
        dataType: "json"
      }).done(done).fail(fail).always(always);
    }
    var initiate_url = "]]..initiate_url..[[";
    $(function() {
      $("#go").click(function() {
        initiate(initiate_url, function(initiate_data) {
          var map_requests = Array();
          $.each(initiate_data["data"], function(key, value) {
            map_requests.push(map(initiate_data["links"]["map"], key, value, function(){
              console.log("map: " + key);
            }));
          });
          function allMapsDone() {
            mapresults(initiate_data["links"]["mapresults"], function(reduce_data) {
              var reduce_requests = Array();
              $.each(reduce_data, function(key2, value2) {
                reduce_requests.push(reduce(initiate_data["links"]["reduce"], key2, value2, function() {
                  console.log("reduce: " + key2);
                }));
              });
              function allReducesDone() {
                reduceresults(initiate_data["links"]["reduceresults"], function(result_data) {
                  result(initiate_data["links"]["result"], result_data, function(results){
                    console.log("results sent");
                    $("#result").html(JSON.stringify(results, undefined, 4));
                  });
                });
              }
              $.when.apply($, reduce_requests).done(allReducesDone).fail(allReducesDone);
            });
          }
          $.when.apply($, map_requests).done(allMapsDone).fail(allMapsDone);
        });
      });
    });
    </script>
  </body>
</html>]]
    return html, {["Content-Type"]="text/html"}
end

mapreduce.initiate = function (data)
    storage["mapreduce:data:mapcount"] = tablelength(data)

    local response = {}

    response["links"] = {}
    response["links"]["map"] = storage["mapreduce:config:urls:map"]
    response["links"]["mapresults"] = storage["mapreduce:config:urls:mapresults"]
    response["links"]["reduce"] = storage["mapreduce:config:urls:reduce"]
    response["links"]["reduceresults"] = storage["mapreduce:config:urls:reduceresults"]
    response["links"]["result"] = storage["mapreduce:config:urls:result"]

    response["data"] = {}
    for k,v in pairs(data) do
        local data_location = "mapreduce:data:map:"..k
        storage[data_location] = v
        response["data"][k] = data_location
    end

    return json.stringify(response), {["Content-Type"]="application/json"}
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

mapreduce.mapresults = function()
    local groups = json.parse(storage["mapreduce:data:groups"])
    storage["mapreduce:data:reducecount"] = tablelength(groups)
    local groups_locations = {}
    for k,v in pairs(groups) do
        local data_location = "mapreduce:data:reduce:"..k
        storage[data_location] = json.stringify(v)
        groups_locations[k] = data_location
    end
    return json.stringify(groups_locations), {["Content-Type"]="application/json"}
end

mapreduce.reduceresults = function()
    return "mapreduce:data:results", {["Content-Type"]="text/plain"}
end

mapreduce.key = function (request)
    return request.query.key
end

mapreduce.value = function (request)
    if string.find(storage["mapreduce:config:urls:map"], request.path) then
        return storage[request.body]
    elseif string.find(storage["mapreduce:config:urls:reduce"], request.path) then
        return json.parse(storage[request.body])
    elseif string.find(storage["mapreduce:config:urls:result"], request.path) then
        return json.parse(storage[request.body])
    else
        return request.body
    end
end

return mapreduce
