{%

layout = "layouts/default.lsp"

local  pageindex = ngx.var.pageindex
local pageszie = ngx.var.pagesize
local  title = "index"


local red = redis:new()
red:set_timeout(1000)
local ok, err = red:connect("192.168.7.152", 6379)
if not ok then
   ngx.say("failed to connect: ", err)
   return
end

local keyList, err = red:keys("*")

%}
<div>
{% for i = 1, #keyList do %}
     {% if i > 1 then %},{% end %}
     <a href="/persona.lsp?key={* keyList[i] *}">{* keyList[i] *}</a> </br>

{% end %}<br/>
</div>
