{%

layout = "layouts/default.lsp"

local  pageindex = ngx.var.arg_pageindex
local  title = "index"


if pageindex == nil or tonumber(pageindex) < 1 then
  pageindex = 1
end
local pageSize = 10
local cursor = (tonumber(pageindex) - 1) * pageSize

local red = redis:new()
red:set_timeout(1000)
local ok, err = red:connect("192.168.7.154", 6379)
if not ok then
   ngx.say("failed to connect: ", err)
   return
end

red:select(0)
local result, err = red:scan(cursor, "MATCH", "*", "COUNT", pageSize)

if result == nil then
   ngx.say("failed to scan: ", err)
   return
end

local newCursor = result[1]
local keyList = result[2]

%}
Users:
<div>
{% for i = 1, #keyList do %}
     {# {% if i > 1 then %},{% end %} #}
     <a href="/persona.lsp?key={* keyList[i] *}">{* keyList[i] *}</a> </br>
{% end %}<br/>
</div>

{% if tonumber(pageindex) > 1 then %}
<a href="?pageindex={* pageindex - 1 *}">上一页</a>
{% end %}
{% if tonumber(newCursor) > 0 then %}
<a href="?pageindex={* pageindex + 1 *}">下一页</a>
{% end %}
