{%

layout = "layouts/default.lsp"

local  key= ngx.var.arg_key
local  title = "hotword"

local red = redis:new()
red:set_timeout(1000)
local ok, err = red:connect("192.168.7.154", 6379)
if not ok then
   ngx.say("failed to connect: ", err)
   return
end
red:select(2)
local tags, err = red:zrevrange("hotwordlist", 0, 100, "withscores")


%}
<div>
{% for i = 1,  #tags, 2 do %}

     query: {* tags[i] *} count: {* tags[i+1] *} </br>

{% end %}<br/>

</div>
