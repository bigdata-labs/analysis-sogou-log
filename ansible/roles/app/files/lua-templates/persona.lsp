{%

layout = "layouts/default.lsp"

local  key= ngx.var.arg_key
local  title = "persona"

local red = redis:new()
red:set_timeout(1000)
local ok, err = red:connect("192.168.7.154", 6379)
if not ok then
   ngx.say("failed to connect: ", err)
   return
end

local tags, err = red:zrevrange(key, 0, 10, "withscores")


%}
<div>
user: {{ key }} <br/>

{% for i = 1,  #tags, 2 do %}

     tag: {* tags[i] *} score: {* tags[i+1] *} </br>

{% end %}<br/>

</div>
