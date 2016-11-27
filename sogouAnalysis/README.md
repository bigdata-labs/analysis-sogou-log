在**本地测试**时, 可能会出现`Utils: Service 'sparkDriver' could not bind on port 0`.这时只要设置一下环境变量:
`SPARK_LOCAL_IP=127.0.0.1`.