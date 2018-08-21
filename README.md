# 使用youzan/nsq的go-micro broker插件

## 用法

1、引入包

```go
import(
    _ "github.com/weisd/broker-youzan-nsq"
)
```

2、启动参数

```shell
./srv --broker=youzan --broker_address="<host1>:4161,<host2>:4161,<host3>:4161"
```