# net
### net working on quic-go ,it is go to c project!
### 本项目主要用于提供C、C++、go等跨语言跨平台的基础网络连接支持
#### 项目环境需要 go 1.25.9 及以上版本的支持
1. build.bat 用于编译windows平台。
2. build.sh 用于编译linux平台。
3. 生成的结果存于output目录内，C或c++程序需要引用net.h文件，并使用对应的静态或动态文件。
4. 新增xdp的代理支持。

#### 代码架构
1. agent       代理逻辑
2. client      客户端逻辑
3. exports     导出的api
4. framework   架构基础
5. ice         pion ice打洞支持
6. main        导出的入口
7. output      成果输出目录
8. server      服务端逻辑
9. tests       单元测试
10. websocket   websocket协议支持

#### FAQ
1. 打包出现 go/pkg/tool/linux_amd64/link: running gcc failed: exit status 1
```text
问题点： app.rc 生成的 app.syso 导致了跨平台编译问题，通过脚本在执行前删除此文件来保证执行的正确性！
```

2. go在生成dll时，会自动将导出目录下的所有api都生成.h的文件内，我们通过引用的方式加入