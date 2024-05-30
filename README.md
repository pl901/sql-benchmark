develop based on spring boot and JDK21

how to run 

here is the project structure
```
root
│   jre_linux_aarch64
│   jre_linux_x64    
│   jre_win_x64
│   application.yml
│   run.bat
│   run.sh
|   sql-benchmark-1.0-SNAPSHOT.jar
```
1.modify the application.yml,set the right database config

2.package the sql-benchmark project and put jar into the root folder

3.run the run.bat or run.sh

### 中文介绍
 基于springboot和JDK21开发的一个sql性能测试工具

 运行说明

 1.修改application.yml，设置正确的数据库配置

 2.打包sql-benchmark项目，将生成的jar文件放入根目录

 3.运行run.bat或者run.sh