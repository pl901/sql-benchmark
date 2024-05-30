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

when the program runs,it will generate the report like this
```
root
|  MySqlBench-insert.csv
|  MySqlBench-update.csv   
|  MySqlBench-query.csv
|  SqlBench-summary.csv
└───sqlbenchmark-logs
│   │   sqlbenchmark.log

```
every database has three csv files,which means insert‘s metrics data,update's metrics data
and query's metrics data,and one summary file,which is the summary of all the csv files
"sqlbenchmark.log" file includes the log of the database connection pool's metrics
### 中文介绍
 基于springboot和JDK21开发的一个sql性能测试工具

 运行说明

 1.修改application.yml，设置正确的数据库配置

 2.打包sql-benchmark项目，将生成的jar文件放入根目录

 3.运行run.bat或者run.sh
 
当程序运行时，会生成一个报告，如
```
root
|  MySqlBench-insert.csv
|  MySqlBench-update.csv   
|  MySqlBench-query.csv
|  SqlBench-summary.csv
└───sqlbenchmark-logs
│   │   sqlbenchmark.log

```
 每种数据库都有三个csv文件，分别表示insert的指标数据，update的指标数据和query的指标数据，
 还有一个summary文件，是所有csv文件的汇总，"sqlbenchmark.log"文件包含了数据库连接池的指标数据
    