# 大数据成长之路

## Hadoop
### 历史之路
[Hadoop十年解读](https://www.infoq.cn/article/hadoop-ten-years-interpretation-and-development-forecast)

### HDFS JavaAPI
#### 副本因子的坑
```text
如果通过hdfs shell上传的文件那么他的副本因子是根据 hdfs-site.xml中的配置,
如果是通过Java API方式那么他会使用副本因子为3的配置

```
### 项目实践
#### 用户行为日志分析

**日志数据内容**
* 访问的系统属性:操作系统、浏览器等等
* 访问特征:点击的url、从哪个url跳转过的(referer)、页面停留时间等
* 访问信息:session_id、访问ip

**数据处理流程**
* 数据采集 Flume:Web日志写入HDFS中
* 数据清洗 脏数据清理:Spark、Hive、MapReduce
* 数据处理 按照需求进行相应业务的统计和分析
* 数据处理结果入库   结果可以存放到RDBMS、NoSQL等
* 数据的可视化  通过图形化展示的方式展现出来:饼图、柱状图、地图等

### HDFS文档
* [官方文档](https://hadoop.apache.org/docs/r3.2.1/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
* [石墨笔记](https://shimo.im/docs/RjGgVxDJ8KT96xr8/)
* [HDFS文件读取写入流程](https://www.processon.com/view/link/5e40b7e4e4b085b5f21a193d)
