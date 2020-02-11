# 大数据成长之路

## Hadoop
### 历史之路
[Hadoop十年解读](https://www.infoq.cn/article/hadoop-ten-years-interpretation-and-development-forecast)

### HDFS JavaAPIgit
#### 副本因子的坑
```text
如果通过hdfs shell上传的文件那么他的副本因子是根据 hdfs-site.xml中的配置,
如果是通过Java API方式那么他会使用副本因子为3的配置

```
### HDFS文档
* [官方文档](https://hadoop.apache.org/docs/r3.2.1/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
* [石墨笔记](https://shimo.im/docs/RjGgVxDJ8KT96xr8/)
* [HDFS文件读取写入流程](https://www.processon.com/view/link/5e40b7e4e4b085b5f21a193d)
