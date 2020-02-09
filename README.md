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