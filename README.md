# Elasticsearch demo

Spring boot2.3.4 + Elasticsearch7.x Java demo：

- elasticsearch版本7.9.2，使用Java High Level REST Client交互ES
- [IndexService](https://github.com/Mengzuozhu/es-demo/blob/master/src/main/java/com/mzz/esdemo/service/IndexService.java) 索引管理常用功能示例：配置（setting）、映射(mapping)、索引（index）管理、结构与数据复制等
- [DocumentService](https://github.com/Mengzuozhu/es-demo/blob/master/src/main/java/com/mzz/esdemo/service/DocumentService.java) 文档管理常用功能示例：增删改查文档
- [SearchService](https://github.com/Mengzuozhu/es-demo/blob/master/src/main/java/com/mzz/esdemo/service/SearchService.java) 搜索常用功能示例：matchAllQuery，termsQuery，rangeQuery，matchQuery等
- [AggregationService](https://github.com/Mengzuozhu/es-demo/blob/master/src/main/java/com/mzz/esdemo/service/AggregationService.java) 聚合常用功能示例：最值、平均值、唯一值数目等
- [EsSearchWrapper](https://github.com/Mengzuozhu/es-demo/blob/master/src/main/java/com/mzz/esdemo/handler/EsSearchWrapper.java) 封装：分页（from size）、游标（Scroll）、Search After查询

[相关博客地址](https://blog.csdn.net/m0_37862405/article/details/113529247)



**参考文档：**

[https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.x/java-rest-high-getting-started.html](https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.x/java-rest-high-getting-started.html)

[https://www.elastic.co/guide/cn/elasticsearch/guide/current/getting-started.html](https://www.elastic.co/guide/cn/elasticsearch/guide/current/getting-started.html)

