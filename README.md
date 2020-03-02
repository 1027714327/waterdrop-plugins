### 一、 waterdrop 合并文件
[toc]
#### 1. 开发背景

目前，业务中台数据需要准实时落入数仓，根据建设方案，在hive中，将临时表数据准实时合并到主表中，调度实践根据业务规则而定。

#### 2. 实现目标

根据传入的表相关信息，将临时表数据合并入主表。

#### 3. 部署

参考开源文档：https://github.com/InterestingLab/waterdrop

目前部署在CDH生产集群：slave6.pagoda.com.cn

部署路径：/home/bgyetl/waterdrop-1.4.0

#### 4. 编写配置文件

<img src="C:\Users\刘唐荣\AppData\Local\Temp\企业微信截图_15825595344015.png" alt="img" style="zoom:75%;" />

编写需要分四步：

- 编写主配置

```
spark {
  spark.app.name = "state_Waterdrop" # state_表名称
  spark.executor.instances = 2
  spark.executor.cores = 4
  spark.executor.memory = "5g"
}
```

- 编写输入配置
```
input {
    hive {
        pre_sql = "select * from ods.erp_mag_customer_bgy where part_date ='20200224' limit 100 "
        table_name = "erp_mag_customer_bgy"
    }
}
```


- 编写处理逻辑
```
filter {
    remove {
        source_field = ["part_date","synctime"]
    }
}
```


- 编写输出配置
```
output {
    stdout {}
}
```

- 合成完整配置文件

```json
spark {
  spark.app.name = "Waterdrop"
  spark.executor.instances = 2
  spark.executor.cores = 4
  spark.executor.memory = "5g"
}
input {
    hive {
        pre_sql = "select * from ods.erp_mag_customer_bgy where part_date ='20200224' limit 100 "
        table_name = "erp_mag_customer_bgy"
    }
}
filter {
    remove {
        source_field = ["part_date","synctime"]
    }
}
output {
    stdout {}
}
```



#### 5. 启动

```shell
cd /data/home/liutangrong/data_merge/bin
sh start_wd.sh xxx.config
```

```shell
#! /bin/bash
current_path=`pwd`
case "`uname`" in
    Linux)
                bin_abs_path=$(readlink -f $(dirname $0))
                ;;
        *)
                bin_abs_path=`cd $(dirname $0); pwd`
                ;;
esac
base=${bin_abs_path}/..
echo "config is $base/conf/$1"
# ../bin/start-waterdrop.sh --master yarn --deploy-mode client --config ./$1
nohup sh /home/bgyetl/waterdrop-1.4.0/bin/start-waterdrop.sh --master yarn --deploy-mode client --config $base/conf/$1 &
```
**注意：配置文件名称为: state_表名称.conf，这是一个强制执行的规定。**

#### 6. 调度配置

目前调度暂时选用linux自带调度软件调度；也可使用xxl-job调度。

#### 7.自研插件
##### 7.1. input插件-MHive

* Description
  自定义SQL解析，可以方便根据情景查询指定需要的数据。
* Options

| name                                                         | type   | required | default value\example value |
| ------------------------------------------------------------ | ------ | -------- | ------------- |
| pre_sql | string | yes     | -          |
| result_table_name| string | yes    | -        |
| time_format| string | no       | yyyyMMdd     |
| rank_partition_keys| List | no       | ["id","mem_id"] |
| rank_order_keys| List | no       | ["part_date desc"] |
**pre_sql：**需要查询数据的SQL。
**result_table_name：**注册到Spark全局表名称，供后续流程使用。
**time_format：**时间格式化默认格式。
**rank_partition_keys：**排序分区字段，根据rank_order_keys取第一条数据。
**rank_order_keys：**排序规则。

* Other

  | parameter | explain                                          | example    |
  | --------- | ------------------------------------------------ | ---------- |
  | bizDate   | 当前时间减一天，T+1常用（格式以time_format为准） | ${bizDate} |
  | nowDate   | 当前时间（格式以time_format为准）                | ${nowDate} |
  | d         | 天                                               | 5d         |
  | h         | 小时                                             | 5h         |
  | m         | 分钟                                             | 5m         |
  | s         | 秒                                               | 5s         |

  以上参数可以再筛选分区或时间上被自定义使用，举几个例子：

  part_date='${nowDate}'：表示part_date会以time_format为格式的时间作为参数筛选数据。

  part_date='${nowDate-1d}'：表示part_date会以time_format为格式的时间且当前时间减一天作为参数筛选数据。

  part_date='${nowDate-1d,yyyyMMddHH}'：表示part_date会以yyyyMMddHH为时间格式作为参数筛选数据，这是不以time_format为基准。

  目前没有实现月和年，没有具体需求，所以不做过度设计。

* Example
```
  com.pagoda.waterdrop.input.MHive {
    pre_sql = "select * from state.f_mem_rchg_dtl_tmp where part_date ='${nowDate-5d}'"
    result_table_name = "f_mem_rchg_dtl_temp"
    time_format="yyyyMMdd"
    rank_partition_keys=["recordId"]
    rank_order_keys =["part_date desc","sync_time desc" ]
  }
```
以上配置表达信息为：取f_mem_rchg_dtl_tmp part_date=nowDate-5天的分区数据，然后根据recordId分区，按part_date 和sync_time 降序，取第一条数据。

##### 7.2. filter插件-MergeData（自定义合并数据插件）

* Description
  自定义合并数据插件，准实时合并数据。
* Options

| name              | type   | required | default value |
| ----------------- | ------ | -------- | ------------- |
| master_table      | string | yes      | -             |
| tmp_table         | string | yes      | -             |
| join_keys        | list | no       | -     |
| join_type         | string | no       | left join     |
| result_table_name | string   | no       | -             |
**master_table：**合并数据的主表。
**tmp_table：**合并数据的临时表。
**join_keys：**主表与临时表关联字段集合。
**join_type：**主表与临时表关联类型。
**result_table_name：**注册到Spark全局表名称，供后续流程使用。
* Example

```
   com.pagoda.waterdrop.filter.MergeData{
     master_table="f_mem_rchg_dtl"
     tmp_table="f_mem_rchg_dtl_temp"
     join_type= "left join"
     join_keys=["recordId"]
     result_table_name="instet_table"
   }
```

##### 7.3. out插件-InsertHive

* Description
  自定义写入数据到Hive插件，解决数据写入Hive不刷新元数据问题。
* Options

| name              | type   | required | default value |
| ----------------- | ------ | -------- | ------------- |
| source_table_name | string | yes      | -             |
| save_mode         | string | yes      | error         |
| table             | string | no       | -             |
**source_table_name：**需要输出到hive来源表。
**save_mode：**数据保存模式，参考spark。
**table：**写入hive的目标表。
* Example

```
   com.pagoda.waterdrop.output.InsertHive {
    source_table_name = "f_mem_rchg_dtl_temp"
    save_mode="overwrite"
    table="state.f_mem_rchg_dtl_tmp"
   }
```
注意，如果是分区表，必须添加以下参数：
```
  spark.sql.catalogImplementation = "hive"
  spark.security.credentials.hive.enabled="false"
  spark.security.credentials.hbase.enabled="false"
  hive.exec.dynamic.partition.mode="nonstrict"
```


##### 7.4. 完整例子
```
spark {
  spark.app.name = "mysql_2_state:f_mem_rchg_dtl_temp"
  spark.executor.instances = 2
  spark.executor.cores = 2
  spark.executor.memory = "2g"
  spark.sql.catalogImplementation = "hive"
  spark.security.credentials.hive.enabled="false"
  spark.security.credentials.hbase.enabled="false"
  hive.exec.dynamic.partition.mode="nonstrict"
}
input {
  com.pagoda.waterdrop.input.MHive {
    pre_sql = "select * from state.f_mem_rchg_dtl_tmp where part_date ='${nowDate-5d}'"
    result_table_name = "f_mem_rchg_dtl_temp"
    time_format="yyyyMMdd"
    rank_partition_keys=["recordId"]
    rank_order_keys =["part_date desc","sync_time desc" ]
  }
  com.pagoda.waterdrop.input.MHive {
    pre_sql = "select * from ods.f_mem_rchg_dtl  where part_date ='${nowDate-2d}'"
    result_table_name = "f_mem_rchg_dtl"
    time_format="yyyyMMdd"
    rank_partition_keys=["recordId"]
    rank_order_keys =["part_date desc","sync_time desc" ]
  }
}
filter {
   com.pagoda.waterdrop.filter.MergeData{
     master_table="f_mem_rchg_dtl"
     tmp_table="f_mem_rchg_dtl_temp"
     join_type= "left join"
     join_keys=["recordId"]
     result_table_name="instet_table"
   }
}
output {
   com.pagoda.waterdrop.output.InsertHive {
    source_table_name = "instet_table"
    save_mode="overwrite"
    table="state.f_mem_rchg_dtl_tmp"
   }
}
```
