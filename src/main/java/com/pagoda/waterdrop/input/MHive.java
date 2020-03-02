package com.pagoda.waterdrop.input;

import com.pagoda.waterdrop.Tools;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.interestinglab.waterdrop.apis.BaseStaticInput;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.WindowSpec;
import org.spark_project.guava.base.Joiner;
import scala.Tuple2;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions.*;
import scala.collection.JavaConverters;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author 刘唐荣
 * @version 1.0.0
 * @ClassName MHive.java
 * @Description TODO
 * @createTime 2020-02-27 22:07:00
 */
public class MHive extends BaseStaticInput {

    Config config = ConfigFactory.empty();

    @Override
    public Dataset<Row> getDataset(SparkSession spark) {
        String regTable = config.getString("result_table_name");
        String preSql = config.getString("pre_sql");
        String timeFormat = config.getString("time_format");
        List<String> rankPartitionKeys = config.getStringList("rank_partition_keys");
        List<String> rankOrderKeys = config.getStringList("rank_order_keys");
        //boolean addSyncTime = config.getBoolean("add_sync_time");
        List<String> keys = Tools.getTemplateKey(preSql);
        Map<String, Object> paras = Tools.processTemplateKey(keys, timeFormat);
        String newPreSql = Tools.processTemplate(preSql, paras);
        Dataset<Row> tmpDataSet = spark.sql(newPreSql);
        Optional<Column> winFunc = Optional.empty();
        if (!rankPartitionKeys.isEmpty() && !rankOrderKeys.isEmpty()) {
            List<Column> partitionKeyCols = rankPartitionKeys.stream().map(f -> functions.col(f)).collect(Collectors.toList());
            List<Column> orderKeyCols = rankOrderKeys.stream().map(f -> {
                String[] orderKeys = f.split(" ");
                String orderType = orderKeys.length > 1 ? orderKeys[1].toLowerCase().trim() : "";
                Column defCol;
                switch (orderType) {
                    case "asc":
                        defCol = functions.col(orderKeys[0].trim()).asc();
                        break;
                    case "desc":
                        defCol = functions.col(orderKeys[0].trim()).desc();
                        break;
                    default:
                        defCol = functions.col(orderKeys[0].trim());
                        break;
                }
                return defCol;
            }).collect(Collectors.toList());

            WindowSpec window = Window.partitionBy(JavaConverters.asScalaBufferConverter(partitionKeyCols).asScala())
                    .orderBy(JavaConverters.asScalaBufferConverter(orderKeyCols).asScala());
            winFunc = Optional.of(functions.row_number().over(window));
        }
        Dataset<Row> dataSet;
        if (winFunc.isPresent()) {
            dataSet = tmpDataSet.withColumn("rank", winFunc.get())
                    .where("rank = 1")
                    .drop(functions.col("rank"));
        } else {
            dataSet = tmpDataSet;
        }
        dataSet.createOrReplaceTempView(regTable);
        return dataSet;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return this.config;
    }

    @Override
    public Tuple2<Object, String> checkConfig() {
        if (config.hasPath("pre_sql")) {
            return new Tuple2<>(true, "");
        } else {
            return new Tuple2<>(false, "please specify [pre_sql]");
        }
    }

    /**
     * ,date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss")        as sync_time     -- 跑数时间
     * ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as sync_time --跑数时间
     * ,Row_Number() over(partition by memberaccountid order by modifytime desc)  rank
     *
     * @throws
     * @author: 刘唐荣
     * @date: 2020/3/2 10:49
     * @param: null
     * @return:
     */
    @Override
    public void prepare(SparkSession spark) {
        Map<String, Object> map = new HashMap();
        map.put("time_format", "yyyyMMdd");
        map.put("rank_partition_keys", new ArrayList<String>());
        map.put("rank_order_keys", new ArrayList<String>());
        //map.put("add_sync_time", false);
        Config defaultConfig = ConfigFactory.parseMap(map);
        config = config.withFallback(defaultConfig);
    }
}
