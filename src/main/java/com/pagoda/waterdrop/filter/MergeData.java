package com.pagoda.waterdrop.filter;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.interestinglab.waterdrop.apis.BaseFilter;
import org.apache.spark.internal.Logging;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.spark_project.guava.base.Joiner;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author 刘唐荣
 * @version 1.0.0
 * @ClassName MSql.java
 * @Description TODO
 * @createTime 2020-02-28 13:41:00
 */
public class MergeData extends BaseFilter {

    Config conf = ConfigFactory.empty();

    @Override
    public void prepare(SparkSession spark) {
        Map<String, Object> map = new HashMap();
        map.put("join_type", " left join ");
        map.put("union_tmp_table_where_sql", " 1=1 ");
        map.put("union_tmp_table_col", " *  ");
        Config defaultConfig = ConfigFactory.parseMap(map);
        conf = conf.withFallback(defaultConfig);
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> df) {
        String masterTable = conf.getString("master_table");
        String tmpTable = conf.getString("tmp_table");
        String joinType = conf.getString("join_type");
        String resultTableName = conf.getString("result_table_name");
        String unionTmpTableWhereSql=conf.getString("union_tmp_table_where_sql");
        String unionTmpTableCol=conf.getString("union_tmp_table_col");

        String joinString = Joiner.on(" and ").join(conf.getStringList("join_keys").stream().map(f -> " m." + f + "= t." + f).collect(Collectors.toList()));
        String whereSQL = "  WHERE " + Joiner.on(" and ").join(conf.getStringList("join_keys").stream().map(f -> " t." + f + " is null ").collect(Collectors.toList()));
        Dataset<Row> diffMasterTable = spark.sql("SELECT  m.*  FROM  " + masterTable + "  as m " + joinType + "  " + tmpTable + "  as t on " + joinString + whereSQL);
        Dataset<Row> tmpTableDF = spark.sql("SELECT "+unionTmpTableCol+" FROM " + tmpTable + " where " + unionTmpTableWhereSql);
        Dataset<Row> allDf=tmpTableDF.unionAll(diffMasterTable);
        allDf.createOrReplaceTempView(resultTableName);
        return allDf;
    }

    @Override
    public void setConfig(Config config) {
        this.conf = config;
    }

    @Override
    public Config getConfig() {
        return this.conf;
    }

    /**
     * master_table
     * tmp_table
     * join_keys
     * join_type
     * result_table_name
     *
     * @return
     */
    @Override
    public Tuple2<Object, String> checkConfig() {
        if (conf.hasPath("table_name")) {
            if (conf.hasPath("table_name")) {
                log().warn("parameter [table_name] is deprecated since 1.4");
            }
            return new Tuple2(true, "");
        } else {
            return new Tuple2(true, "");
        }
    }
}
