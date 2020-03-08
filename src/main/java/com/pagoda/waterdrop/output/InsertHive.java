package com.pagoda.waterdrop.output;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.interestinglab.waterdrop.apis.BaseOutput;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author 刘唐荣
 * @version 1.0.0
 * @ClassName InsertHive.java
 * @Description TODO
 * @createTime 2020-02-27 14:02:00
 */
public class InsertHive extends BaseOutput {


    private Config config = ConfigFactory.empty();

    @Override
    public Config getConfig() {
        return this.config;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }


    @Override
    public void process(Dataset<Row> df) {
        //double num = Integer.valueOf(1.0 * (df.count()) / 100);
        String tableName=config.getString("table");
        String saveMode = config.getString("save_mode");
        String pre_sql = config.getString("pre_sql");
        if (!"None".equals(pre_sql)){
            df.sparkSession().sql(pre_sql);
            return;
        }

        if ("overwrite".equals(saveMode)) {
            df.write().mode(saveMode).saveAsTable(tableName+"_tmp");
            Dataset<Row> newDf = df.sparkSession().table(tableName + "_tmp");
            newDf.write().mode(saveMode).saveAsTable(tableName);
            df.sparkSession().sql("drop table "+tableName + "_tmp");
        } else {
            DataFrameWriter<Row> writer = df.write().mode(saveMode);
            writer.insertInto(tableName);
        }

    }

    @Override
    public Tuple2<Object, String> checkConfig() {
        boolean isExist = config.hasPath("table") && Objects.nonNull(config.getString("table"));
        if (isExist) {
            return new Tuple2<>(true, "");
        } else {
            return new Tuple2<>(false, "please specify  table is not null ");
        }
    }


    @Override
    public void prepare(SparkSession spark) {
        Map<String, Object> map = new HashMap();
        map.put("save_mode", "error");
        map.put("pre_sql","None");
        Config defaultConfig = ConfigFactory.parseMap(map);
        config = config.withFallback(defaultConfig);
    }

}
