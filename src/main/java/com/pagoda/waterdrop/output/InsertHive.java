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
        DataFrameWriter<Row> writer = df.write().mode(config.getString("save_mode"));
        writer.insertInto(config.getString("table"));
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
        map.put("save_mode","error");
        // map.put("save_mode","error");
        Config defaultConfig = ConfigFactory.parseMap(map);
        config = config.withFallback(defaultConfig);
    }

}
