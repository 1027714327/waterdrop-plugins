package com.pagoda.waterdrop.output;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.interestinglab.waterdrop.apis.BaseOutput;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 修复表的分区数据
 *
 * @author 刘唐荣
 * @version 1.0.0
 * @ClassName MsckRepairTable.java
 * @Description TODO
 * @createTime 2020-02-27 10:31:00
 */
public class MsckRepairTable extends BaseOutput {

    private Config config = ConfigFactory.empty();

    @Override
    public Config getConfig() {
        return this.config;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    /**
     * table list 要修复的名称列表
     * 检查配置
     *
     * @return
     */
    @Override
    public Tuple2<Object, String> checkConfig() {
        boolean isExist = !config.hasPath("table") || config.hasPath("table") && config.getStringList("table").size() > 0;
        if (isExist) {
            return new Tuple2<>(true, "");
        } else {
            return new Tuple2<>(false, "please specify [table] as List");
        }
    }

    @Override
    public void prepare(SparkSession spark) {
        Map<String, Object> map = new HashMap();
        Config defaultConfig = ConfigFactory.parseMap(map);
        config = config.withFallback(defaultConfig);
    }

    @Override
    public void process(Dataset<Row> df) {
        List<String> tables = config.getStringList("table");
        SparkSession sparkSession = df.sparkSession();
        tables.stream().forEach(tb -> {
            if (!"".equals(tb) && Objects.nonNull(tb)) {
                sparkSession.sql(String.format("MSCK REPAIR TABLE %s ", tb));
            }
        });
    }

}
