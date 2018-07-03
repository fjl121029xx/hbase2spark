package com.li.hbase2spark.udaf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class VideoPlayModuleTimeUDAF extends UserDefinedAggregateFunction {
    @Override
    public StructType inputSchema() {

        return DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("videoId", DataTypes.StringType, true),
                DataTypes.createStructField("playTime", DataTypes.StringType, true),
                DataTypes.createStructField("userPlayTime", DataTypes.StringType, true)
        ));
    }

    @Override
    public StructType bufferSchema() {

        return DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("videoPlayModuleTime", DataTypes.StringType, true)
        ));
    }

    @Override
    public DataType dataType() {

        return DataTypes.StringType;
    }

    @Override
    public boolean deterministic() {

        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {

        buffer.update(0, "");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

        String videoPlayModuleTime = buffer.getString(0);

        String videoId = input.getString(0);
        Long playTime = Long.parseLong(input.getString(1));
        Long userPlayTime = Long.parseLong(input.getString(2));

        String str = "videoId=" + videoId + "|playTime=" + playTime + "|userPlayTime=" + userPlayTime + "&&";
        buffer.update(0, videoPlayModuleTime + str);
    }

    @Override
    public void merge(MutableAggregationBuffer merge, Row row) {


        String videoPlayModuleTimeMerge = merge.getString(0);
        String videoPlayModuleTimeRow = row.getString(0);

        merge.update(0, videoPlayModuleTimeMerge + videoPlayModuleTimeRow);
    }

    @Override
    public Object evaluate(Row buffer) {

        return buffer.getString(0);
    }
}
