package com.li.hbase2spark.spark;

import com.li.hbase2spark.bean.VideoPlayBean;
import com.li.hbase2spark.udaf.VideoPlayModuleTimeUDAF;
import com.li.hbase2spark.udaf.VideoPlayTimeUDAF;
import com.li.hbase2spark.utils.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;

public class Hbase2SparkApplication {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Hbase2SparkApplication");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.100.2,192.168.100.3,192.168.100.4");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.rootdir", "/hbase");
        conf.set(TableInputFormat.INPUT_TABLE, "videoplay");

        JavaPairRDD<ImmutableBytesWritable, Result> videoplayRdd = sc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        long l = videoplayRdd.count();
        System.out.println("videoplay Rdd Count : " + l);

        JavaRDD<Row> videoPlayRow = videoplayRdd.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {

            @Override
            public Row call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {

                Result result = t._2;
                String rowkey = new String(result.getRow());

                String[] id2Video = rowkey.split("-");
                String username;
                String videoId;
                if (rowkey.contains("-")) {

                    username = rowkey.substring(0, rowkey.indexOf("-"));
                    videoId = rowkey.substring(rowkey.indexOf("-") + 1, rowkey.length());
                } else {
                    username = rowkey;
                    videoId = "meiyoushipina";
                }


                String cv = Bytes.toString(result.getValue(Bytes.toBytes("playinfo"), Bytes.toBytes("cv")));
                String playTime = Bytes.toString(result.getValue(Bytes.toBytes("playinfo"), Bytes.toBytes("playTime")));
                String terminal = Bytes.toString(result.getValue(Bytes.toBytes("playinfo"), Bytes.toBytes("terminal")));
                String userPlayTime = Bytes.toString(result.getValue(Bytes.toBytes("playinfo"), Bytes.toBytes("userPlayTime")));
                String wholeTime = Bytes.toString(result.getValue(Bytes.toBytes("playinfo"), Bytes.toBytes("wholeTime")));


                return RowFactory.create(username, videoId, cv, playTime, terminal, userPlayTime, wholeTime);
            }
        });

        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("videoId", DataTypes.StringType, true),
                DataTypes.createStructField("cv", DataTypes.StringType, true),
                DataTypes.createStructField("playTime", DataTypes.StringType, true),
                DataTypes.createStructField("terminal", DataTypes.StringType, true),
                DataTypes.createStructField("userPlayTime", DataTypes.StringType, true),
                DataTypes.createStructField("wholeTime", DataTypes.StringType, true)
        ));

        Dataset<Row> videoPlayDF = sqlContext.createDataFrame(videoPlayRow, schema);
        sqlContext.udf().register("videoPlayTime", new VideoPlayTimeUDAF());
        sqlContext.udf().register("videoPlayModuleTime", new VideoPlayModuleTimeUDAF());
        videoPlayDF.createOrReplaceTempView("tb_video_play_time");

        Dataset<Row> playInfo = sqlContext.sql("" +
                "select username," +
                "videoPlayTime(username,playTime,userPlayTime) as videoPlayTime," +
                "videoPlayModuleTime(videoId,playTime,userPlayTime) as videoPlayModuleTime" +
                " from tb_video_play_time group by username ");

        playInfo.show();

        playInfo.toJavaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {

                String username = row.getString(0);
                String videoPlayTime = row.getString(1);
                String videoPlayModuleTime = row.getString(2);

                VideoPlayBean vb = new VideoPlayBean();

                vb.setUsername(username);
                vb.setVideoPlayTime(videoPlayTime);
                vb.setVideoModulePlayTime(videoPlayModuleTime);

                HBaseUtil.put2hbase(VideoPlayBean.TEST_HBASE_TABLE, vb);

            }
        });

        sc.close();
    }

}
