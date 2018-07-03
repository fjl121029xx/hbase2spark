package com.li.hbase2spark.udaf;

import com.li.hbase2spark.constants.VideoPlayConstant;
import com.li.hbase2spark.utils.ValueUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class VideoPlayTimeUDAF extends UserDefinedAggregateFunction {
    @Override
    public StructType inputSchema() {

        return DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("playTime", DataTypes.StringType, true),
                DataTypes.createStructField("userPlayTime", DataTypes.StringType, true)
        ));
    }

    @Override
    public StructType bufferSchema() {

        return DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("videoPlayTimeUDAF", DataTypes.StringType, true)
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

    //username=000|totalVideo=0|playTime=0000|userPlayTime=00000
    @Override
    public void initialize(MutableAggregationBuffer buffer) {

        buffer.update(0, "username=000|totalVideo=0|playTime=0000|userPlayTime=00000");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

        String videoPlayTime = buffer.getString(0);
        String username = ValueUtil.parseStr2Str(videoPlayTime, VideoPlayConstant.SSTREAM_VIDEO_PLAY_FIELD_USERNAME);
        Long totalVideo = ValueUtil.parseStr2Long(videoPlayTime, VideoPlayConstant.SSTREAM_VIDEO_PLAY_FIELD_TOTALVIDEO);
        Long playTime = ValueUtil.parseStr2Long(videoPlayTime, VideoPlayConstant.SSTREAM_VIDEO_PLAY_FIELD_PLAYTIME);
        Long userPlayTime = ValueUtil.parseStr2Long(videoPlayTime, VideoPlayConstant.SSTREAM_VIDEO_PLAY_FIELD_USERPLAYTIME);


        String usernameRow = input.getString(0);
        Long playTimeRow = Long.parseLong(input.getString(1));
        Long userPlayTimeRow = Long.parseLong(input.getString(2));

        totalVideo++;

        if (username.equals("000")) {
            username = usernameRow;
        }

        playTime += playTimeRow;
        userPlayTime += userPlayTimeRow;

        String str = "username=" + usernameRow + "|totalVideo=" + totalVideo + "|playTime=" + playTime + "|userPlayTime=" + userPlayTime + "";
        buffer.update(0, str);
    }

    @Override
    public void merge(MutableAggregationBuffer merge, Row row) {
        String videoPlayTimeMerge = merge.getString(0);
        String usernameMerge = ValueUtil.parseStr2Str(videoPlayTimeMerge, VideoPlayConstant.SSTREAM_VIDEO_PLAY_FIELD_USERNAME);
        Long totalVideoMerge = ValueUtil.parseStr2Long(videoPlayTimeMerge, VideoPlayConstant.SSTREAM_VIDEO_PLAY_FIELD_TOTALVIDEO);
        Long playTimeMerge = ValueUtil.parseStr2Long(videoPlayTimeMerge, VideoPlayConstant.SSTREAM_VIDEO_PLAY_FIELD_PLAYTIME);
        Long userPlayTimeMerge = ValueUtil.parseStr2Long(videoPlayTimeMerge, VideoPlayConstant.SSTREAM_VIDEO_PLAY_FIELD_USERPLAYTIME);


        String videoPlayTimeRow = row.getString(0);
        String usernameRow = ValueUtil.parseStr2Str(videoPlayTimeRow, VideoPlayConstant.SSTREAM_VIDEO_PLAY_FIELD_USERNAME);
        Long totalVideoRow = ValueUtil.parseStr2Long(videoPlayTimeRow, VideoPlayConstant.SSTREAM_VIDEO_PLAY_FIELD_TOTALVIDEO);
        Long playTimeRow = ValueUtil.parseStr2Long(videoPlayTimeRow, VideoPlayConstant.SSTREAM_VIDEO_PLAY_FIELD_PLAYTIME);
        Long userPlayTimeRow = ValueUtil.parseStr2Long(videoPlayTimeRow, VideoPlayConstant.SSTREAM_VIDEO_PLAY_FIELD_USERPLAYTIME);

        totalVideoMerge += totalVideoRow;
        playTimeMerge += playTimeRow;
        userPlayTimeMerge += userPlayTimeRow;

        String str = "username=" + usernameRow + "|totalVideo=" + totalVideoMerge + "|playTime=" + playTimeMerge + "|userPlayTime=" + userPlayTimeMerge + "";
        merge.update(0, str);
    }

    @Override
    public Object evaluate(Row buffer) {

        return buffer.getString(0);
    }
}
