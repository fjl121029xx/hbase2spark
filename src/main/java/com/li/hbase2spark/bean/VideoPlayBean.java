package com.li.hbase2spark.bean;


import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
public class VideoPlayBean {

    public static final String TEST_HBASE_TABLE = "test_tr_videoplaytime";
    public static final String HBASE_TABLE = "tr_videoplaytime";

    /**
     * username=000|totalVideo=0|playTime=0000|userPlayTime=00000
     */
    public static final String HBASE_TABLE_FAMILY_COLUMNS = "playinfo";

    public static final String HBASE_TABLE_COLUMN_VIDEOPLAYTIME = "videoPlayTime";
    /**h
     *  videoId=000|playTime=000|userPlayTime=000&&
     */
    public static final String HBASE_TABLE_FAMILY_COLUMNS2 = "module_playinfo";

    public static final String HBASE_TABLE_COLUMN_VIDEOMODULEPLAYTIME = "videoModulePlayTime";



    private String username;

    /**
     * 视频id 三中格式
     *       展示：gensee-22ed5e89fc6243bf8fd06a99503ba125
     *       百家云:
     *          录播课：
     *              有老师:videoIdWithTeacher-11239581
     *              无老师:videoIdWithoutTeacher-11239581
     *          直播回放：(roomId不为空,长期房间的话sessionId不为空)
     *              roomId-18060571125512-sessionId-201806070
     */
    private String videoId;

    private String teacher;

    private String cv;

    private String playTime;

    private String terminal;

    private String userPlayTime;

    private String wholeTime;

    /**
     * 录播课: 1  直播回放:2
     */
    private String type;


    //展示
    //htwxm_2996766 用户名    gensee-22ed5e89fc6243bf8fd06a99503ba125 joincode|cv=6.3|playTime=1261|terminal=1|userPlayTime904|wholeTime=8655


    //百家云
    //htwxm_2944167 用户名-    videoIdWithTeacher-11239581 视频id
    //htwxm_2948920 用户名     roomId-18060571125512-sessionId-201806070 区别长期短期
    /**
     *
     */
    private String videoPlayTime;

    /**
     *
     */
    private String videoModulePlayTime;
}
