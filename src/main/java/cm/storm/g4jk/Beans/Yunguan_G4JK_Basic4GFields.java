package cm.storm.g4jk.Beans;

/*
 * 对应4G网分数据的字段属性名称，标记流bolt发射流的字段名称，一共有用的有23个
 */
public class Yunguan_G4JK_Basic4GFields {
		public static final String STARTTIME="starttime";							//系统获取4G网分流记录的时间
		public static final String IMSI="imsi";												//IMSI 15位，识别手机卡
		public static final String URL="url";												//url相关路径信息
		public static final String IMEI="imei";												//imei 15位，识别手机
		public static final String TAC="tac";												//基站TAC码
		public static final String CID="cid";												//4G小区号
		//空字段，事件类型（目前无法区分get/post流程）
		public static final String UL_DATA="ul_data";									//上行流量 Byte
		public static final String DL_DATA="dl_data";									//下行流量 Byte
		public static final String DELAY="delay";										//会话时长 ms
		public static final String USER_AGENT="user_agent"; 					//终端型号
	    //空字段，是否为自有业务
	    //空字段，手机品牌
	    //空字段，手机型号
		public static final String APN="apn";												//接入点
		public static final String PRO_TYPE="pro_type";								//协议类型，默认都为1
		public static final String USER_IP="user_ip";									//业务源ip
		public static final String APP_SERVER_IP="app_server_ip";			//业务目标ip
		public static final String USER_PORT="user_port"; 						//源端口
		public static final String APP_SERVER_PORT="app_server_port";	//目标端口
		public static final String APPTYPE="apptype";								//业务大类，查看4G业务大类维表
		public static final String INTAPPID="intappid";								//app小类，查看4G业务小类维表						 
		public static final String INTSID="intsid";										//业务小类，查看4G业务小类维表									
}
