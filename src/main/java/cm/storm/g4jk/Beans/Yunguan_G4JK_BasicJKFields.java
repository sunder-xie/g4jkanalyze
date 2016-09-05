package cm.storm.g4jk.Beans;

/*
 * 对应家宽网分数据的字段属性名称，标记流bolt发射流的字段名称，一共有用的有14个
 */
public class Yunguan_G4JK_BasicJKFields {
	public static final String STARTTIME="starttime";						//家宽网分流记录的会话开始时间
	public static final String LASTTIME="lasttime";							//家宽网分流记录的会话结束时间
	public static final String USER_NAME="user_name";					//用户账号
	public static final String USER_TYPE="user_type";						//用户类型（家宽、集客）
	public static final String URI="uri";											//网站主域名
	public static final String APP_TYPE="app_type";							//URL一级分类，相当于业务大类，查看家宽维表
	public static final String APP_SUB_TYPE="app_sub_type";			//URL二级分类，相当于业务小类，查看家宽维表
	public static final String UP_FLUX="up_flux";								//上行流量 Byte
	public static final String DOWN_FLUX="down_flux";					//下行流量 Byte
    //空字段，业务使用协议（get或post），空值代表HTTP协议
	public static final String SUCCESS="success";								//会话时长 success为1表示成功，为空表示失败
	public static final String USER_IP="user_ip";								//业务源ip
	public static final String DEST_IP="dest_ip";								//业务目标ip
	//空字段，源端口为空
	public static final String DEST_PORT="dest_port";						//目标端口
	public static final String RESPONSE_TIME="response_time";		//响应时间，单位是ms
}
