package cm.storm.g4jk.Beans;

/*
 * 家宽网分数据的类结构定义，具体字段描述如下类别所描述，目前字段16个，可用14个 20160905
 */
public class Yunguan_G4JK_BasicJKBean {
	private String starttime="2000-01-01 00:00:00.000";		//家宽网分流记录的会话开始时间
	private String lasttime="2000-01-01 00:00:00.000";		//家宽网分流记录的会话结束时间
	private String user_name="none";									//用户账号
	private String user_type="none";									//用户类型（家宽、集客）
	private String uri="none";												//网站主域名
	private String app_type="none";										//URL一级分类，相当于业务大类，查看家宽维表
	private String app_sub_type="none";								//URL二级分类，相当于业务小类，查看家宽维表
	private String up_flux="0";												//上行流量 Byte
	private String down_flux="0";											//下行流量 Byte
    private String protocol_type="1";									//空字段，业务使用协议（get或post），流中默认空值代表HTTP协议，此处设置为1
	private String success="1";											//会话时长 success为1表示成功，为空表示失败
	private String user_ip="none";										//业务源ip
	private String dest_ip="none";										//业务目标ip
	private String src_port="none";										//空字段，源端口，默认也是为空
	private String dest_port="none";									//目标端口
	private String response_time="0";									//响应时间 ms
	public String getStarttime() {
		return starttime;
	}
	public void setStarttime(String starttime) {
		this.starttime = starttime;
	}
	public String getLasttime() {
		return lasttime;
	}
	public void setLasttime(String lasttime) {
		this.lasttime = lasttime;
	}
	public String getUser_name() {
		return user_name;
	}
	public void setUser_name(String user_name) {
		this.user_name = user_name;
	}
	public String getUser_type() {
		return user_type;
	}
	public void setUser_type(String user_type) {
		this.user_type = user_type;
	}
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public String getApp_type() {
		return app_type;
	}
	public void setApp_type(String app_type) {
		this.app_type = app_type;
	}
	public String getApp_sub_type() {
		return app_sub_type;
	}
	public void setApp_sub_type(String app_sub_type) {
		this.app_sub_type = app_sub_type;
	}
	public String getUp_flux() {
		return up_flux;
	}
	public void setUp_flux(String up_flux) {
		this.up_flux = up_flux;
	}
	public String getDown_flux() {
		return down_flux;
	}
	public void setDown_flux(String down_flux) {
		this.down_flux = down_flux;
	}
	public String getProtocol_type() {
		return protocol_type;
	}
	public void setProtocol_type(String protocol_type) {
		this.protocol_type = protocol_type;
	}
	public String getSuccess() {
		return success;
	}
	public void setSuccess(String success) {
		this.success = success;
	}
	public String getUser_ip() {
		return user_ip;
	}
	public void setUser_ip(String user_ip) {
		this.user_ip = user_ip;
	}
	public String getDest_ip() {
		return dest_ip;
	}
	public void setDest_ip(String dest_ip) {
		this.dest_ip = dest_ip;
	}
	public String getSrc_port() {
		return src_port;
	}
	public void setSrc_port(String src_port) {
		this.src_port = src_port;
	}
	public String getDest_port() {
		return dest_port;
	}
	public void setDest_port(String dest_port) {
		this.dest_port = dest_port;
	}
	public String getResponse_time() {
		return response_time;
	}
	public void setResponse_time(String response_time) {
		this.response_time = response_time;
	}
}
