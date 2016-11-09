package cm.storm.g4jk.Beans;

/*
 * 4G网分数据的类结构定义，具体字段描述如下类别所描述，目前字段23个，有用的字段19个 20160905
 */
public class Yunguan_G4JK_Basic4GBean {
	private String starttime="2000-01-01 00:00:00.000";	//系统获取4G网分流记录的时间
	private String imsi="123456789012345"; 					//IMSI 15位，识别手机卡
	private String url="none";											//url相关路径信息
	private String imei="123456789012345";						//imei 15位，识别手机
	private String tac="none";											//基站TAC码
	private String cid="none";											//4G小区号
	private String event_type="none";								//空字段，事件类型（目前无法区分get/post流程）
	private String ul_data="0";											//上行流量 Byte
	private String dl_data="0";											//下行流量 Byte
	private String delay="0";											//会话时长 ms 
    private String user_agent="none";								//终端型号
    private String gmcc_bus_ind="none";							//空字段，是否为自有业务
    private String phone_brand="none";							//空字段，手机品牌
    private String phone_type="none";								//空字段，手机型号
	private String apn="none";											//接入点
    private String pro_type="1";									    //协议类型，默认都为1
    private String user_ip="none";									//业务源ip
    private String app_server_ip="none";							//业务目标ip
    private String user_port="none";								//源端口
    private String app_server_port="none";						//目标端口
    private String apptype="none";									//业务大类
    private String intappid="none";									//app小类
    private String intsid="none";										//业务小类
    private String host="none";										//域名-20161109网分数据新加入字段
	public String getStarttime() {
		return starttime;
	}
	public void setStarttime(String starttime) {
		this.starttime = starttime;
	}
	public String getImsi() {
		return imsi;
	}
	public void setImsi(String imsi) {
		this.imsi = imsi;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getImei() {
		return imei;
	}
	public void setImei(String imei) {
		this.imei = imei;
	}
	public String getTac() {
		return tac;
	}
	public void setTac(String tac) {
		this.tac = tac;
	}
	public String getCid() {
		return cid;
	}
	public void setCid(String cid) {
		this.cid = cid;
	}
	public String getEvent_type() {
		return event_type;
	}
	public void setEvent_type(String event_type) {
		this.event_type = event_type;
	}
	public String getUl_data() {
		return ul_data;
	}
	public void setUl_data(String ul_data) {
		this.ul_data = ul_data;
	}
	public String getDl_data() {
		return dl_data;
	}
	public void setDl_data(String dl_data) {
		this.dl_data = dl_data;
	}
	public String getDelay() {
		return delay;
	}
	public void setDelay(String delay) {
		this.delay = delay;
	}
	public String getUser_agent() {
		return user_agent;
	}
	public void setUser_agent(String user_agent) {
		this.user_agent = user_agent;
	}
	public String getGmcc_bus_ind() {
		return gmcc_bus_ind;
	}
	public void setGmcc_bus_ind(String gmcc_bus_ind) {
		this.gmcc_bus_ind = gmcc_bus_ind;
	}
	public String getPhone_brand() {
		return phone_brand;
	}
	public void setPhone_brand(String phone_brand) {
		this.phone_brand = phone_brand;
	}
	public String getPhone_type() {
		return phone_type;
	}
	public void setPhone_type(String phone_type) {
		this.phone_type = phone_type;
	}
	public String getApn() {
		return apn;
	}
	public void setApn(String apn) {
		this.apn = apn;
	}
	public String getPro_type() {
		return pro_type;
	}
	public void setPro_type(String pro_type) {
		this.pro_type = pro_type;
	}
	public String getUser_ip() {
		return user_ip;
	}
	public void setUser_ip(String user_ip) {
		this.user_ip = user_ip;
	}
	public String getApp_server_ip() {
		return app_server_ip;
	}
	public void setApp_server_ip(String app_server_ip) {
		this.app_server_ip = app_server_ip;
	}
	public String getUser_port() {
		return user_port;
	}
	public void setUser_port(String user_port) {
		this.user_port = user_port;
	}
	public String getApp_server_port() {
		return app_server_port;
	}
	public void setApp_server_port(String app_server_port) {
		this.app_server_port = app_server_port;
	}
	public String getApptype() {
		return apptype;
	}
	public void setApptype(String apptype) {
		this.apptype = apptype;
	}
	public String getIntappid() {
		return intappid;
	}
	public void setIntappid(String intappid) {
		this.intappid = intappid;
	}
	public String getIntsid() {
		return intsid;
	}
	public void setIntsid(String intsid) {
		this.intsid = intsid;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
    
}
