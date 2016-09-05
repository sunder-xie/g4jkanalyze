package cm.storm.g4jk.Beans;

/*
 * 4G网分数据位置信息的类结构定义，具体字段描述如下类别所描述，目前字段4个 20160905
 */
public class Yunguan_G4JK_BasicTAUBean {
	private String ttime="2000-01-01 00:00:00.000"; 	//4G网分数据会话开始时间
	private String imsi="123456789012345";					//IMSI，15号
	private String tac="none";										//tac，基站TAC码
	private String ci="none";										//ci，小区号
	public String getTtime() {
		return ttime;
	}
	public void setTtime(String ttime) {
		this.ttime = ttime;
	}
	public String getImsi() {
		return imsi;
	}
	public void setImsi(String imsi) {
		this.imsi = imsi;
	}
	public String getTac() {
		return tac;
	}
	public void setTac(String tac) {
		this.tac = tac;
	}
	public String getCi() {
		return ci;
	}
	public void setCi(String ci) {
		this.ci = ci;
	}
	
}
