package cm.storm.g4jk.Bolts;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

//import cm.storm.g4jk.Beans.Yunguan_G4JK_Basic4GBean;
import cm.storm.g4jk.Beans.Yunguan_G4JK_Basic4GFields;
import cm.storm.g4jk.Commons.RedisServer;

/**
 * 实时分析网分记录IMSI对应的号码，以及号码可以参与的SJJSXXX活动，符合规则则添加SJJSid到对应的key中，并且将号码丢入触点集合中。
 * @author chinamobile
 * 20170314
 */
public class Yunguan_G4JK_SJJSToRedis extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = 4438803730362084161L;

	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_SJJSToRedis.class);
	
	//元组发射搜集器
	private OutputCollector collector;

	//获取redis连接
	private RedisServer redisserver;
	
	//初始化bolt元组搜集器，用于存放需要发射元组
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	//初始方案：先通过url进行检索，获取对应的中文字与维表上的中文字直接匹配
	@Override
	public void execute(Tuple tuple) {
		//redis操作
		redisserver=RedisServer.getInstance();
		String tdate=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.STARTTIME);
		String imsi=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.IMSI);
		String url=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.URL);
		String appid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTAPPID);
		String intsid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTSID);
		Set<String> sjjslist=null;
		String words=null;
		String key=null;
		String phnum=null;
		String appvalue=null;
		String intvalue=null;
		String chinesewords=null;
		String[] params=null;
		String[] keywords=null;
		String testwords=null;
		boolean flag=false;

		if(tdate.length()>=23&&imsi!=null&&imsi.length()>=15){
			tdate=tdate.substring(0,10);	//获取日期YYYY-MM-DD
			key="ref_imsiphn_"+imsi;
			phnum=redisserver.get(key);
			if(phnum!=null&&phnum.length()>=11){
				//业务：对应触点的SJJS列表
				key="ref_sjjsparams_set";
				sjjslist=redisserver.smembers(key);
				if(sjjslist!=null&&sjjslist.size()>0)
				{
					for(String sjjs : sjjslist){
						flag=false;
						testwords="";
						intvalue=null;
						appvalue=null;
						chinesewords=null;
						key="ref_sjjsparams_"+sjjs;
						words=redisserver.get(key);	//例子--上网行为类型:天猫#淘宝#支付宝#京东;上网搜索热词:家宽#宽带#极光#电信;
						if(words!=null){
							params=words.split(";");
							if(params!=null){
								for(int i=0;i<params.length;i++){
									keywords=null;
									words=params[i].trim();
									if(words!=null&&words.contains(":")==true)keywords=words.split(":");
									//必须是变量名称和值组成，长度必须为2
									if(keywords!=null&&keywords.length==2)testwords+=keywords[1].trim()+"#";
								}
							}
							if(testwords.length()>1)keywords=testwords.split("#");
							if(intsid!=null&&intsid.trim().equals("")==false&&intsid.trim().equals("none")==false){
								key="ref_wtag_"+intsid;
								intvalue=redisserver.get(key);//获取对应的用户业务标签信息
							}
							if(appid!=null&&appid.trim().equals("")==false&&appid.trim().equals("none")==false){
								key="ref_wtag_"+appid;
								appvalue=redisserver.get(key);//获取对应的用户标签信息
							}
							chinesewords=getChineseWordsFromUrl(url);
						}
						for(int j=0;j<keywords.length;j++){
							if(keywords[j].equals("")==false){
								if(intvalue!=null&&intvalue.contains(keywords[j])==true)flag=true;
								else if(appvalue!=null&&appvalue.contains(keywords[j])==true)flag=true;
								else if(chinesewords!=null&&chinesewords.contains(keywords[j])==true)flag=true;
							}
							if(flag==true)break;
						}
						if(flag==true){						//需要触点，再将号码放入当天的触点集合中
							key="mfg4_"+tdate+"_sjjs_"+phnum;
							redisserver.sadd(key, sjjs);
							key="mfg4_"+tdate+"_UnTouchSet";
							redisserver.sadd(key, phnum);
						}
					}
				}				
			}
		}
		
		//释放内存
		redisserver=null;
		tdate=null;
		imsi=null;
		intsid=null;
		phnum=null;
		words=null;
		sjjslist=null;
		appid=null;
		key=null;
		appvalue=null;
		intvalue=null;
		url=null;
		chinesewords=null;
		params=null;
		keywords=null;
		testwords=null;
		collector.ack(tuple);
	}

	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
	}
	
	//自定义方法区域
	/**
	 * 提取url中的中文
	 * @param url 网分数据中的url
	 * @return 返回中文字符串或者null
	 */
	public String getChineseWordsFromUrl(String url){
		String url_ch=null;
		//对url做转换操作，通用转换做法假定就是 GBK 的编码：
		//将其解码成字节码，然后再把字节码编码为GBK，
		//如果转换回来后与没有转换之前是相等的。
		//这样假设成立，也就是GBK编码。如果解析失败，则用utf8解码
		try {
			String reg = "[^\u4e00-\u9fa5]";   //^匹配所有非中文字符, \u4e00, \u9fa5代表是两个unicode编码值，他们正好是Unicode表中的汉字的头和尾
			String fis= null;
			String sec = null;
			//尝试解码3次，单次解码未必直接能够解析出中文
			for(int i=0;i<3;i++)
			{
				fis = java.net.URLDecoder.decode(url, "gb2312");
				sec = new String(fis.getBytes("gb2312"), "gb2312");
				if (fis.equals(sec)==true)
					url=fis;
		        else
		        	url= java.net.URLDecoder.decode(url, "utf-8");
			}
	
			//提取url中的中文
			url = url.replaceAll(reg, "");
			//记录中文信息
			if(url!=null&&url.length()>=2)url_ch=url;
		} catch (Exception ex) {
			//LOG.info("Yunguan_G4JK_TouchSjjsToRedis execute error: "+ex.getMessage());
			return null;
		}
		return url_ch;
	}
}



//业务app小类，上网业务小类id
//String appsid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTAPPID);
//String intsid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTSID);

//元组存储结构
//private Yunguan_G4JK_Basic4GBean g4jkbasic4gbean=null;
////拼接字段
//process_tuple(tuple);
////将元组继续往后台传输
//if(g4jkbasic4gbean!=null)
//{
//	//发射元组
//	collector.emit(new Values(
//		g4jkbasic4gbean.getStarttime(),
//		g4jkbasic4gbean.getImsi(),
//		g4jkbasic4gbean.getUrl(),
//		g4jkbasic4gbean.getImei(),
//		g4jkbasic4gbean.getTac(),
//		g4jkbasic4gbean.getCid(),
//		g4jkbasic4gbean.getEvent_type(),
//		g4jkbasic4gbean.getUl_data(),
//		g4jkbasic4gbean.getDl_data(),
//		g4jkbasic4gbean.getDelay(),
//		g4jkbasic4gbean.getUser_agent(),
//		g4jkbasic4gbean.getGmcc_bus_ind(),
//		g4jkbasic4gbean.getPhone_brand(),
//		g4jkbasic4gbean.getPhone_type(),
//		g4jkbasic4gbean.getApn(),
//		g4jkbasic4gbean.getPro_type(),
//		g4jkbasic4gbean.getUser_ip(),
//		g4jkbasic4gbean.getApp_server_ip(),
//		g4jkbasic4gbean.getUser_port(),
//		g4jkbasic4gbean.getApp_server_port(),
//		g4jkbasic4gbean.getApptype(),
//		g4jkbasic4gbean.getIntappid(),
//		g4jkbasic4gbean.getIntsid()
//	));
//}
//g4jkbasic4gbean=null;	
/**
 * @param tuple:从string中获取数据
 */
//public void process_tuple(Tuple tuple){
//	g4jkbasic4gbean=new Yunguan_G4JK_Basic4GBean();
//	//字段共23个，目前4个空值，19个可用
//	//字段1，获取日期并做格式转换
//	g4jkbasic4gbean.setStarttime(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.STARTTIME));
//	//字段2，获取IMSI
//	g4jkbasic4gbean.setImsi(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.IMSI));		
//	//字段3，获取url
//	g4jkbasic4gbean.setUrl(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.URL));		
//	//字段4，获取IMEI
//	g4jkbasic4gbean.setImei(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.IMEI));		
//	//字段5，获取基站TAC码，至少4位
//	g4jkbasic4gbean.setTac(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.TAC));		
//	//字段6，获取基站小区码cell_id，或者填写数字，或者填写none
//	g4jkbasic4gbean.setCid(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.CID));		
//	//字段7，事件类型
//	g4jkbasic4gbean.setEvent_type(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.EVENT_TYPE));
//	//字段8，上行流量
//	g4jkbasic4gbean.setUl_data(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.UL_DATA));
//	//字段9，下行流量
//	g4jkbasic4gbean.setDl_data(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.DL_DATA));		
//	//字段10，会话时长
//	g4jkbasic4gbean.setDelay(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.DELAY));	
//	//字段11，终端型号
//	g4jkbasic4gbean.setUser_agent(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.USER_AGENT));
//	//字段12，是否自有业务
//	g4jkbasic4gbean.setGmcc_bus_ind(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.GMCC_BUS_IND));
//	//字段13，手机品牌
//	g4jkbasic4gbean.setPhone_brand(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.PHONE_BRAND));
//	//字段14，手机型号
//	g4jkbasic4gbean.setPhone_type(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.PHONE_TYPE));
//	//字段15，接入点
//	g4jkbasic4gbean.setApn(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.APN));
//	//字段16，协议类型
//	g4jkbasic4gbean.setPro_type(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.PRO_TYPE));
//	//字段17，业务源ip
//	g4jkbasic4gbean.setUser_ip(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.USER_IP));
//	//字段18，业务目标ip
//	g4jkbasic4gbean.setApp_server_ip(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.APP_SERVER_IP));
//	//字段19，业务源端口
//	g4jkbasic4gbean.setUser_port(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.USER_PORT));
//	//字段20，业务目的端口
//	g4jkbasic4gbean.setApp_server_port(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.APP_SERVER_PORT));
//	//字段21，业务大类
//	g4jkbasic4gbean.setApptype(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.APPTYPE));
//	//字段22，app小类
//	g4jkbasic4gbean.setIntappid(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTAPPID));
//	//字段23，业务小类
//	g4jkbasic4gbean.setIntsid(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTSID));
//}
//outputFieldsDeclarer.declare(new Fields(
//		Yunguan_G4JK_Basic4GFields.STARTTIME,
//		Yunguan_G4JK_Basic4GFields.IMSI,
//		Yunguan_G4JK_Basic4GFields.URL,
//		Yunguan_G4JK_Basic4GFields.IMEI,
//		Yunguan_G4JK_Basic4GFields.TAC,
//		Yunguan_G4JK_Basic4GFields.CID,
//		Yunguan_G4JK_Basic4GFields.EVENT_TYPE,
//		Yunguan_G4JK_Basic4GFields.UL_DATA,
//		Yunguan_G4JK_Basic4GFields.DL_DATA,
//		Yunguan_G4JK_Basic4GFields.DELAY,
//		Yunguan_G4JK_Basic4GFields.USER_AGENT,
//		Yunguan_G4JK_Basic4GFields.GMCC_BUS_IND,
//		Yunguan_G4JK_Basic4GFields.PHONE_BRAND,
//		Yunguan_G4JK_Basic4GFields.PHONE_TYPE,
//		Yunguan_G4JK_Basic4GFields.APN,
//		Yunguan_G4JK_Basic4GFields.PRO_TYPE,
//		Yunguan_G4JK_Basic4GFields.USER_IP,
//		Yunguan_G4JK_Basic4GFields.APP_SERVER_IP,
//		Yunguan_G4JK_Basic4GFields.USER_PORT,
//		Yunguan_G4JK_Basic4GFields.APP_SERVER_PORT,
//		Yunguan_G4JK_Basic4GFields.APPTYPE,
//		Yunguan_G4JK_Basic4GFields.INTAPPID,
//		Yunguan_G4JK_Basic4GFields.INTSID
//	));
