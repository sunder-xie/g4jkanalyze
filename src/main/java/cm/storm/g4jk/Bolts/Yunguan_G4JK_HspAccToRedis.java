package cm.storm.g4jk.Bolts;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
//import org.apache.storm.tuple.Values;
//import org.apache.storm.tuple.Fields;
//
//import cm.storm.g4jk.Beans.Yunguan_G4JK_Basic4GBean;
import cm.storm.g4jk.Beans.Yunguan_G4JK_Basic4GFields;
import cm.storm.g4jk.Commons.RedisServer;

/**
 * 每15分钟统计热点区域的人流量(区别imsi)，同时统计当天app应用的热度信息
 * @author chinamobile
 * 20160907
 */
public class Yunguan_G4JK_HspAccToRedis extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = 4610478279647936193L;
	
	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_HspAccToRedis.class);
	
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
	
	//按照4G网分数据tac,ci对应的hotspot的imsi汇总，流量的汇总，15分钟产生一次，记录数量千级别
	@Override
	public void execute(Tuple tuple) {
		//redis操作
		redisserver=RedisServer.getInstance();
		String tdate=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.STARTTIME);
		String imsi=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.IMSI);
		String tac=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.TAC);
		String ci=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.CID);
		String intappid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTAPPID);
		Set<String> hotspotlist=null;
		String hour=null;
		String minute=null;
		String imsi_catch_time=null;
		String imsi_tdate1="19000101000000";
		String imsi_tdate2="19000101000000";
		String appdate=tdate;
		String appvalue=null;
		int clk=0;
		String key=null;
//		String value=null;
//		long rt=0;

		if(tdate.length()>=23&&imsi.length()>=15){
			//查询维表获取热点区域标签，一个tac，ci可能因为项目不同被归属在不同的项目热点区域之下
			key="ref_hsp_"+tac+"_"+ci;
			hotspotlist=redisserver.smembers(key);

			if(hotspotlist!=null&&hotspotlist.size()>0)
			{
				hour=tdate.substring(11,13);
				minute=tdate.substring(14,16);
				imsi_catch_time=tdate.substring(0,4)+tdate.substring(5,7)+tdate.substring(8,10)+hour+minute+tdate.substring(17,19);
				clk=Integer.valueOf(minute); 	//会自动过滤数字前边的0
				tdate=tdate.substring(0,10);
				if(clk>=0&&clk<15)minute="00";
				else if(clk>=15&&clk<30)minute="15";
				else if(clk>=30&&clk<45)minute="30";
				else if(clk>=45)minute="45";
				
				for(String hotspot : hotspotlist){			
					//标记hotspot捕获imsi的时间
					key="mfg4_"+tdate+"_hspimsi_"+hotspot+"_"+imsi;
					imsi_tdate1=redisserver.get(key);
					if(imsi_tdate1==null||imsi_tdate1.equals("nil"))imsi_tdate1=imsi_catch_time+";"+imsi_catch_time;
					else if (imsi_tdate1.length()>=29){
						imsi_tdate2=imsi_tdate1.substring(15);
						imsi_tdate1=imsi_tdate1.substring(0,14);
						if(imsi_catch_time.compareTo(imsi_tdate1)<0)imsi_tdate1=imsi_catch_time+";"+imsi_tdate2;
						else if(imsi_catch_time.compareTo(imsi_tdate2)>0)imsi_tdate1=imsi_tdate1+";"+imsi_catch_time;
						else imsi_tdate1=imsi_tdate1+";"+imsi_tdate2;
					}
					redisserver.set(key, imsi_tdate1);
					
					//将imsi累计到热点区域中,以15分钟为维度进行创建
					key="mfg4_"+tdate+"_hspset_"+hotspot+"_"+hour+"_"+minute;
					redisserver.sadd(key, imsi);
//					key="mfg4_"+tdate+"_imsihot_"+hour+"_"+minute+"_"+imsi;
//					value=hotspot;
//					rt=redisserver.sadd(key,value);
//					if(rt>0){
//						key="mfg4_"+tdate+"_hspset_"+hotspot+"_"+hour+"_"+minute;	
//						redisserver.incr(key);
//					}
				}
			}
			
			//统计appid的使用集合，每个appid的使用热度
			if(intappid!=null&&intappid.trim().equals("")==false&&intappid.trim().equals("none")==false){
				appdate=appdate.substring(0,10);	//获取日期
				key="ref_wtag_"+intappid;
				appvalue=redisserver.get(key);
				//应用的维表中存在翻译信息则进行数据累加，不对这两大类做统计
				if(appvalue!=null&&appvalue.length()>0&&appvalue.contains("浏览器")==false&&appvalue.contains("其他")==false){
					key="mfg4_"+appdate+"_AppidSet";
					redisserver.sadd(key, intappid);
					
					key="mfg4_"+appdate+"_AppUse_"+intappid;
					redisserver.incr(key); 	//累计当天的访问次数
				}
			}
		}

		//释放内存
		redisserver=null;
		tdate=null;
		imsi=null;
		tac=null;
		ci=null;
		hotspotlist=null;
		hour=null;
		minute=null;
		clk=0;
		key=null;
//		value=null;
//		rt=0;
		imsi_catch_time=null;
		imsi_tdate1=null;
		imsi_tdate2=null;
		appdate=null;
		appvalue=null;
		collector.ack(tuple);
	}
	
	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
	}

}

//将标签产生的流量值累计到对应的标签中，2016年10月8日，未使用暂停
//将热点区域产生的流量值累计到热点区域对应的标签中
//String dlflux=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.DL_DATA);
//String ulflux=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.UL_DATA);
//double g4flux=0;
//key="mfg4_"+tdate+"_hspflux_"+hotspot+"_"+hour+"_"+minute;
//g4flux=(Double.valueOf(dlflux)+Double.valueOf(ulflux))/1048576; //单位由Byte转为MB
//redisserver.incrbyfloat(key, g4flux);
//dlflux=null;
//ulflux=null;
//g4flux=0;


//元组存储结构
//private Yunguan_G4JK_Basic4GBean g4jkbasic4gbean=null;

//g4jkbasic4gbean=new Yunguan_G4JK_Basic4GBean();
//if(g4jkbasic4gbean!=null)
//{
//	g4jkbasic4gbean.setStarttime(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.STARTTIME));
//	g4jkbasic4gbean.setImsi(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.IMSI));
//	g4jkbasic4gbean.setUrl(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.URL));
//	g4jkbasic4gbean.setImei(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.IMEI));
//	g4jkbasic4gbean.setTac(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.TAC));
//	g4jkbasic4gbean.setCid(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.CID));
//	g4jkbasic4gbean.setEvent_type(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.EVENT_TYPE));
//	g4jkbasic4gbean.setUl_data(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.UL_DATA));
//	g4jkbasic4gbean.setDl_data(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.DL_DATA));
//	g4jkbasic4gbean.setDelay(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.DELAY));
//	g4jkbasic4gbean.setUser_agent(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.USER_AGENT));
//	g4jkbasic4gbean.setGmcc_bus_ind(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.GMCC_BUS_IND));
//	g4jkbasic4gbean.setPhone_brand(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.PHONE_BRAND));
//	g4jkbasic4gbean.setPhone_type(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.PHONE_TYPE));
//	g4jkbasic4gbean.setApn(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.APN));
//	g4jkbasic4gbean.setPro_type(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.PRO_TYPE));
//	g4jkbasic4gbean.setUser_ip(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.USER_IP));
//	g4jkbasic4gbean.setApp_server_ip(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.APP_SERVER_IP));
//	g4jkbasic4gbean.setUser_port(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.USER_PORT));
//	g4jkbasic4gbean.setApp_server_port(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.APP_SERVER_PORT));
//	g4jkbasic4gbean.setApptype(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.APPTYPE));
//	g4jkbasic4gbean.setIntappid(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTAPPID));
//	g4jkbasic4gbean.setIntsid(tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTSID));
//
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

//outputFieldsDeclarer.declare(new Fields(
//Yunguan_G4JK_Basic4GFields.STARTTIME,
//Yunguan_G4JK_Basic4GFields.IMSI,
//Yunguan_G4JK_Basic4GFields.URL,
//Yunguan_G4JK_Basic4GFields.IMEI,
//Yunguan_G4JK_Basic4GFields.TAC,
//Yunguan_G4JK_Basic4GFields.CID,
//Yunguan_G4JK_Basic4GFields.EVENT_TYPE,
//Yunguan_G4JK_Basic4GFields.UL_DATA,
//Yunguan_G4JK_Basic4GFields.DL_DATA,
//Yunguan_G4JK_Basic4GFields.DELAY,
//Yunguan_G4JK_Basic4GFields.USER_AGENT,
//Yunguan_G4JK_Basic4GFields.GMCC_BUS_IND,
//Yunguan_G4JK_Basic4GFields.PHONE_BRAND,
//Yunguan_G4JK_Basic4GFields.PHONE_TYPE,
//Yunguan_G4JK_Basic4GFields.APN,
//Yunguan_G4JK_Basic4GFields.PRO_TYPE,
//Yunguan_G4JK_Basic4GFields.USER_IP,
//Yunguan_G4JK_Basic4GFields.APP_SERVER_IP,
//Yunguan_G4JK_Basic4GFields.USER_PORT,
//Yunguan_G4JK_Basic4GFields.APP_SERVER_PORT,
//Yunguan_G4JK_Basic4GFields.APPTYPE,
//Yunguan_G4JK_Basic4GFields.INTAPPID,
//Yunguan_G4JK_Basic4GFields.INTSID
//));


//key="mfg4_"+tdate+"_hspflux_"+hour+"_"+minute+"_"+hotspot+"_"+tag;
//g4flux=(Double.valueOf(dlflux)+Double.valueOf(ulflux))/1048576; //单位由Byte转为MB
////将标签产生的流量值累计到热点区域对应的标签中
//redisserver.incrbyfloat(key, g4flux);

//key="mfg4_"+tdate+"_hspwtagflux_"+hour+"_"+minute+"_"+hotspot+"_"+apptag;
//g4flux=(Double.valueOf(dlflux)+Double.valueOf(ulflux))/1048576; //单位由Byte转为MB
////将标签产生的流量值累计到热点区域对应的标签中
//redisserver.incrbyfloat(key, g4flux);

//if(tag!=null&&tag.equals("nil")==false)
//{
//	//将imsi累计到热点区域对应的标签中,以15分钟为维度进行创建
//	key="mfg4_"+tdate+"_hspset_"+hour+"_"+minute+"_"+hotspot+"_"+tag;
//	redisserver.sadd(key, imsi);
//}

//用户上网标签人数统计，流量统计，测试代码
//if(apptag!=null&&apptag.equals("nil")==false)
//{
//	//将imsi累计到热点区域对应的app标签中，累计1天
//	key="mfg4_"+tdate+"_hspwtagset_"+hour+"_"+minute+"_"+hotspot+"_"+apptag; 
//	redisserver.sadd(key, imsi);
//}

//key="mfg4_"+tdate+"_hspdayset_"+hotspot;	//记录每天对应的hostspot中的imsi明细
//redisserver.sadd(key, imsi);

//标记hotspot捕获imsi的时间
//key="mfg4_"+tdate+"_"+imsi+"_"+hotspot;

//将imsi累计到热点区域中,以15分钟为维度进行创建
//key="mfg4_"+tdate+"_hspset_"+hour+"_"+minute+"_"+hotspot;
//redisserver.sadd(key, imsi);

//获取上网标签行为
//String apptype=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.APPTYPE);
//String intappid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTAPPID);
//apptype=null;
//intappid=null;

//String tag=null;
//String apptag=null;
////查询维表获取维表标记的imsi标签
//key="ref_tags_"+imsi;
//tag=redisserver.get(key);
//
////查询维表获取上网大类标签
//key="ref_wtag_"+apptype+"_"+intappid;
//apptag=redisserver.get(key);
//apptag=null;
//tag=null;
