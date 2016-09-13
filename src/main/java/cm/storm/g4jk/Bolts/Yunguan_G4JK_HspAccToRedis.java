package cm.storm.g4jk.Bolts;

import java.util.Map;

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
 * 每15分钟统计热点区域的人流量(区别imsi)，4G http流量使用量
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
		String dlflux=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.DL_DATA);
		String ulflux=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.UL_DATA);
		String tac=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.TAC);
		String ci=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.CID);
		String hotspot=null;
		String hour=null;
		String minute=null;
		String tag=null;
		int clk1=0,clk2=0;
		String key=null;
		double g4flux=0;
		if(tdate.length()>=23&&imsi.length()>=15){
			key="ref_hsp_"+tac+"_"+ci;
			//查询维表获取标签
			hotspot=redisserver.get(key);
			
			key="ref_tags_"+imsi;
			//查询维表获取标签
			tag=redisserver.get(key);

			if(hotspot!=null&&hotspot.equals("nil")==false)
			{
				hour=tdate.substring(11,13);
				minute=tdate.substring(14,16);
				clk1=Integer.valueOf(hour); 	//会自动过滤数字前边的0
				clk2=Integer.valueOf(minute); 	//会自动过滤数字前边的0
				tdate=tdate.substring(0,10);
				if(clk2>=0&&clk2<15)minute="15";
				else if(clk2>=15&&clk2<30)minute="30";
				else if(clk2>=30&&clk2<45)minute="45";
				else if(clk2>=45){
					clk1+=1;
					hour=String.format("%02d", clk1);
					minute="00";
				}
				
				key="mfg4_"+tdate+"_hspdayset_"+hotspot;	//记录每天对应的hostspot中的imsi明细
				redisserver.sadd(key, imsi);
				
				key="mfg4_"+tdate+"_hspset_"+hour+"_"+minute+"_"+hotspot;
				//将imsi累计到热点区域中,以15分钟为维度进行创建
				redisserver.sadd(key, imsi);
				
				key="mfg4_"+tdate+"_hspflux_"+hour+"_"+minute+"_"+hotspot;
				g4flux=(Double.valueOf(dlflux)+Double.valueOf(ulflux))/1048576; //单位由Byte转为MB
				//将热点区域产生的流量值累计到热点区域对应的标签中
				redisserver.incrbyfloat(key, g4flux);
				
				if(tag!=null&&tag.equals("nil")==false)
				{
					key="mfg4_"+tdate+"_hspset_"+hour+"_"+minute+"_"+hotspot+"_"+tag;
					//将imsi累计到热点区域对应的标签中,以15分钟为维度进行创建
					redisserver.sadd(key, imsi);
					
					key="mfg4_"+tdate+"_hspflux_"+hour+"_"+minute+"_"+hotspot+"_"+tag;
					g4flux=(Double.valueOf(dlflux)+Double.valueOf(ulflux))/1048576; //单位由Byte转为MB
					//将标签产生的流量值累计到热点区域对应的标签中
					redisserver.incrbyfloat(key, g4flux);
				}
			}
		}

		//释放内存
		redisserver=null;
		tdate=null;
		imsi=null;
		dlflux=null;
		ulflux=null;
		tac=null;
		ci=null;
		hotspot=null;
		hour=null;
		minute=null;
		clk1=0;
		clk2=0;
		key=null;
		tag=null;
		g4flux=0;
		collector.ack(tuple);
	}
	
	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
	}

}

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
