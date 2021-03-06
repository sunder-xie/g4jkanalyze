package cm.storm.g4jk.Bolts;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import cm.storm.g4jk.Beans.Yunguan_G4JK_BasicTAUFields;
import cm.storm.g4jk.Commons.FileServer;
import cm.storm.g4jk.Commons.RedisServer;
import cm.storm.g4jk.Commons.ResourcesConfig;
import cm.storm.g4jk.Commons.TimeFormatter;

/**
 * 每15分钟补充统计热点区域，热力图对应的人流量(区别imsi)
 * @author chinamobile
 * 20160907
 */
public class Yunguan_G4JK_TauAccToRedis extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = -3632540551532546324L;

	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_TauAccToRedis.class);
	
	//元组发射搜集器
	private OutputCollector collector;

	//获取redis连接
	private RedisServer redisserver;
	
	//获取文件对象
	private FileServer fileserver;
	
	//初始化bolt元组搜集器，用于存放需要发射元组
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	//按照4G网分数据tac,ci，汇总流量，15分钟窗口，记录数量万级别
	@Override
	public void execute(Tuple tuple) {
		//redis操作
		redisserver=RedisServer.getInstance();
		fileserver=FileServer.getInstance();
		String tdate=tuple.getStringByField(Yunguan_G4JK_BasicTAUFields.TTIME);
		String imsi=tuple.getStringByField(Yunguan_G4JK_BasicTAUFields.IMSI);
		String tac=tuple.getStringByField(Yunguan_G4JK_BasicTAUFields.TAC);
		String ci=tuple.getStringByField(Yunguan_G4JK_BasicTAUFields.CI);
		Set<String> hotspotlist=null;
		String hour=null;
		String minute=null;
		String tcsll=null;
		int clk=0;
		String key=null;
		String value=null;
		long rt=0;
		String imsi_catch_time=null;
		String imsi_tdate1="19000101000000";
		String imsi_tdate2="19000101000000";
		String imsi_hsp_file=null;
		String data_time=null;
		
		if(tdate.length()>=23&&imsi.length()>=15){
			//热点区域信息所需维表
			key="ref_hsp_"+tac+"_"+ci;
			hotspotlist=redisserver.smembers(key);
			data_time=TimeFormatter.getNow(); 						//获取当前时间YYYY-MM-DD HH:mm:ss
			//基站tac ci缩减维表
			key="ref_hpm_"+tac+"_"+ci;
			tcsll=redisserver.get(key);

			hour=tdate.substring(11,13);
			minute=tdate.substring(14,16);
			imsi_catch_time=tdate.substring(0,19);
			imsi_catch_time=imsi_catch_time.replaceAll("[^0-9]","");
			clk=Integer.valueOf(minute); 	//会自动过滤数字前边的0
			tdate=tdate.substring(0,10);
			if(clk>=0&&clk<15)minute="00";
			else if(clk>=15&&clk<30)minute="15";
			else if(clk>=30&&clk<45)minute="30";
			else if(clk>=45)minute="45";
			
			//热点区域人流补充
			if(hotspotlist!=null&&hotspotlist.size()>0)
			{
				for(String hotspot : hotspotlist){
					//标记hotspot捕获imsi的时间
					key="mfg4_"+tdate+"_hspimsi_"+hotspot+"_"+imsi;
					imsi_tdate1=redisserver.get(key);
					imsi_hsp_file="nil";
					if(imsi_tdate1==null||imsi_tdate1.equals("nil")){
						imsi_hsp_file=data_time+"|"+hotspot+"|"+imsi+"|"+imsi_catch_time+"|"+imsi_catch_time+"\n";
						imsi_tdate1=imsi_catch_time+";"+imsi_catch_time;
					}
					else if (imsi_tdate1.length()>=29){
						imsi_tdate2=imsi_tdate1.substring(15);
						imsi_tdate1=imsi_tdate1.substring(0,14);
						if(imsi_catch_time.compareTo(imsi_tdate1)<0){
							imsi_hsp_file=data_time+"|"+hotspot+"|"+imsi+"|"+imsi_catch_time+"|"+imsi_tdate2+"\n";
							imsi_tdate1=imsi_catch_time+";"+imsi_tdate2;
						}
						else if(imsi_catch_time.compareTo(imsi_tdate2)>0){
							imsi_hsp_file=data_time+"|"+hotspot+"|"+imsi+"|"+imsi_tdate1+"|"+imsi_catch_time+"\n";
							imsi_tdate1=imsi_tdate1+";"+imsi_catch_time;
						}
						else imsi_tdate1=imsi_tdate1+";"+imsi_tdate2;
					}
					if(imsi_hsp_file.equals("nil")==false){
						fileserver.setWordsToFile(imsi_hsp_file, ResourcesConfig.LOCAL_IMIS_HSP_PATH);
					}
					redisserver.set(key, imsi_tdate1);
					
					//将imsi累计到热点区域中,以15分钟为维度进行创建
//					key="mfg4_"+tdate+"_hspset_"+hotspot+"_"+hour+"_"+minute;
//					redisserver.sadd(key, imsi);
					if(imsi.equals("123456789012345")==false){
						key="mfg4_"+tdate+"_imsihot_"+imsi;
						value=hour+"_"+minute+"_"+hotspot;
						rt=redisserver.sadd(key,value);
					}else rt=1;
					if(rt>0){
						key="mfg4_"+tdate+"_hspset_"+hotspot+"_"+hour+"_"+minute;	
						redisserver.incr(key);
					}
				}
			}
			
			//热力图区域人流补充
			if(tcsll!=null&&tcsll.equals("nil")==false)
			{
				//将imsi累计到对应的标签中
//				key="mfg4_"+tdate+"_hmset_"+hour+"_"+minute+"_"+tcsll;
//				redisserver.sadd(key, imsi);
				//将imsi累计到对应的标签中，空间换效率尝试20161031
				if(imsi.equals("123456789012345")==false){
					key="mfg4_"+tdate+"_imsihot_"+imsi;
					value=hour+"_"+minute+"_"+tcsll;
					rt=redisserver.sadd(key,value);
					if(rt>0){
						key="mfg4_"+tdate+"_localtotal_"+hour+"_"+minute; //统计每个时刻的总人数
						redisserver.incr(key);
						key="mfg4_"+tdate+"_localtotalset";//汇总一天总的imsi集合，用于统计总人数
						redisserver.sadd(key, imsi);
					}
				}else rt=1;
				if(rt>0){
					key="mfg4_"+tdate+"_hmset_"+hour+"_"+minute+"_"+tcsll;	
					redisserver.incr(key);
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
		tcsll=null;
		key=null;
		value=null;
		rt=0;
		imsi_catch_time=null;
		imsi_tdate1=null;
		imsi_tdate2=null;
		clk=0;
		imsi_hsp_file=null;
		data_time=null;

		collector.ack(tuple);
	}

	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
	}

}

//统计标签对应的人数
//Set<String> custtag=null;
//custtag=null;
//key="ref_custtag_"+imsi;
//custtag=redisserver.smembers(key);

//补充累计当天人群标签对应的人流量
//if(custtag!=null&&custtag.size()>0){
//	for(String cid:custtag){
//		key="mfg4_"+tdate+"_custtag_"+cid;
//		redisserver.sadd(key,imsi);
//	}
//}
//String tag=null;
//key="ref_tags_"+imsi;
////查询维表获取标签
//tag=redisserver.get(key);


//key="mfg4_"+tdate+"_hspdayset_"+hotspot;	//记录每天对应的hostspot中的imsi明细
//redisserver.sadd(key, imsi);

//标记hotspot捕获imsi的时间
//key="mfg4_"+tdate+"_"+imsi+"_"+hotspot;
//key="mfg4_"+tdate+"_hsptime_"+hotspot+"_"+imsi;
//imsi_tdate1=redisserver.get(key);
//if(imsi_tdate1==null||imsi_tdate1.equals("nil"))imsi_tdate1=imsi_catch_time+";"+imsi_catch_time;
//else if (imsi_tdate1.length()>=29){
//	imsi_tdate2=imsi_tdate1.substring(15);
//	imsi_tdate1=imsi_tdate1.substring(0,14);
//	if(imsi_catch_time.compareTo(imsi_tdate1)<0)imsi_tdate1=imsi_catch_time+";"+imsi_tdate2;
//	else if(imsi_catch_time.compareTo(imsi_tdate2)>0)imsi_tdate1=imsi_tdate1+";"+imsi_catch_time;
//	else imsi_tdate1=imsi_tdate1+";"+imsi_tdate2;
//}
//redisserver.set(key, imsi_tdate1);

//将imsi累计到热点区域中,以15分钟为维度进行创建
//key="mfg4_"+tdate+"_hspset_"+hour+"_"+minute+"_"+hotspot;
//redisserver.sadd(key, imsi);
//key="mfg4_"+tdate+"_hspimsi_"+hotspot+"_"+hour+"_"+minute+"_"+imsi;
//redisserver.set(key, "1");
//
//if(tag!=null&&tag.equals("nil")==false)
//{
//	key="mfg4_"+tdate+"_hspset_"+hour+"_"+minute+"_"+hotspot+"_"+tag;
//	//将imsi累计到热点区域对应的标签中,以15分钟为维度进行创建
//	redisserver.sadd(key, imsi);
//}
