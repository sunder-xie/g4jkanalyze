package cm.storm.g4jk.Bolts;

import java.util.Map;
//import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import cm.storm.g4jk.Beans.Yunguan_G4JK_Basic4GFields;
import cm.storm.g4jk.Commons.RedisServer;

/**
 * 每15分钟统计热点区域的人流量(区别imsi)，人群标签统计
 * @author chinamobile
 * 20160907
 */
public class Yunguan_G4JK_HmapAccToRedis extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = -6609360345719069810L;

	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_HmapAccToRedis.class);
	
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

	//按照4G网分数据tac,ci，汇总流量，15分钟窗口，记录数量万级别
	@Override
	public void execute(Tuple tuple) {
		//redis操作
		redisserver=RedisServer.getInstance();
		String tdate=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.STARTTIME);
		String imsi=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.IMSI);
		String tac=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.TAC);
		String ci=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.CID);
		String url=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.URL);
		String intsid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTSID);
		String tcsll=null;
		String hour=null;
		String minute=null;
		String appdate=tdate;
		String appvalue=null;
		int clk=0;
		String key=null;
		String value=null;
		long rt=0;
		
		if(tdate.length()>=23&&imsi.length()>=15){
			key="ref_hpm_"+tac+"_"+ci;
			tcsll=redisserver.get(key);
			hour=tdate.substring(11,13);
			minute=tdate.substring(14,16);
			clk=Integer.valueOf(minute); 	//会自动过滤数字前边的0
			tdate=tdate.substring(0,10);
			if(tcsll!=null&&tcsll.equals("nil")==false){
				if(clk>=0&&clk<15)minute="00";
				else if(clk>=15&&clk<30)minute="15";
				else if(clk>=30&&clk<45)minute="30";
				else if(clk>=45)minute="45";

//				key="mfg4_"+tdate+"_hmset_"+hour+"_"+minute+"_"+tcsll;
//				redisserver.sadd(key, imsi);
				//将imsi累计到对应的标签中，空间换效率尝试20161031，如果对于imsi为空，默认值123456789012345的情况，直接累加，不做判断，默认为未知用户
				if(imsi.equals("123456789012345")==false){
					key="mfg4_"+tdate+"_imsihot_"+imsi;
					value=hour+"_"+minute+"_"+tcsll;
					rt=redisserver.sadd(key,value);
				}else rt=1;
				if(rt>0){
					key="mfg4_"+tdate+"_hmset_"+hour+"_"+minute+"_"+tcsll;	
					redisserver.incr(key);
					key="mfg4_"+tdate+"_localtotal_"+hour+"_"+minute; //统计每个时刻的总人数
					redisserver.incr(key);
				}
				
				//临时处理代码段，新增累计当天的淘宝，京东，天猫每隔一小时的人数
				if(intsid!=null&&intsid.trim().equals("")==false&&intsid.trim().equals("none")==false){
					if(intsid.equals("1613")==true) key="mfg4_"+tdate+"_taobao_"+tcsll+"_"+hour;		//淘宝
					else if(intsid.equals("2545")==true) key="mfg4_"+tdate+"_tmall_"+tcsll+"_"+hour;	//天猫
					else if(intsid.equals("1061")==true) key="mfg4_"+tdate+"_jd_"+tcsll+"_"+hour;		//京东
				}
				redisserver.sadd(key, imsi);
			}
			
			//统计appid的使用集合，每个appid的使用热度，补充微信支付判断逻辑
			if(intsid!=null&&intsid.trim().equals("")==false&&intsid.trim().equals("none")==false){
				appdate=appdate.substring(0,10);	//获取日期
				key="ref_wtag_"+intsid;
				appvalue=redisserver.get(key);
				//应用的维表中存在翻译信息则进行数据累加，不对这两大类做统计
				if(appvalue!=null&&appvalue.length()>0&&appvalue.contains("浏览器")==false&&appvalue.contains("其他")==false){
					key="mfg4_"+appdate+"_AppidSet";
					redisserver.sadd(key, intsid);

					key="mfg4_"+appdate+"_AppUse_"+intsid;
					redisserver.incr(key); 	//累计当天的访问次数
					
					//微信支付判断逻辑,intappid为8943或者66,url中包含pay，则计入mfg4_YYYY-MM-DD_AppUse_3333，3333为自定义的维表数据 微信支付
					if((intsid.equals("66")||intsid.equals("8943"))&&(url.toLowerCase().contains("pay")==true)){
						key="mfg4_"+tdate+"_AppUse_3333";
						redisserver.incr(key); 	//累计当天微信支付次数
					}
				}
			}
		}
		//释放内存
		redisserver=null;
		tdate=null;
		imsi=null;
		tac=null;
		ci=null;
		url=null;
		intsid=null;
		tcsll=null;
		hour=null;
		minute=null;
		clk=0;
		key=null;
		value=null;
		rt=0;
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

//String intsid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTSID);
//Set<String> custtag=null;
//custtag=null;

//20161110临时处理代码段，新增累计当天的淘宝，京东，天猫每隔一小时的人数
//if(intsid.endsWith("1613")==true) key="mfg4_"+tdate+"_taobao_"+tcsll+"_"+hour;		//淘宝
//else if(intsid.endsWith("2545")==true) key="mfg4_"+tdate+"_tmall_"+tcsll+"_"+hour;	//天猫
//else if(intsid.endsWith("1061")==true) key="mfg4_"+tdate+"_jd_"+tcsll+"_"+hour;		//京东
//else if(intsid.endsWith("1733")==true) key="mfg4_"+tdate+"_vip_"+tcsll+"_"+hour;		//唯品会
//else if(intsid.endsWith("1593")==true) key="mfg4_"+tdate+"_snyg_"+tcsll+"_"+hour;	//苏宁易购
//redisserver.sadd(key, imsi);

//20161110临时处理代码段，统计标签对应的人数
//key="ref_custtag_"+imsi;
//custtag=redisserver.smembers(key);
//if(custtag!=null&&custtag.size()>0){
//	for(String cid:custtag){
//		key="mfg4_"+tdate+"_custtag_"+cid;
//		redisserver.sadd(key,imsi);
//	}
//}


//将标签产生的流量值累计到对应的标签中，2016年10月8日，未使用暂停
//String dlflux=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.DL_DATA);
//String ulflux=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.UL_DATA);
//double g4flux=0;
//key="mfg4_"+tdate+"_hmflux_"+hour+"_"+minute+"_"+tcsll;
//g4flux=(Double.valueOf(dlflux)+Double.valueOf(ulflux))/1048576; //单位由Byte转为MB
//redisserver.incrbyfloat(key, g4flux);
//g4flux=0;
//dlflux=null;
//ulflux=null;

//if(clk2>=0&&clk2<5)minute="05";
//else if(clk2>=5&&clk2<10)minute="10";
//else if(clk2>=10&&clk2<15)minute="15";
//else if(clk2>=15&&clk2<20)minute="20";
//else if(clk2>=20&&clk2<25)minute="25";
//else if(clk2>=25&&clk2<30)minute="30";
//else if(clk2>=30&&clk2<35)minute="35";
//else if(clk2>=35&&clk2<40)minute="40";
//else if(clk2>=40&&clk2<45)minute="45";
//else if(clk2>=45&&clk2<50)minute="50";
//else if(clk2>=50&&clk2<55)minute="55";
//else if(clk2>=55){
//	clk1+=1;
//	hour=String.format("%02d", clk1);
//	minute="00";
//}
