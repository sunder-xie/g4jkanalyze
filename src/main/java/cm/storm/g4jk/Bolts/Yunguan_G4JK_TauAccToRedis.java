package cm.storm.g4jk.Bolts;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import cm.storm.g4jk.Beans.Yunguan_G4JK_BasicTAUFields;
import cm.storm.g4jk.Commons.RedisServer;

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
		String tdate=tuple.getStringByField(Yunguan_G4JK_BasicTAUFields.TTIME);
		String imsi=tuple.getStringByField(Yunguan_G4JK_BasicTAUFields.IMSI);
		String tac=tuple.getStringByField(Yunguan_G4JK_BasicTAUFields.TAC);
		String ci=tuple.getStringByField(Yunguan_G4JK_BasicTAUFields.CI);
		String hotspot=null;
		String hour=null;
		String minute=null;
		String tag=null;
		String tcsll=null;
		int clk1=0,clk2=0;
		String key=null;
		
		if(tdate.length()>=23&&imsi.length()>=15){
			key="ref_hsp_"+tac+"_"+ci;
			//查询维表获取标签
			hotspot=redisserver.get(key);
			
			key="ref_tags_"+imsi;
			//查询维表获取标签
			tag=redisserver.get(key);
			
			key="ref_hpm_"+tac+"_"+ci;
			//查询维表获取标签
			tcsll=redisserver.get(key);
			
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
			
			//热点区域人流补充
			if(hotspot!=null&&hotspot.equals("nil")==false)
			{
				key="mfg4_"+tdate+"_hspdayset_"+hotspot;	//记录每天对应的hostspot中的imsi明细
				redisserver.sadd(key, imsi);
				
				key="mfg4_"+tdate+"_hspset_"+hour+"_"+minute+"_"+hotspot;
				//将imsi累计到热点区域中,以15分钟为维度进行创建
				redisserver.sadd(key, imsi);

				if(tag!=null&&tag.equals("nil")==false)
				{
					key="mfg4_"+tdate+"_hspset_"+hour+"_"+minute+"_"+hotspot+"_"+tag;
					//将imsi累计到热点区域对应的标签中,以15分钟为维度进行创建
					redisserver.sadd(key, imsi);
				}
			}
			
			//热力图区域人流补充
			if(tcsll!=null&&tcsll.equals("nil")==false)
			{
				key="mfg4_"+tdate+"_hmset_"+hour+"_"+minute+"_"+tcsll;
				//将imsi累计到对应的标签中
				redisserver.sadd(key, imsi);
			}
		}
		
		//释放内存
		redisserver=null;
		tdate=null;
		imsi=null;
		tac=null;
		ci=null;
		hotspot=null;
		hour=null;
		minute=null;
		tcsll=null;
		key=null;
		clk1=0;
		clk2=0;
		collector.ack(tuple);
	}

	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
	}

}
