package cm.storm.g4jk.Bolts;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import cm.storm.g4jk.Beans.Yunguan_G4JK_Basic4GFields;
import cm.storm.g4jk.Commons.RedisServer;

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
		String clock=null;
		int clk=0;
		String key=null;
		double g4flux=0;
		if(tdate.length()>=23&&imsi.length()>=15){
			key="ref_"+tac+"_"+ci;
			//查询维表获取标签
			hotspot=redisserver.get(key);
			if(hotspot!=null&&hotspot.equals("nil")==false)
			{
				hour=tdate.substring(11,13);
				clock=tdate.substring(14,16);
				clk=Integer.valueOf(clock); //会自动过滤数字前边的0
				tdate=tdate.substring(0,10);
				if(clk>=0&&clk<15)clock="0";
				else if(clk>=15&&clk<30)clock="1";
				else if(clk>=30&&clk<45)clock="2";
				else if(clk>45)clock="3";
				key="g4_"+tdate+"_hspset_"+hotspot+"_"+hour+"_"+clock;

				//将imsi累计到对应的标签中
				redisserver.sadd(key, imsi);
				
				key="g4_"+tdate+"_hspflux_"+hotspot+"_"+hour+"_"+clock;
				g4flux=(Double.valueOf(dlflux)+Double.valueOf(ulflux))/1048576; //单位由Byte转为MB
				//将标签产生的流量值累计到对应的标签中
				redisserver.incrbyfloat(key, g4flux);
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
		clock=null;
		clk=0;
		key=null;
		g4flux=0;
		collector.ack(tuple);

	}

	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
	}

}
