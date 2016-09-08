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

/**
 * 每5分钟统计热点区域的人流量(区别imsi)，4G http流量使用量
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

	//按照4G网分数据tac,ci，汇总流量，5分钟窗口，记录数量万级别
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
		String hour=null;
		String clock=null;
		int clk=0;
		String key=null;
		double g4flux=0;
		if(tdate.length()>=23&&imsi.length()>=15){
			hour=tdate.substring(11,13);
			clock=tdate.substring(14,16);
			clk=Integer.valueOf(clock); //会自动过滤数字前边的0
			tdate=tdate.substring(0,10);
			if(clk>=0&&clk<5)clock="00";
			else if(clk>=5&&clk<10)clock="01";
			else if(clk>=10&&clk<15)clock="02";
			else if(clk>=15&&clk<20)clock="03";
			else if(clk>=20&&clk<25)clock="04";
			else if(clk>=25&&clk<30)clock="05";
			else if(clk>=30&&clk<35)clock="06";
			else if(clk>=35&&clk<40)clock="07";
			else if(clk>=40&&clk<45)clock="08";
			else if(clk>=45&&clk<50)clock="09";
			else if(clk>=50&&clk<55)clock="10";
			else if(clk>=55)clock="11";
			key="mfg4_"+tdate+"_hmset_"+tac+"_"+ci+"_"+hour+"_"+clock;
			//将imsi累计到对应的标签中
			redisserver.sadd(key, imsi);
			
			key="mfg4_"+tdate+"_hmflux_"+tac+"_"+ci+"_"+hour+"_"+clock;
			g4flux=(Double.valueOf(dlflux)+Double.valueOf(ulflux))/1048576; //单位由Byte转为MB
			//将标签产生的流量值累计到对应的标签中
			redisserver.incrbyfloat(key, g4flux);
		}
		//释放内存
		redisserver=null;
		tdate=null;
		imsi=null;
		dlflux=null;
		ulflux=null;
		tac=null;
		ci=null;
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
