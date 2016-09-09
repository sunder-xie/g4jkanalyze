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
		String tcsll=null;
		String hour=null;
		String minute=null;
		int clk1=0,clk2=0;
		String key=null;
		double g4flux=0;
		if(tdate.length()>=23&&imsi.length()>=15){
			key="ref_hpm_"+tac+"_"+ci;
			tcsll=redisserver.get(key);
			if(tcsll!=null&&tcsll.equals("nil")==false){
				hour=tdate.substring(11,13);
				minute=tdate.substring(14,16);
				clk1=Integer.valueOf(hour); //会自动过滤数字前边的0
				clk2=Integer.valueOf(minute); //会自动过滤数字前边的0
				tdate=tdate.substring(0,10);
				if(clk2>=0&&clk2<5)minute="05";
				else if(clk2>=5&&clk2<10)minute="10";
				else if(clk2>=10&&clk2<15)minute="15";
				else if(clk2>=15&&clk2<20)minute="20";
				else if(clk2>=20&&clk2<25)minute="25";
				else if(clk2>=25&&clk2<30)minute="30";
				else if(clk2>=30&&clk2<35)minute="35";
				else if(clk2>=35&&clk2<40)minute="40";
				else if(clk2>=40&&clk2<45)minute="45";
				else if(clk2>=45&&clk2<50)minute="50";
				else if(clk2>=50&&clk2<55)minute="55";
				else if(clk2>=55){
					clk1+=1;
					hour=String.format("%010d", clk1);
					minute="00";
				}

				key="mfg4_"+tdate+"_hmset_"+hour+"_"+minute+"_"+tcsll;
				//将imsi累计到对应的标签中
				redisserver.sadd(key, imsi);
				
				key="mfg4_"+tdate+"_hmflux_"+hour+"_"+minute+"_"+tcsll;
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
		tcsll=null;
		hour=null;
		minute=null;
		clk1=0;
		clk2=0;
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
