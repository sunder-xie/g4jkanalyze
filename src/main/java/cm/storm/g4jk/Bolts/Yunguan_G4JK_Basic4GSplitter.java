package cm.storm.g4jk.Bolts;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import cm.storm.g4jk.Beans.Yunguan_G4JK_Basic4GBean;
import cm.storm.g4jk.Beans.Yunguan_G4JK_Basic4GFields;

/*
 * 2016-09-05 对4G网分记录字段拆分解析
 * nicolashsu
 */
public class Yunguan_G4JK_Basic4GSplitter extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = 4487280511535695268L;
	
	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_Basic4GSplitter.class);
	
	//元组发射搜集器
	private OutputCollector collector;
	
	//元组存储结构
	private Yunguan_G4JK_Basic4GBean g4jkbasic4gbean=null;

	//初始化bolt元组搜集器，用于存放需要发射元组
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	//从Spoutwf4g获取字段信息进行字段解析,发射
	@Override
	public void execute(Tuple tuple) {
		String str_tuple=tuple.getString(0);		//获取tuple中的string组成的记录
		if(StringUtils.isBlank(str_tuple)==false)
		{
			//字段获取开始
			process_tuple(str_tuple);
			//字段获取结束
			
			if(g4jkbasic4gbean!=null)
			{
				//发射元组
				collector.emit(new Values(
					g4jkbasic4gbean.getStarttime(),
					g4jkbasic4gbean.getImsi(),
					g4jkbasic4gbean.getUrl(),
					g4jkbasic4gbean.getImei(),
					g4jkbasic4gbean.getTac(),
					g4jkbasic4gbean.getCid(),
					g4jkbasic4gbean.getUl_data(),
					g4jkbasic4gbean.getDl_data(),
					g4jkbasic4gbean.getDelay(),
					g4jkbasic4gbean.getUser_agent(),
					g4jkbasic4gbean.getApn(),
					g4jkbasic4gbean.getPro_type(),
					g4jkbasic4gbean.getUser_ip(),
					g4jkbasic4gbean.getApp_server_ip(),
					g4jkbasic4gbean.getUser_port(),
					g4jkbasic4gbean.getApp_server_port(),
					g4jkbasic4gbean.getApptype(),
					g4jkbasic4gbean.getIntappid(),
					g4jkbasic4gbean.getIntsid()
				));
			}
		}
		//释放内存
		g4jkbasic4gbean=null;
		collector.ack(tuple);
	}
	
	//自定义方法区域
	/*
	 * @param tuple:原始String类型的一条会话流
	 */
	public void process_tuple(String tuple){
		
	}

	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
		outputFieldsDeclarer.declare(new Fields(
			Yunguan_G4JK_Basic4GFields.STARTTIME,
			Yunguan_G4JK_Basic4GFields.IMSI,
			Yunguan_G4JK_Basic4GFields.URL,
			Yunguan_G4JK_Basic4GFields.IMEI,
			Yunguan_G4JK_Basic4GFields.TAC,
			Yunguan_G4JK_Basic4GFields.CID,
			Yunguan_G4JK_Basic4GFields.UL_DATA,
			Yunguan_G4JK_Basic4GFields.DL_DATA,
			Yunguan_G4JK_Basic4GFields.DELAY,
			Yunguan_G4JK_Basic4GFields.USER_AGENT,
			Yunguan_G4JK_Basic4GFields.APN,
			Yunguan_G4JK_Basic4GFields.PRO_TYPE,
			Yunguan_G4JK_Basic4GFields.USER_IP,
			Yunguan_G4JK_Basic4GFields.APP_SERVER_IP,
			Yunguan_G4JK_Basic4GFields.USER_PORT,
			Yunguan_G4JK_Basic4GFields.APP_SERVER_PORT,
			Yunguan_G4JK_Basic4GFields.APPTYPE,
			Yunguan_G4JK_Basic4GFields.INTAPPID,
			Yunguan_G4JK_Basic4GFields.INTSID
		));
	}

	
	
}
