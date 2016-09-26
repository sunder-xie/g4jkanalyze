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

import cm.storm.g4jk.Beans.Yunguan_G4JK_BasicJKBean;
import cm.storm.g4jk.Beans.Yunguan_G4JK_BasicJKFields;
import cm.storm.g4jk.Commons.TimeFormatter;

/*
 * 2016-09-05 对家宽网分记录字段拆分解析
 * nicolashsu
 */
public class Yunguan_G4JK_BasicJKSplitter extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = -5352411334056685457L;

	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_BasicJKSplitter.class);
	
	//元组发射搜集器
	private OutputCollector collector;
		
	//元组存储结构
	private Yunguan_G4JK_BasicJKBean g4jkbasicjkbean=null;

	//初始化bolt元组搜集器，用于存放需要发射元组
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	//从Spoutwfjk获取字段信息进行字段解析,发射
	@Override
	public void execute(Tuple tuple) {
		String str_tuple=tuple.getString(0);		//获取tuple中的string组成的记录
		if(StringUtils.isBlank(str_tuple)==false)
		{
			//字段获取开始
			process_tuple(str_tuple);
			//字段获取结束
			
			if(g4jkbasicjkbean!=null)
			{
				//发射元组
				collector.emit(new Values(
						g4jkbasicjkbean.getStarttime(),
						g4jkbasicjkbean.getLasttime(),
						g4jkbasicjkbean.getUser_name(),
						g4jkbasicjkbean.getUser_type(),
						g4jkbasicjkbean.getUri(),
						g4jkbasicjkbean.getApp_type(),
						g4jkbasicjkbean.getApp_sub_type(),
						g4jkbasicjkbean.getUp_flux(),
						g4jkbasicjkbean.getDown_flux(),
						g4jkbasicjkbean.getProtocol_type(),
						g4jkbasicjkbean.getSuccess(),
						g4jkbasicjkbean.getUser_ip(),
						g4jkbasicjkbean.getDest_ip(),
						g4jkbasicjkbean.getSrc_port(),
						g4jkbasicjkbean.getDest_port(),
						g4jkbasicjkbean.getResponse_time()
				));
			}
		}
		//释放内存
		g4jkbasicjkbean=null;
		collector.ack(tuple);
	}

	//自定义方法区域
	/*
	 * @param tuple:原始String类型的一条会话流
	 */
	public void process_tuple(String tuple){
		g4jkbasicjkbean=new Yunguan_G4JK_BasicJKBean();
		String attr_value=new String("");
		String[] fields_set=tuple.split("\t");//按照TAB作为间隔划分字段
		if(fields_set==null){
			g4jkbasicjkbean=null;
			return;
		}
		
		//字段共16个，减去2个空值，14个可用
		//字段1，获取会话开始时间
		if(fields_set.length>0){
			attr_value=TimeFormatter.Tra_realdate2(fields_set[0]);
			g4jkbasicjkbean.setStarttime(attr_value);
		}
		
		//字段2，获取会话结束时间
		if(fields_set.length>1){
			attr_value=TimeFormatter.Tra_realdate2(fields_set[1]);
			g4jkbasicjkbean.setLasttime(attr_value);
		}
		
		//字段3，用户账号
		if(fields_set.length>2){
			if(fields_set[2].length()>0)g4jkbasicjkbean.setUser_name(fields_set[2].trim());
		}
		
		//字段4，用户类型
		if(fields_set.length>3){
			if(fields_set[3].length()>0)g4jkbasicjkbean.setUser_type(fields_set[3]);
		}
		
		//字段5，URI
		if(fields_set.length>4){
			if(fields_set[4].length()>0)g4jkbasicjkbean.setUri(fields_set[4]);
		}
		
		//字段6，URL一级业务分类
		if(fields_set.length>5){
			if(fields_set[5].length()>0)g4jkbasicjkbean.setApp_type(fields_set[5]);
		}
		
		//字段7，URL二级业务分类
		if(fields_set.length>6){
			if(fields_set[6].length()>0)g4jkbasicjkbean.setApp_sub_type(fields_set[6]);
		}
		
		//字段8，上行流量
		if(fields_set.length>7){
			if(fields_set[7].length()>0)g4jkbasicjkbean.setUp_flux(fields_set[8]);
		}
		
		//字段9，下行流量
		if(fields_set.length>8){
			if(fields_set[8].length()>0)g4jkbasicjkbean.setDown_flux(fields_set[8]);
		}
		
		//字段10，协议类型，默认为1
		if(fields_set.length>9){
			if(fields_set[9].length()>0)g4jkbasicjkbean.setProtocol_type(fields_set[9]);
		}
		
		//字段11，业务是否成功
		if(fields_set.length>10){
			if(fields_set[10].length()>0)g4jkbasicjkbean.setSuccess(fields_set[10]);
		}
		
		//字段12，业务源ip
		if(fields_set.length>11){
			if(fields_set[11].length()>0)g4jkbasicjkbean.setUser_ip(fields_set[11]);
		}
		
		//字段13，目的ip
		if(fields_set.length>12){
			if(fields_set[12].length()>0)g4jkbasicjkbean.setDest_ip(fields_set[12]);
		}
		
		//字段14，源端口
		if(fields_set.length>13){
			if(fields_set[13].length()>0)g4jkbasicjkbean.setSrc_port(fields_set[13]);
		}
		
		//字段15，目的端口
		if(fields_set.length>14){
			if(fields_set[14].length()>0)g4jkbasicjkbean.setDest_port(fields_set[14]);
		}
		
		//字段16，响应时间
		if(fields_set.length>15){
			if(fields_set[15].length()>0)g4jkbasicjkbean.setResponse_time(fields_set[15]);
		}
	}
	
	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
		outputFieldsDeclarer.declare(new Fields(
			Yunguan_G4JK_BasicJKFields.STARTTIME,
			Yunguan_G4JK_BasicJKFields.LASTTIME,
			Yunguan_G4JK_BasicJKFields.USER_NAME,
			Yunguan_G4JK_BasicJKFields.USER_TYPE,
			Yunguan_G4JK_BasicJKFields.URI,
			Yunguan_G4JK_BasicJKFields.APP_TYPE,
			Yunguan_G4JK_BasicJKFields.APP_SUB_TYPE,
			Yunguan_G4JK_BasicJKFields.UP_FLUX,
			Yunguan_G4JK_BasicJKFields.DOWN_FLUX,
			Yunguan_G4JK_BasicJKFields.PROTOCOL_TYPE,
			Yunguan_G4JK_BasicJKFields.SUCCESS,
			Yunguan_G4JK_BasicJKFields.USER_IP,
			Yunguan_G4JK_BasicJKFields.DEST_IP,
			Yunguan_G4JK_BasicJKFields.SRC_PORT,
			Yunguan_G4JK_BasicJKFields.DEST_PORT,
			Yunguan_G4JK_BasicJKFields.RESPONSE_TIME
		));
	}

}
