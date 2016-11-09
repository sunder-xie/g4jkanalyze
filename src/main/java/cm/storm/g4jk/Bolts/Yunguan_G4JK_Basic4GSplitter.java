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
import cm.storm.g4jk.Commons.TimeFormatter;

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
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	//从Spoutwf4g获取字段信息进行字段解析,发射
	@Override
	public void execute(Tuple tuple) {
		String str_tuple=tuple.getString(0);		//获取tuple中的String组成的记录
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
					g4jkbasic4gbean.getEvent_type(),
					g4jkbasic4gbean.getUl_data(),
					g4jkbasic4gbean.getDl_data(),
					g4jkbasic4gbean.getDelay(),
					g4jkbasic4gbean.getUser_agent(),
					g4jkbasic4gbean.getGmcc_bus_ind(),
					g4jkbasic4gbean.getPhone_brand(),
					g4jkbasic4gbean.getPhone_type(),
					g4jkbasic4gbean.getApn(),
					g4jkbasic4gbean.getPro_type(),
					g4jkbasic4gbean.getUser_ip(),
					g4jkbasic4gbean.getApp_server_ip(),
					g4jkbasic4gbean.getUser_port(),
					g4jkbasic4gbean.getApp_server_port(),
					g4jkbasic4gbean.getApptype(),
					g4jkbasic4gbean.getIntappid(),
					g4jkbasic4gbean.getIntsid(),
					g4jkbasic4gbean.getHost()
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
		g4jkbasic4gbean=new Yunguan_G4JK_Basic4GBean();
		String attr_value=new String("");
		String[] fields_set=tuple.split("\t");//按照TAB作为间隔划分字段
		if(fields_set==null){
			g4jkbasic4gbean=null;
			return;
		}
		
		//字段共24个，目前4个空值，20个可用
		//字段1，获取日期并做格式转换
		if(fields_set.length>0){
			attr_value=TimeFormatter.Tra_realdate2(fields_set[0]);
			g4jkbasic4gbean.setStarttime(attr_value);
		}
		
		//字段2，获取IMSI
		if(fields_set.length>1){
			if(fields_set[1].length()==15)g4jkbasic4gbean.setImsi(fields_set[1].trim());
		}
		
		//字段3，获取url
		if(fields_set.length>2){
			if(fields_set[2].length()>0)g4jkbasic4gbean.setUrl(fields_set[2]);
		}
		
		//字段4，获取IMEI
		if(fields_set.length>3){
			if(fields_set[3].length()==15)g4jkbasic4gbean.setImei(fields_set[3]);
		}
		
		//字段5，获取基站TAC码，至少4位
		if(fields_set.length>4){
			if(fields_set[4].length()>=4)g4jkbasic4gbean.setTac(fields_set[4].trim());
		}
		
		//字段6，获取基站小区码cell_id，或者填写数字，或者填写none
		if(fields_set.length>5){
			if(fields_set[5].length()>0)g4jkbasic4gbean.setCid(fields_set[5].trim());
		}
		
		//字段7，事件类型
		if(fields_set.length>6){
			if(fields_set[6].length()>0)g4jkbasic4gbean.setEvent_type(fields_set[6]);
		}
		
		//字段8，上行流量
		if(fields_set.length>7){
			if(fields_set[7].length()>0)g4jkbasic4gbean.setUl_data(fields_set[7]);
		}
		
		//字段9，下行流量
		if(fields_set.length>8){
			if(fields_set[8].length()>0)g4jkbasic4gbean.setDl_data(fields_set[8]);
		}
		
		//字段10，会话时长
		if(fields_set.length>9){
			if(fields_set[9].length()>0)g4jkbasic4gbean.setDelay(fields_set[9]);
		}
		
		//字段11，终端型号
		if(fields_set.length>10){
			if(fields_set[10].length()>0)g4jkbasic4gbean.setUser_agent(fields_set[10]);
		}
		
		//字段12，是否自有业务
		if(fields_set.length>11){
			if(fields_set[11].length()>0)g4jkbasic4gbean.setGmcc_bus_ind(fields_set[11]);
		}
		
		//字段13，手机品牌
		if(fields_set.length>12){
			if(fields_set[12].length()>0)g4jkbasic4gbean.setPhone_brand(fields_set[12]);
		}
		
		//字段14，手机型号
		if(fields_set.length>13){
			if(fields_set[13].length()>0)g4jkbasic4gbean.setPhone_type(fields_set[13]);
		}
		
		//字段15，接入点
		if(fields_set.length>14){
			if(fields_set[14].length()>0)g4jkbasic4gbean.setApn(fields_set[14]);
		}
		
		//字段16，协议类型
		if(fields_set.length>15){
			if(fields_set[15].length()>0)g4jkbasic4gbean.setPro_type(fields_set[15]);
		}
		
		//字段17，业务源ip
		if(fields_set.length>16){
			if(fields_set[16].length()>0)g4jkbasic4gbean.setUser_ip(fields_set[16]);
		}
		
		//字段18，业务目标ip
		if(fields_set.length>17){
			if(fields_set[17].length()>0)g4jkbasic4gbean.setApp_server_ip(fields_set[17]);
		}
		
		//字段19，业务源端口
		if(fields_set.length>18){
			if(fields_set[18].length()>0)g4jkbasic4gbean.setUser_port(fields_set[18]);
		}
		
		//字段20，业务目的端口
		if(fields_set.length>19){
			if(fields_set[19].length()>0)g4jkbasic4gbean.setApp_server_port(fields_set[19]);
		}

		//字段21，业务大类
		if(fields_set.length>20){
			if(fields_set[20].length()>0)g4jkbasic4gbean.setApptype(fields_set[20]);
		}
		
		//字段22，app小类
		if(fields_set.length>21){
			if(fields_set[21].length()>0)g4jkbasic4gbean.setIntappid(fields_set[21]);
		}
		
		//字段23，业务小类
		if(fields_set.length>22){
			if(fields_set[22].length()>0)g4jkbasic4gbean.setIntsid(fields_set[22]);
		}
		
		//字段24，域名
		if(fields_set.length>23){
			if(fields_set[23].length()>0)g4jkbasic4gbean.setHost(fields_set[23]);
		}
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
			Yunguan_G4JK_Basic4GFields.EVENT_TYPE,
			Yunguan_G4JK_Basic4GFields.UL_DATA,
			Yunguan_G4JK_Basic4GFields.DL_DATA,
			Yunguan_G4JK_Basic4GFields.DELAY,
			Yunguan_G4JK_Basic4GFields.USER_AGENT,
			Yunguan_G4JK_Basic4GFields.GMCC_BUS_IND,
			Yunguan_G4JK_Basic4GFields.PHONE_BRAND,
			Yunguan_G4JK_Basic4GFields.PHONE_TYPE,
			Yunguan_G4JK_Basic4GFields.APN,
			Yunguan_G4JK_Basic4GFields.PRO_TYPE,
			Yunguan_G4JK_Basic4GFields.USER_IP,
			Yunguan_G4JK_Basic4GFields.APP_SERVER_IP,
			Yunguan_G4JK_Basic4GFields.USER_PORT,
			Yunguan_G4JK_Basic4GFields.APP_SERVER_PORT,
			Yunguan_G4JK_Basic4GFields.APPTYPE,
			Yunguan_G4JK_Basic4GFields.INTAPPID,
			Yunguan_G4JK_Basic4GFields.INTSID,
			Yunguan_G4JK_Basic4GFields.HOST
		));
	}

	
	
}
