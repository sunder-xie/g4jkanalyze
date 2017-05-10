package cm.storm.g4jk.Bolts;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import cm.storm.g4jk.Beans.Yunguan_G4JK_Basic4GFields;

/**
 * url的中文提取，本段代码添加了url的中文信息提取并组合成字符串，继续转发。
 * @author chinamobile
 * 20161008
 */
public class Yunguan_G4JK_ZhWordsToCountBolt extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = 6883339164702097976L;

	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_ZhWordsToCountBolt.class);
	
	//元组发射搜集器
	private OutputCollector collector;
	
	//初始化bolt元组搜集器，用于存放需要发射元组
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	//初始方案：先通过url进行检索，获取对应的中文字与维表上的中文字直接匹配
	@Override
	public void execute(Tuple tuple) {
		//redis操作
		String tdate=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.STARTTIME);
		String url=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.URL);
		//临时需求
		String imsi=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.IMSI);
		String tac=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.TAC);
		String ci=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.CID);
		String intsid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTSID);
		String host=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.HOST);
		boolean flag=false;
		//记录url中的中文字符串
		String chinesewords=null;

		//如果不需要热词检索逻辑则注释掉相应的热词检测保存代码，仅保留最后的转发逻辑
		chinesewords=getChineseWordsFromUrl(url);
		//查看过滤无效的中文字串
		flag=fillterUnValidWords(chinesewords);
		if(flag==false&&chinesewords!=null
		  &&tdate.length()>=23&&chinesewords.length()>=2){
			chinesewords=chinesewords.trim();
			//如果提取之后存在中文信息，并且符合一定规律将信息转发给bolt做中文热词分析，有域名才进行分析，否则没有意义
			collector.emit(new Values(tdate,chinesewords,imsi,tac,ci,intsid,host));
		}
		
		//释放内存
		tdate=null;
		url=null;
		chinesewords=null;
		imsi=null;
		tac=null;
		ci=null;
		intsid=null;
		host=null;
		collector.ack(tuple);
	}

	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
		outputFieldsDeclarer.declare(new Fields(Yunguan_G4JK_Basic4GFields.STARTTIME,
				"ChineseInfo",
				Yunguan_G4JK_Basic4GFields.IMSI,
				Yunguan_G4JK_Basic4GFields.TAC,
				Yunguan_G4JK_Basic4GFields.CID,
				Yunguan_G4JK_Basic4GFields.INTSID,
				Yunguan_G4JK_Basic4GFields.HOST
		));
	}
	
	//自定义方法区域
	/**
	 * 提取url中的中文
	 * @param url 网分数据中的url
	 * @return 返回中文字符串或者null
	 */
	public String getChineseWordsFromUrl(String url){
		String url_ch=null;
		//对url做转换操作，通用转换做法假定就是 GBK 的编码：
		//将其解码成字节码，然后再把字节码编码为GBK，
		//如果转换回来后与没有转换之前是相等的。
		//这样假设成立，也就是GBK编码。如果解析失败，则用utf8解码
		try {
			String reg = "[^\u4e00-\u9fa5]";   //^匹配所有非中文字符, \u4e00, \u9fa5代表是两个unicode编码值，他们正好是Unicode表中的汉字的头和尾
			String fis= null;
			String sec = null;
			//尝试解码3次，单次解码未必直接能够解析出中文
			for(int i=0;i<3;i++)
			{
				fis = java.net.URLDecoder.decode(url, "gb2312");
				sec = new String(fis.getBytes("gb2312"), "gb2312");
				if (fis.equals(sec)==true)
					url=fis;
		        else
		        	url= java.net.URLDecoder.decode(url, "utf-8");
			}

			//提取url中的中文
			url = url.replaceAll(reg, "");
			//记录中文信息
			if(url!=null&&url.length()>=2)url_ch=url;
		} catch (Exception ex) {
			//LOG.info("Yunguan_G4JK_TouchSjjsToRedis execute error: "+ex.getMessage());
			return null;
		}
		return url_ch;
	}
	
	/**
	 * 过滤 邪黄赌毒，敏感信息，同时可减少数据量
	 * @param str
	 * @return true代表包含黄赌毒，或者敏感信息，不做热词统计
	 */
	public boolean fillterUnValidWords(String str)
	{
		if(str==null||str.trim().equals("")==true)return true;
		str=str.trim();
		String not="赌博,奇葩,六合,假牌,假证,迷药,杀人,放火,仿真枪,成人,成人网,自慰,充气娃娃,血滴子,阳具,阴道,阴部,抢劫,偷盗,枪支,弹药,假冒,事变,政变,老千,法轮,全能神,全能教,邪教,冰毒,摇头丸,大麻,造反,色吧,鸡鸡,手淫,性吧,性福,性欲,狠狠插,红灯区,卖淫,淫乱,爆乳,约炮,色情,情色,吞精,精液,艳照,淫荡,勾引,爱爱,做爱,偷情,偷性,交配,撸管,色系,鸡巴"
				+ "龟头,毒品,吸毒,叫鸡,洗钱,黑钱,赌钱,性骚扰,裸奔,裸照,轮奸,强奸,色图,淫娃,爆乳,妖姬,海天盛筵,生殖器,插插,壮阳,性故事,不雅照,一夜情,造爱,草榴,咪咪爱,阴蒂,阴唇,色色,走光,少妇,熟妇,熟女,日逼,操逼,黄图,黄片,强暴,强奸,迷奸,乱伦,阴茎,性交,裸体,射精,鸡婆,性侵,打飞机,奶子,吸奶,喂奶,巨乳,乳交,口交,口爆";
		String[] tmp=not.split(",");
		
		for(int i=0;i<tmp.length;i++)
		{
			if(str.contains(tmp[i])==true)return true;
		}
		return false;
	}
}

//String sdate=null;
//sdate=tdate;
//tdate=tdate.substring(0,10);	//获取日期YYYY-MM-DD
//sdate=null;
//String imsi=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.IMSI);
//String tac=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.TAC);
//String ci=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.CID);
//String intsid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTSID);
//String host=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.HOST);
//imsi=null;
//tac=null;
//ci=null;
//intsid=null;
//host=null;
//&&host!=null
//imsi, tac, ci, intsid, host, 
//host.equals("none")==false&&
//Yunguan_G4JK_Basic4GFields.TAC,
//Yunguan_G4JK_Basic4GFields.CID,
//Yunguan_G4JK_Basic4GFields.INTSID,
//Yunguan_G4JK_Basic4GFields.HOST,
//Yunguan_G4JK_Basic4GFields.STARTTIME
