package cm.storm.g4jk.test;

import cm.storm.g4jk.Beans.Yunguan_G4JK_Basic4GBean;
import cm.storm.g4jk.Commons.TimeFormatter;

public class flowtest {

	public static void main(String[] args) {
		Yunguan_G4JK_Basic4GBean g4jkbasic4gbean=new Yunguan_G4JK_Basic4GBean();
		String tuple1=new String("2016/8/15 9:37:26.498	460003021211258		867620029196398	9313	31054593		1952	1645						CMNET.MNC000.MCC460.GPRS	1	169274806	3747870114	45534	80	0	24142	24142");
		//String tuple1=new String("2016/8/15 9:35:50.260	460009202282599	/core?t=2&chipid=&tm=120&ra=2&ishcdn=0&pf=2&p=22&p1=221&p2=2211&sdktp=1&c1=&r=522214200&aid=522214200&u=F7A389AC%2D75F6%2D41A6%2D9226%2D072ADFCDABC5&pu=1306122429&v=7%2E6&krv=3%2E2%2E3&dt=&hu=3&rn=1471224948&islocal=2&as=2efb1cf06a72e56e8922fdbf9862a198&ve=694bc6435902d7fc91134763b7a2abf1&pe=&vfrm=&chl=&hcdnv=10.10.6.17&tpcd=1&isdrm=1&ht=0&ptid=02032001010000000000&mod=&lvbck=1011&nettype=2	352021064139124	10105	49114626		811	313	55	QYPlayer/iOS/3.2.3				CMNET.MNC000.MCC460.GPRS	1	173589659	3747870157	50172	80	0	24142	24142\n");
		
		String attr_value=new String("");
		String[] fields_set=tuple1.split("\t");//按照TAB作为间隔划分字段
		//字段1，获取日期并做格式转换
		if(fields_set.length>0){
			attr_value=TimeFormatter.Tra_realdate2(fields_set[0]);
			g4jkbasic4gbean.setStarttime(attr_value);
		}
		
		//字段2，获取IMSI
		if(fields_set.length>1){
			if(fields_set[1].length()==15)g4jkbasic4gbean.setImsi(fields_set[1]);
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
			if(fields_set[4].length()>=4)g4jkbasic4gbean.setTac(fields_set[4]);
		}
		
		//字段6，获取基站小区码cell_id，或者填写数字，或者填写none
		if(fields_set.length>5){
			if(fields_set[5].length()>0)g4jkbasic4gbean.setCid(fields_set[5]);
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
		
		System.out.println(g4jkbasic4gbean.getStarttime());
		System.out.println(g4jkbasic4gbean.getImsi());
		System.out.println(g4jkbasic4gbean.getUrl());
		System.out.println(g4jkbasic4gbean.getImei());
		System.out.println(g4jkbasic4gbean.getTac());
		System.out.println(g4jkbasic4gbean.getCid());
		System.out.println(g4jkbasic4gbean.getEvent_type());
		System.out.println(g4jkbasic4gbean.getUl_data());
		System.out.println(g4jkbasic4gbean.getDl_data());
		System.out.println(g4jkbasic4gbean.getDelay());
		System.out.println(g4jkbasic4gbean.getUser_agent());
		System.out.println(g4jkbasic4gbean.getGmcc_bus_ind());
		System.out.println(g4jkbasic4gbean.getPhone_brand());
		System.out.println(g4jkbasic4gbean.getPhone_type());
		System.out.println(g4jkbasic4gbean.getApn());
		System.out.println(g4jkbasic4gbean.getPro_type());
		System.out.println(g4jkbasic4gbean.getUser_ip());
		System.out.println(g4jkbasic4gbean.getApp_server_ip());
		System.out.println(g4jkbasic4gbean.getUser_port());
		System.out.println(g4jkbasic4gbean.getApp_server_port());
		System.out.println(g4jkbasic4gbean.getApptype());
		System.out.println(g4jkbasic4gbean.getIntappid());
		System.out.println(g4jkbasic4gbean.getIntsid());
//		attr_value=new String("");
//		fields_set=tuple2.split("\t");//按照TAB作为间隔划分字段
	}

}
