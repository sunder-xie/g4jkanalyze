package cm.storm.g4jk.test;
//
//import java.net.URLDecoder;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//import cm.storm.g4jk.Beans.Yunguan_G4JK_Basic4GFields;

import java.util.List;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

//import cm.storm.g4jk.Beans.Yunguan_G4JK_Basic4GBean;
//import cm.storm.g4jk.Commons.TimeFormatter;

public class flowtest {	
	public static void main(String[] args) {
//		Yunguan_G4JK_Basic4GBean g4jkbasic4gbean=new Yunguan_G4JK_Basic4GBean();
//		String tuple1=new String("2016/8/15 9:37:26.498	460003021211258		867620029196398	9313	31054593		1952	1645						CMNET.MNC000.MCC460.GPRS	1	169274806	3747870114	45534	80	0	24142	24142");
//		//String tuple1=new String("2016/8/15 9:35:50.260	460009202282599	/core?t=2&chipid=&tm=120&ra=2&ishcdn=0&pf=2&p=22&p1=221&p2=2211&sdktp=1&c1=&r=522214200&aid=522214200&u=F7A389AC%2D75F6%2D41A6%2D9226%2D072ADFCDABC5&pu=1306122429&v=7%2E6&krv=3%2E2%2E3&dt=&hu=3&rn=1471224948&islocal=2&as=2efb1cf06a72e56e8922fdbf9862a198&ve=694bc6435902d7fc91134763b7a2abf1&pe=&vfrm=&chl=&hcdnv=10.10.6.17&tpcd=1&isdrm=1&ht=0&ptid=02032001010000000000&mod=&lvbck=1011&nettype=2	352021064139124	10105	49114626		811	313	55	QYPlayer/iOS/3.2.3				CMNET.MNC000.MCC460.GPRS	1	173589659	3747870157	50172	80	0	24142	24142\n");
//		
//		String attr_value=new String("");
//		String[] fields_set=tuple1.split("\t");//按照TAB作为间隔划分字段
//		//字段1，获取日期并做格式转换
//		if(fields_set.length>0){
//			attr_value=TimeFormatter.Tra_realdate2(fields_set[0]);
//			g4jkbasic4gbean.setStarttime(attr_value);
//		}
//		
//		//字段2，获取IMSI
//		if(fields_set.length>1){
//			if(fields_set[1].length()==15)g4jkbasic4gbean.setImsi(fields_set[1]);
//		}
//		
//		//字段3，获取url
//		if(fields_set.length>2){
//			if(fields_set[2].length()>0)g4jkbasic4gbean.setUrl(fields_set[2]);
//		}
//		
//		//字段4，获取IMEI
//		if(fields_set.length>3){
//			if(fields_set[3].length()==15)g4jkbasic4gbean.setImei(fields_set[3]);
//		}
//		
//		//字段5，获取基站TAC码，至少4位
//		if(fields_set.length>4){
//			if(fields_set[4].length()>=4)g4jkbasic4gbean.setTac(fields_set[4]);
//		}
//		
//		//字段6，获取基站小区码cell_id，或者填写数字，或者填写none
//		if(fields_set.length>5){
//			if(fields_set[5].length()>0)g4jkbasic4gbean.setCid(fields_set[5]);
//		}
//		
//		//字段7，事件类型
//		if(fields_set.length>6){
//			if(fields_set[6].length()>0)g4jkbasic4gbean.setEvent_type(fields_set[6]);
//		}
//		
//		//字段8，上行流量
//		if(fields_set.length>7){
//			if(fields_set[7].length()>0)g4jkbasic4gbean.setUl_data(fields_set[7]);
//		}
//		
//		//字段9，下行流量
//		if(fields_set.length>8){
//			if(fields_set[8].length()>0)g4jkbasic4gbean.setDl_data(fields_set[8]);
//		}
//		
//		//字段10，会话时长
//		if(fields_set.length>9){
//			if(fields_set[9].length()>0)g4jkbasic4gbean.setDelay(fields_set[9]);
//		}
//		
//		//字段11，终端型号
//		if(fields_set.length>10){
//			if(fields_set[10].length()>0)g4jkbasic4gbean.setUser_agent(fields_set[10]);
//		}
//		
//		//字段12，是否自有业务
//		if(fields_set.length>11){
//			if(fields_set[11].length()>0)g4jkbasic4gbean.setGmcc_bus_ind(fields_set[11]);
//		}
//		
//		//字段13，手机品牌
//		if(fields_set.length>12){
//			if(fields_set[12].length()>0)g4jkbasic4gbean.setPhone_brand(fields_set[12]);
//		}
//		
//		//字段14，手机型号
//		if(fields_set.length>13){
//			if(fields_set[13].length()>0)g4jkbasic4gbean.setPhone_type(fields_set[13]);
//		}
//		
//		//字段15，接入点
//		if(fields_set.length>14){
//			if(fields_set[14].length()>0)g4jkbasic4gbean.setApn(fields_set[14]);
//		}
//		
//		//字段16，协议类型
//		if(fields_set.length>15){
//			if(fields_set[15].length()>0)g4jkbasic4gbean.setPro_type(fields_set[15]);
//		}
//		
//		//字段17，业务源ip
//		if(fields_set.length>16){
//			if(fields_set[16].length()>0)g4jkbasic4gbean.setUser_ip(fields_set[16]);
//		}
//		
//		//字段18，业务目标ip
//		if(fields_set.length>17){
//			if(fields_set[17].length()>0)g4jkbasic4gbean.setApp_server_ip(fields_set[17]);
//		}
//		
//		//字段19，业务源端口
//		if(fields_set.length>18){
//			if(fields_set[18].length()>0)g4jkbasic4gbean.setUser_port(fields_set[18]);
//		}
//		
//		//字段20，业务目的端口
//		if(fields_set.length>19){
//			if(fields_set[19].length()>0)g4jkbasic4gbean.setApp_server_port(fields_set[19]);
//		}
//
//		//字段21，业务大类
//		if(fields_set.length>20){
//			if(fields_set[20].length()>0)g4jkbasic4gbean.setApptype(fields_set[20]);
//		}
//		
//		//字段22，app小类
//		if(fields_set.length>21){
//			if(fields_set[21].length()>0)g4jkbasic4gbean.setIntappid(fields_set[21]);
//		}
//		
//		//字段23，业务小类
//		if(fields_set.length>22){
//			if(fields_set[22].length()>0)g4jkbasic4gbean.setIntsid(fields_set[22]);
//		}
//		
//		System.out.println(g4jkbasic4gbean.getStarttime());
//		System.out.println(g4jkbasic4gbean.getImsi());
//		System.out.println(g4jkbasic4gbean.getUrl());
//		System.out.println(g4jkbasic4gbean.getImei());
//		System.out.println(g4jkbasic4gbean.getTac());
//		System.out.println(g4jkbasic4gbean.getCid());
//		System.out.println(g4jkbasic4gbean.getEvent_type());
//		System.out.println(g4jkbasic4gbean.getUl_data());
//		System.out.println(g4jkbasic4gbean.getDl_data());
//		System.out.println(g4jkbasic4gbean.getDelay());
//		System.out.println(g4jkbasic4gbean.getUser_agent());
//		System.out.println(g4jkbasic4gbean.getGmcc_bus_ind());
//		System.out.println(g4jkbasic4gbean.getPhone_brand());
//		System.out.println(g4jkbasic4gbean.getPhone_type());
//		System.out.println(g4jkbasic4gbean.getApn());
//		System.out.println(g4jkbasic4gbean.getPro_type());
//		System.out.println(g4jkbasic4gbean.getUser_ip());
//		System.out.println(g4jkbasic4gbean.getApp_server_ip());
//		System.out.println(g4jkbasic4gbean.getUser_port());
//		System.out.println(g4jkbasic4gbean.getApp_server_port());
//		System.out.println(g4jkbasic4gbean.getApptype());
//		System.out.println(g4jkbasic4gbean.getIntappid());
//		System.out.println(g4jkbasic4gbean.getIntsid());

		//测试自动去掉多余的0
//		String testnum="000123";
//		int testint=Integer.valueOf(testnum);
//		System.out.println(String.valueOf(testint));
		
		//测试中文提取与统计长度
		try {
			String url="/hm.gif?cc=0&ck=1&cl=24-bit&ds=720x1280&et=0&ja=0&ln=zh-CN&lo=0&lt=1452054716&nv=1&rnd=1052692563&si=cdf7b63861fb9e5aa11b9f3859918fac&st=3&su=http%3A%2F%2Fcommon.diditaxi.com.cn%2Fgeneral%2FwebEntry%3Fwx%3Dtrue%26code%3D01169203ae60e01df8320537bd1ecb5o%26state%3D123&v=1.1.22&lv=3&tt=%E7%B2%89%E8%89%B2%E6%98%9F%E6%9C%9F%E4%B8%89";
			//测试url串1："/hm.gif?cc=0&ck=1&cl=24-bit&ds=720x1280&et=0&ja=0&ln=zh-CN&lo=0&lt=1452054716&nv=1&rnd=1052692563&si=cdf7b63861fb9e5aa11b9f3859918fac&st=3&su=http%3A%2F%2Fcommon.diditaxi.com.cn%2Fgeneral%2FwebEntry%3Fwx%3Dtrue%26code%3D01169203ae60e01df8320537bd1ecb5o%26state%3D123&v=1.1.22&lv=3&tt=%E7%B2%89%E8%89%B2%E6%98%9F%E6%9C%9F%E4%B8%89";
			//测试url串2："/025A84D404EA4E5834979B8A356DB4FA53340640/%5Bwww.qiqipu.com%5D%CB%DE%B5%D0.BD1024%B8%DF%C7%E5%D6%D0%D3%A2%CB%AB%D7%D6.mp4";
			//测试url串3："/17.gif?n_try=0&t_ani=554&t_liv=6379&t_load=-9508&etype=slide&page=detail&app=mediacy&browser=baidubox&phoneid=50206&tanet=3&taspeed=287&logid=11218310436162814452&os=&wd=%E5%B0%91%E5%A6%87%E8%81%8A%E5%BE%AE%E4%BF%A1%E5%8F%91%E6%AF%94%E7%9A%84%E5%9B%BE%E7%89%87&sid=2c3ec78c910929ab174688703d173c16754ac96a&sampid=50&spat=1-0-nj02-&group="
			//url=java.net.URLDecoder.decode(url, "utf-8");
			String fis= java.net.URLDecoder.decode(url, "gb2312");
			String sec = new String(fis.getBytes("gb2312"), "gb2312");
			String res=null;
			
			if (fis.equals(sec)==true)
				url=fis;
	        else
	        	url= java.net.URLDecoder.decode(url, "utf-8");

			//提取其中的中文，分词，编解码，均测试通过
			String reg = "[^\u4e00-\u9fa5]";  
			url = url.replaceAll(reg, "");
			System.out.println(url);
			List<Word> words = null;
			if(url!=null&&url.length()>2){
				if(url.length()>6){
					//1.对中文做分词，移除停用词，采用words库，详细参考pom的配置
					words=WordSegmenter.seg(url);
					//2.对热词做md5转码，然后存入集合中，同时每个字符做计数
					if(words!=null&&words.isEmpty()==false){
						for(int i=0;i<words.size();i++)
						{
							res=words.get(i).getText();
							res=flowtest.Base32Encode(res);
							System.out.println(res);
							res=flowtest.Base32Decode(res);
							System.out.println(res);
						}
					}
				}else{
					res=url;
					res=flowtest.Base32Encode(res);
					System.out.println(res);
					res=flowtest.Base32Decode(res);
					System.out.println(res);
				}
			}
			//System.out.println(url.length());
		} catch (Exception ex) {
			//logger.info("Yunguan_G4JKtest execute error: "+ex.getMessage());
		}
	}
	
	private static final String base32Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
	
	public static String Base32Encode(String chwd){
		String res=null;
		try{
	        byte bytes[] = chwd.getBytes();//获取中文转成的bytes
	        int i = 0, index = 0, digit = 0;  
	        int currByte, nextByte;  
	        StringBuffer base32 = new StringBuffer((bytes.length + 7) * 8 / 5);  
	        while (i < bytes.length) {  
	            currByte = (bytes[i] >= 0) ? bytes[i] : (bytes[i] + 256); // unsign  
	            /* Is the current digit going to span a byte boundary? */  
	            if (index > 3) {  
	                if ((i + 1) < bytes.length) {  
	                    nextByte = (bytes[i + 1] >= 0) ? bytes[i + 1]  
	                            : (bytes[i + 1] + 256);  
	                } else {  
	                    nextByte = 0;  
	                }  
	                digit = currByte & (0xFF >> index);  
	                index = (index + 5) % 8;  
	                digit <<= index;  
	                digit |= nextByte >> (8 - index);  
	                i++;  
	            } else {  
	                digit = (currByte >> (8 - (index + 5))) & 0x1F;  
	                index = (index + 5) % 8;  
	                if (index == 0)  
	                    i++;  
	            }  
	            base32.append(base32Chars.charAt(digit));  
	        }  
	        res=base32.toString();
		}catch(Exception ex){
			//LOG.info(" Thread md5str16 crashes: "+ex.getMessage());
			return null;
		}
		return res;
	}
	
	private static final int[] base32Lookup = { 0xFF, 0xFF, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, // '0', '1', '2', '3', '4', '5', '6', '7'  
	  0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // '8', '9', ':', ';', '<', '=',  '>', '?'  
	  0xFF, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, // '@', 'A', 'B', 'C', 'D', 'E', 'F', 'G'  
	  0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, // 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O'  
	  0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, // 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W'  
	  0x17, 0x18, 0x19, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 'X', 'Y', 'Z', '[', '', ']', '^', '_'  
	  0xFF, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, // '`', 'a', 'b', 'c', 'd', 'e', 'f', 'g'  
	  0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, // 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o'  
	  0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, // 'p', 'q', 'r', 's', 't', 'u', 'v', 'w'  
	  0x17, 0x18, 0x19, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF // 'x', 'y', 'z', '{', '|', '}', '~', 'DEL'  
	}; 
	
	public static String Base32Decode(String base32) {  
        int i, index, lookup, offset, digit;  
        byte[] bytes = new byte[base32.length() * 5 / 8];  
        for (i = 0, index = 0, offset = 0; i < base32.length(); i++) {  
            lookup = base32.charAt(i) - '0';  
            /* Skip chars outside the lookup table */  
            if (lookup < 0 || lookup >= base32Lookup.length) {  
                continue;  
            }  
            digit = base32Lookup[lookup];  
            /* If this digit is not in the table, ignore it */  
            if (digit == 0xFF) {  
                continue;  
            }  
            if (index <= 3) {  
                index = (index + 5) % 8;  
                if (index == 0) {  
                    bytes[offset] |= digit;  
                    offset++;  
                    if (offset >= bytes.length)  
                        break;  
                } else {  
                    bytes[offset] |= digit << (8 - index);  
                }  
            } else {  
                index = (index + 5) % 8;  
                bytes[offset] |= (digit >>> index);  
                offset++;  
                if (offset >= bytes.length) {  
                    break;  
                }  
                bytes[offset] |= digit << (8 - index);  
            }  
        }  
        return new String(bytes);
    } 

}
