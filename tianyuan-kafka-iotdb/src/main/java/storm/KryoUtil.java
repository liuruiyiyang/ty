package storm;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
//import org.junit.Assert;
//import org.junit.Test;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;



/**
 * Kryo序列化工具类
 * @author yangtao
 */
public class KryoUtil {

	/**
	 * 序列化对象
	 * @param obj - 对象
	 * @return 字节流
	 */
	public static ByteBuffer serialize(Object obj){
		return ByteBuffer.wrap(serialize2byte(obj));
	}
	
	public static byte[] serialize2byte(Object obj){
		Kryo kryo = new Kryo();
		//未注册类需进行kryo注册
//		kryo.register(obj.getClass());
		int bufferSize = 100;
		Output output = new Output(bufferSize,-1); //maxBufferSize为-1时取Integer类型的最大值
		boolean writeFlag = false;
		while(!writeFlag){
			try{
				kryo.writeObject(output, obj);
				writeFlag = true;
			}catch(KryoException e){
				e.printStackTrace();
				bufferSize *= 10;
				output = new Output(bufferSize,-1);//maxBufferSize为-1时取Integer类型的最大值
			}
		}
		output.close();
		return output.toBytes();
	}
	
	/**
	 * 序列化对象
	 * @param obj - 对象
	 * @return 字节流
	 */
	public static <T> T deserialize(ByteBuffer byteBuffer,Class<T> clz){
		Input input = new Input(byteBuffer.array());
		return new Kryo().readObject(input, clz);
	}
	
	public static <T> T deserialize(byte[] byteData,Class<T> clz){
		Input input = new Input(byteData);
		return new Kryo().readObject(input, clz);
	}

//	/**
//	 * 替换PersistentBag,防止反序列化时进行懒加载
//	 * @param obj - 对象
//	 * @return 替换了PersistentBag的对象（PersistentBag类型替换为ArrayList）
//	 */
//	@SuppressWarnings({ "unchecked" })
//	public static void replacePersistentBag(Object obj){
//		if(obj instanceof Device){
//			if(((Device)obj).getDeviceSeries()!=null&&((Device)obj).getDeviceSeries().getSynConfigList()!=null){
//				for(SynConfig sc:((Device)obj).getDeviceSeries().getSynConfigList()){
//					if(sc.getSynComponent()!=null)
//						sc.getSynComponent().settPara2TreatList(new ArrayList(sc.getSynComponent().gettPara2TreatList()));
//				}
//				List<SynConfig> lst = new ArrayList<SynConfig>(((Device)obj).getDeviceSeries().getSynConfigList());
//				((Device)obj).getDeviceSeries().setSynConfigList(lst);
//			}
//		}else if(obj instanceof TemplatePara){
//			if(((TemplatePara)obj).gettPara2TreatList()!=null){
//				List<TPara2Treat> lst = new ArrayList<TPara2Treat>(((TemplatePara)obj).gettPara2TreatList());
//				((TemplatePara)obj).settPara2TreatList(lst);
//			}
//		}else if(obj instanceof Template){
//			if(((Template)obj).getTem2TemParaList()!=null){
//				List<Tem2TemPara> lst = new ArrayList<Tem2TemPara>(((Template)obj).getTem2TemParaList());
//				((Template)obj).setTem2TemParaList(lst);
//			}
//			if(((Template)obj).getTemp2TreatList()!=null){
//				List<Temp2Treat> lst2 = new ArrayList<Temp2Treat>(((Template)obj).getTemp2TreatList());
//				((Template)obj).setTemp2TreatList(lst2);
//			}
//			if(((Template)obj).getTem2TemParaList()!=null){
//				for(Tem2TemPara tt: ((Template)obj).getTem2TemParaList()){
//					if(tt.getPk()!=null&&tt.getPk().templatePara!=null&&tt.getPk().templatePara.gettPara2TreatList()!=null)
//						tt.getPk().templatePara.settPara2TreatList(new ArrayList<TPara2Treat>(tt.getPk().templatePara.gettPara2TreatList()));
//				}
//			}
//		}else if(obj instanceof Protocol){
//			if(((Protocol)obj).getPro2TempList()!=null){
//				for(Pro2Temp p2t :((Protocol)obj).getPro2TempList()){
//					if(p2t.getPk()!=null&&p2t.getPk().template!=null&&p2t.getPk().template.getTem2TemParaList()!=null){
//						for(Tem2TemPara t2tp :p2t.getPk().template.getTem2TemParaList()){
//							if(t2tp.getPk()!=null&&t2tp.getPk().templatePara!=null)
//								t2tp.getPk().templatePara.settPara2TreatList(new ArrayList<TPara2Treat>(t2tp.getPk().templatePara.gettPara2TreatList()));
//						}
//						p2t.getPk().template.setTem2TemParaList(new ArrayList<Tem2TemPara>(p2t.getPk().template.getTem2TemParaList()));
//						p2t.getPk().template.setTemp2TreatList(new ArrayList<Temp2Treat>(p2t.getPk().template.getTemp2TreatList()));
//					}
//				}
//				List<Pro2Temp> lst = new ArrayList<Pro2Temp>(((Protocol)obj).getPro2TempList());
//				((Protocol)obj).setPro2TempList(lst);
//			}
//			if(((Protocol)obj).getPro2TempList()!=null){
//				for(Pro2Temp pt:((Protocol)obj).getPro2TempList()){
//					if(pt.getPk()!=null&&pt.getPk().template!=null&&pt.getPk().template.getCommonKey()!=null
//							&&pt.getPk().template.getCommonKey().gettPara2TreatList()!=null){
//						List<TPara2Treat> lst = new ArrayList<TPara2Treat>(pt.getPk().template.getCommonKey().gettPara2TreatList());
//						pt.getPk().template.getCommonKey().settPara2TreatList(lst);
//						KryoUtil.replacePersistentBag(pt.getPk().template);
//					}
//				}
//				List<Pro2Temp> lst = new ArrayList<Pro2Temp>(((Protocol)obj).getPro2TempList());
//				((Protocol)obj).setPro2TempList(lst);
//			}
//		}else if(obj instanceof SubPlan){ 
//			//订阅方案只在订阅判断中用到，仅用到其中的beginTime、endTiem和parameterList的parameterId
//			if(((SubPlan)obj).getParamaterList()!=null){
////				((SubPlan)obj).setCreateTime(null);
////				((SubPlan)obj).setCreator(null);
////				((SubPlan)obj).setPushParam(null);
////				((SubPlan)obj).setPushType(null);
////				((SubPlan)obj).setState(null);
//				for(TemplatePara tp:((SubPlan)obj).getParamaterList()){
////					if(tp.gettPara2TreatList()!=null){
////						((TemplatePara)tp).settPara2TreatList(new ArrayList<TPara2Treat>(tp.gettPara2TreatList()));
////					}
//					tp.settPara2TreatList(null);
//					tp.setLength(null);
//					tp.setParameterID(null);
//					tp.setParameterType(null);
//					tp.setParameterUnit(null);
//					tp.setParameterName(null);
//				}
//				((SubPlan)obj).setParamaterList(new ArrayList<TemplatePara>(((SubPlan)obj).getParamaterList()));
//			}
//		}
////		else if(obj instanceof SynBom){
////			if(((SynBom)obj).getSyncBomItemList()!=null){
////				List lst = new ArrayList(((SynBom)obj).getSyncBomItemList());
////				((SynBom)obj).setSyncBomItemList(lst);
////			}
////		}
//		else if(obj instanceof RSCTask)
//		{
//			if(((RSCTask)obj).getTask2Config()!=null)
//			{
//				List<Task2Config> list=new ArrayList<Task2Config>(((RSCTask)obj).getTask2Config());
//				((RSCTask)obj).setTask2Config(list);
//			}
//			if(((RSCTask)obj).getTask2Device()!=null)
//			{
//				List<Task2Device> list=new ArrayList<Task2Device>(((RSCTask)obj).getTask2Device());
//				((RSCTask)obj).setTask2Device(list);
//			}
//			if(((RSCTask)obj).getProject()!=null)
//			{
//				RSCProject p=((RSCTask)obj).getProject();
//				if(p.getProject2Alg()!=null)
//				{
//					List<Project2Alg> list=new ArrayList<Project2Alg>(p.getProject2Alg());
//					p.setProject2Alg(list);
//				}
//				if(p.getProject2InterResult()!=null)
//				{
//					List<Project2IntermediateResult> list=new ArrayList<Project2IntermediateResult>(p.getProject2InterResult());
//					p.setProject2InterResult(list);
//				}
//				if(p.getProject2Config()!=null)
//				{
//					List<Project2Config> list=new ArrayList<Project2Config>(p.getProject2Config());
//					p.setProject2Config(list);
//					for(Project2Config each:p.getProject2Config())
//					{
//						if(each.getPk()!=null&&each.getPk().configuration!=null&&each.getPk().configuration.getConfig2Sql()!=null)
//							each.getPk().configuration.setConfig2Sql(new ArrayList<Config2RSCInputSql>(each.getPk().configuration.getConfig2Sql()));
//					}
//				}
//				if(p.getProject2KafkaOutput()!=null)
//				{
//					List<Project2RSCKafkaOutput> list=new ArrayList<Project2RSCKafkaOutput>(p.getProject2KafkaOutput());
//					p.setProject2KafkaOutput(list);
//				}
//				if(p.getProject2Parameter()!=null)
//				{
//					List<Project2Parameter> list=new ArrayList<Project2Parameter>(p.getProject2Parameter());
//					p.setProject2Parameter(list);
//					for(Project2Parameter each:p.getProject2Parameter())
//					{
//						if(each.getPk()!=null&&each.getPk().parameter!=null&&each.getPk().parameter.getParam2Kmx()!=null)
//							each.getPk().parameter.setParam2Kmx(new ArrayList<Param2KmxInput>(each.getPk().parameter.getParam2Kmx()));
//					}
//				}
//			}
//		}
//	}
//	
//	/**
//	 * 测试序列化与反序列化
//	 */
//	@Test
//	public void test(){
//		ChangeInfo ci = new ChangeInfo();
//		ci.setChangeType(ChangeInfo.CHANGETYPE_ADD);
//		ci.setResourceType("test");
//		ci.setResourceKey("key");
//		ci.setResourceItem("xxxxxxxxxxx");
//		ByteBuffer buffer = KryoUtil.serialize(ci);
//		System.out.println("序列化后长度为"+buffer.capacity()+"字节");
//		ChangeInfo rst = KryoUtil.deserialize(buffer, ChangeInfo.class);
//		Assert.assertEquals(rst.getChangeType(), ci.getChangeType());
//		Assert.assertEquals(rst.getResourceKey(), ci.getResourceKey());
//		Assert.assertEquals(rst.getResourceType(), ci.getResourceType());
//		Assert.assertEquals(rst.getResourceItem(), ci.getResourceItem());
//	}
	
}
