package transfer.imp;

import java.util.HashMap;
import java.util.Map;


import com.sagittarius.bean.common.ValueType;
import transfer.ITransfer;

public class DataTypes {

	public final static int UNSIGNED_INT = 1, 
			SIGNED_INT = 2, 
			UNSIGNED_BYTE = 3, 
			SIGNED_BYTE = 4, 
			UNSIGNED_SHORT = 5, 
			SIGNED_SHORT = 6,
			UNSIGNED_LONG = 7,
			SIGNED_LONG = 8,
			FLOAT = 9,
			DOUBLE = 10,
			BOOLEAN = 11,
			CHAR = 12,
			STRING = 13,
			ENUM = 14,
			GEO = 15;
	
	private static Map<ValueType, ITransfer> transFuncs = new HashMap<ValueType, ITransfer>();
	
	static {
		transFuncs.put(ValueType.INT, new IntTransfer());
		transFuncs.put(ValueType.LONG, new LongTransfer());
		transFuncs.put(ValueType.FLOAT, new FloatTransfer());
		transFuncs.put(ValueType.DOUBLE, new DoubleTransfer());
		transFuncs.put(ValueType.BOOLEAN, new BooleanTransfer());
		transFuncs.put(ValueType.STRING, new StringTransfer());
	}
	
	public static ITransfer getTranFunc(ValueType valueType){
		
		return transFuncs.get(valueType);
	}
	
}
