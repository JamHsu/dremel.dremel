package dremel.dataset.impl;

public class DataSetConstants {
	
	// we introduce magic numbers at the beginning of the open dremel column files, to prevent some human errors	
	public static final int REPETITION_COLUMN_MAGIC  = 0xEF01;
	public static final int DEFINITION_COLUMN_MAGIC  = 0xEF02;
	
	public static final int BYTE_COLUMN_MAGIC = 0xEF03;
	public static final int INT_COLUMN_MAGIC  = 0xEF04;

}
