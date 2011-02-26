package dremel.dataset.impl;

public class ColumnFileSet
{
	private String baseFileName;
	private String dataFileName;
	private String defFileName;
	private String repFileName;
	
	public ColumnFileSet(String forBaseFileName)
	{
		baseFileName = forBaseFileName;
		
		dataFileName = baseFileName+"_data.dremel";
		repFileName = baseFileName+"_ref.dremel";
		defFileName = baseFileName+"_def.dremel";
	}
	
	public ColumnFileSet(ColumnFileSet fileSet) {
		// TODO Auto-generated constructor stub
	}

	public String getDataFileName()
	{
		return dataFileName;
	}
	public String getDefFileName()
	{
		return defFileName;
	}
	public String getRepFileName()
	{
		return repFileName;
	}
	public String getBaseFileName()
	{
		return baseFileName;
	}
			
}