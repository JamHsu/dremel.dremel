package dremel.dataset;

public interface ColumnWriter {

	public void addIntDataTriple(int data, boolean isNull, byte repLevel, byte defLevel);
	public void setNullValue(byte repLevel, byte defLevel);
	public void close();
}