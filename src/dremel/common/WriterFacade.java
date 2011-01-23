package dremel.common;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.avro.Schema;

import dremel.common.Drec.FileEncoding;

public interface WriterFacade {

	public abstract void importFromOrec(Schema orecSchema, File orecFile,
			FileEncoding orecEncoding, FileEncoding drecEncoding)
			throws IOException;

	public abstract void importFromDrec(Schema orecSourceSchema,
			File drecSourceFile, FileEncoding drecSourceEncoding,
			Schema orecDestSchema, FileEncoding drecDestEncoding)
			throws IOException, InvocationTargetException;

	/**
	 * 
	 * @param orecSourceSchema - logical schema of the input data 
	 * @param query - execution plan
	 * @param orecDestSchema - logical schema of the result
	 * @param drecDestEncoding - encoding of the result
	 * 
	 * @throws IOException
	 * @throws InvocationTargetException
	 */
	public abstract void importFromQuery(Schema orecSourceSchema, Query query,
			Schema orecDestSchema, FileEncoding drecDestEncoding)
			throws IOException, InvocationTargetException;

}