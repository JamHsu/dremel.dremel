package dremel.common;

import static dremel.common.Drec.getSchema;

import java.io.IOException;

import org.apache.avro.Schema;

/**
 * This class return predefined data strucures upon need. It can be vied as some kind of singleton container
 * @author David.Gruzman
 *
 */
public class StaticStructures {
	
	/**
	 * Return Avro schema for the Dremel file. It is always constant
	 * @return
	 */
	public static Schema getDrecSchema()
	{		
		return Drec.getSchema("DrecSchema.avpr.json");
	}

}
