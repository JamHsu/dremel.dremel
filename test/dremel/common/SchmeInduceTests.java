/**
 * Copyright 2011, BigDataCraft.com
 * Author : David Gruzman
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.Ope
 */

package dremel.common;

import static dremel.common.Drec.getSchema;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class SchmeInduceTests {
	
	 private static Log log = LogFactory.getLog(SchmeInduceTests.class);
	
	 public void schemaTestAvroSchemaConversions(String schemaFileName)
		{		
			
			 Schema inputSchema = getSchema(schemaFileName);
			 SchemaTree schemaTree = SchemaTree.createFromAvroSchema(inputSchema);
			 Schema outputSchema = schemaTree.getAsAvroSchema();
			 //System.out.println("output schema is "+outputSchema);
			 assertEquals(inputSchema, outputSchema);	 
		}
	 
	 @Test
	public void schemaTestAvroSchemaConversions()
	{		 	 		
		 schemaTestAvroSchemaConversions("q16_ResultSchema.avpr.json");
		 schemaTestAvroSchemaConversions("q17_ResultSchema.avpr.json");
	}

	 
	 public void simpleProjectionTest(String queryFilebaseName)
		{
			// load schema
			Schema inputSchema = getSchema("OrecSchema.avpr.json");
			// load expected schema
			Schema expectedResultSchema =  getSchema(queryFilebaseName+"_ResultSchema.avpr.json");
			// load query
			File queryFile = Drec.getFile(queryFilebaseName+".bql");
			String query;
			try {
				query = FileUtils.readFileToString(queryFile);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			// induce schema		
			Schema inducedSchema = InferSchemaAlgorithm.inferSchema(inputSchema, query);
			// compare expected and induced schema
			//System.out.println(inducedSchema.toString());
			assertTrue(inducedSchema.equals(expectedResultSchema));

		}

	 
	@Test
	public void simpleProjectionTest()
	{
		simpleProjectionTest("q16");
		simpleProjectionTest("q17");
	}

}
