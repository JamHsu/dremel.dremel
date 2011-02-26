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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

/**
 * This class can be see as wrapper around the AvroSchema, which is a place to add functionality we are missing.
 * The main intent to create it was a lack of mutability in the Avro Schema, which is needed for the incremental schema inference
 * Later we might add support for the Dremel (or actually protobuf) attributes like Optional. 
 * @author David.Gruzman
 *
 */
public class SchemaTree
{
	
	enum NodeType {RECORD,ARRAY,PRIMIIVE};
	enum PrimitiveType {INT,BOOLEAN, LONG, FLOAT, DOUBLE, STRING, NOT_EXISTING_TYPE};
	
	public final static String ARRAY_PREFIX = "ARRAY_OF_";
	
	// valid if type is record
	private Map<String, SchemaTree> fields = null;
	// valid if type is array
	private SchemaTree arratElementType = null;
	// valid if type is primitive
	private PrimitiveType primitiveType = PrimitiveType.NOT_EXISTING_TYPE;
			
	private String name; // name of this node. In case of array - it automatically get the name of its element type. if name still not assigned it should have value ARRAY_NAME
	
	NodeType type;
	

	public SchemaTree(NodeType nodeType)
	{			
		type = nodeType;			
	}
	
	public static SchemaTree createArray(SchemaTree elementType)
	{
		SchemaTree newSchema = new SchemaTree(NodeType.ARRAY);
		newSchema.arratElementType = elementType;
		newSchema.name = getArrayName(elementType.getName());			
		return newSchema;			
	}
	
	public static SchemaTree createPrimitive(String forName, PrimitiveType forPrimitiveType)
	{
		SchemaTree newSchema = new SchemaTree(NodeType.PRIMIIVE);
		newSchema.primitiveType = forPrimitiveType;
		newSchema.name = forName;
		return newSchema;
		
	}
	
	public static SchemaTree createPrimitive(String forName, Schema.Type primitiveAvroType)
	{
		PrimitiveType primitveType = AvroToSchemaTreePrimitive(primitiveAvroType);
		return createPrimitive(forName, primitveType);		
	}
			
	public static SchemaTree createRecord(String forName)
	{				
		SchemaTree newSchema = new SchemaTree(NodeType.RECORD);
		newSchema.name = forName;
		newSchema.fields = new HashMap<String, SchemaTree>();
		
		return newSchema;			
	}
	
	public static String getArrayName(String elementName) {			
		return ARRAY_PREFIX+elementName;
	}

	private String getName() {
		return name;
	}

	public NodeType getType()
	{
		return type;
	}
				
			
	public void addField(SchemaTree field)
	{
		addField(field, field.getName());			
	}
	
	public void addField(SchemaTree field, String fieldName)
	{			
		if(fields.containsKey(fieldName))
		{
			throw new RuntimeException("Field "+fieldName+ " is duplicated in the record named "+name);
		}
		fields.put(fieldName, field);
	}
			
	/**
	 * Export schema tree as AVRO schema
	 * @return 
	 */
	public Schema getAsAvroSchema()
	{
		Schema result = null;
		
		switch (type)
		{
			case PRIMIIVE:
				result = getPrimitiveAvroSchema();
				break;
			case ARRAY:
				result = getArrayAvroSchema();
				break;
			case RECORD:					
				result = getRecordAvroSchema();
				break;
			default:
				throw new RuntimeException("Unexpected node type "+ type);
		}
					
		return result;

	}
	
	private Schema getRecordAvroSchema() {
		assert(type == NodeType.RECORD);
		Schema result = Schema.createRecord(name, null, null, false);
		List<Schema.Field> schemaFields = new LinkedList<Schema.Field>(); 
		for(String fieldName : fields.keySet())
		{
			Schema fieldSchema = fields.get(fieldName).getAsAvroSchema();
			Schema.Field nextField = new Schema.Field(fieldName, fieldSchema, null, null);
			schemaFields.add(nextField);
		}
		
		result.setFields(schemaFields);
		return result;
	}

	private Schema getArrayAvroSchema() {
		assert(type == NodeType.ARRAY);
		Schema elementSchema = arratElementType.getAsAvroSchema();
		Schema result = Schema.createArray(elementSchema);			
		return result;
	}

	private Schema getPrimitiveAvroSchema() {
		
		assert(type == NodeType.PRIMIIVE);
		Schema.Type avroPrimiveType = Schema.Type.valueOf(primitiveType.toString());
		 
		Schema result = Schema.create(avroPrimiveType);
		
		return result;			
	}

	private static PrimitiveType AvroToSchemaTreePrimitive(Schema.Type avroType)
	{
		return PrimitiveType.valueOf(avroType.toString().toUpperCase());
	}
	

	public static SchemaTree createFromAvroSchema(Schema inputSchema)
	{
		return createFromAvroSchema(inputSchema, inputSchema.getName());
	}
	
	public static SchemaTree createFromAvroSchema(Schema inputSchema, String schemaName)
		{
				
		SchemaTree result = null;
		
		if(inputSchema.getType() == Schema.Type.ARRAY)
		{
		  	 SchemaTree elementType = createFromAvroSchema(inputSchema.getElementType(), schemaName);
		  	 result = SchemaTree.createArray(elementType);		  	 
		}
		
		if(inputSchema.getType() == Schema.Type.RECORD)
		{
			
			result = SchemaTree.createRecord(inputSchema.getName());				
			List<Schema.Field> fields = inputSchema.getFields();
			for(Schema.Field nextField : fields)
			{
				SchemaTree fieldSchemaTree = createFromAvroSchema(nextField.schema(), nextField.name());
				result.addField(fieldSchemaTree, nextField.name());
			}
		}
		
		if(IsPrimitiveSchema(inputSchema))
		{
			//result = SchemaTree.createPrimitive(inputSchema.getName(), inputSchema.getType());
			result = SchemaTree.createPrimitive(schemaName, inputSchema.getType());
			
		}

		return result;
	}
	
	

	public String toString()
	{			
		return toStringRecursive(0);
	}
	
	public String toStringRecursive(int identation)
	{
		identation++;
		
		StringBuffer identBuffer = new StringBuffer();
		for(int i=0; i< identation; i++)
		{
			identBuffer.append("      ");
		}
		
		String ident = identBuffer.toString();
		
		String result = null;
		switch (type)
		{
			case PRIMIIVE:
				result = name+ "type: " + type.toString();
				break;
			case ARRAY:
				result = name+ " of elements "+ arratElementType.toStringRecursive(identation);
				break;
			case RECORD:
				result = " record " + name;
				for(String fieldName : fields.keySet())
				{
					result += " field "+fieldName + fields.get(fieldName)+fields.get(fieldName).toStringRecursive(identation);
				}
				break;
			default:
				throw new RuntimeException("Unexpected node type "+ type);
		}

		return "\n"+ident+result;
	}

	public void mergeWithTree(SchemaTree pathTree) {
		
		boolean merged = false;
		// this and pathTree are arrays - merge elementTypes
		if(pathTree.getType()==NodeType.ARRAY && this.getType()==NodeType.ARRAY)
		{
			assert(pathTree.getName().equals(this.getName()));
			arratElementType.mergeWithTree(pathTree.arratElementType);
			merged = true;
		}
		// this and pathTree are records - merge fields
		if(pathTree.getType()==NodeType.RECORD && this.getType()==NodeType.RECORD)
		{
			assert(pathTree.getName().equals(this.getName()));
			
			mergeFields(pathTree.fields);
			merged = true;
		}
		if(pathTree.getType()==NodeType.PRIMIIVE && this.getType()==NodeType.PRIMIIVE)
		{ // do nothing - if we have two identical primitive schemas
			assert(pathTree.getName().equals(this.getName()));
			assert(pathTree.primitiveType == this.primitiveType);
			merged=true;
		}
		 
	   if(!merged)
	   {
		   throw new RuntimeException("Trees are not compatible first: "+this + " \n second is "+ pathTree);
	   }
		
	}

	private void mergeFields(
			Map<String, SchemaTree> sourceFields) {
		for(String fieldName : sourceFields.keySet())
		{
			if(!fields.containsKey(fieldName))
			{
				addField( sourceFields.get(fieldName), fieldName);
			}else
			{ // field already exists, so it should be merged
				
				SchemaTree thisField = fields.get(fieldName);
				SchemaTree sourceField = sourceFields.get(fieldName);
				thisField.mergeWithTree(sourceField);
				
			}
		}
	}

	/**
	 * Make subset of this schema, which contains only the given column 
	 * @param namePartsList
	 * @return
	 */
	public void makeProjection(List<String> namePartsList) {

		String head = namePartsList.get(0);
		
		if(type == NodeType.ARRAY)
		{				
			if(!head.equals(arratElementType.getName()))
			{
				throw new RuntimeException("Array type not match "+head + " and "+arratElementType.getName());
			}
			if(!(arratElementType.getType() == NodeType.PRIMIIVE))
			{
				arratElementType.makeProjection(getTail(namePartsList));
			}else
			{
				assert(namePartsList.size()==1);
			}
		}
		if(type == NodeType.RECORD)
		{					
			List<String> fieldsToDelete = new LinkedList<String>();
			// delete all fields but the named after head
			for(String fieldName : fields.keySet())
			{
				if(!fieldName.equals(head)){
					fieldsToDelete.add(fieldName);
				}
			}
			for(String fieldToDelete :  fieldsToDelete)
			{
				fields.remove(fieldToDelete);
			}
			
			if(fields.size() != 1)
			{
				throw new RuntimeException("Field not found "+head);
			}
			

			SchemaTree fieldLeft = fields.get(head);
			if(fieldLeft.getType() == NodeType.ARRAY)
			{
				fieldLeft.makeProjection(namePartsList);
			}
			else
			{
				List<String> tail = getTail(namePartsList);
				fieldLeft.makeProjection(tail);
			}
			
		}
		
		if(type == NodeType.PRIMIIVE)
		{
			assert(namePartsList.size() == 1);
			if(name != namePartsList.get(0))
			{
				throw new RuntimeException(" name mismatch "+ namePartsList.get(0) + " instead of "+namePartsList.get(0));
			}
			
		}
	}
  
	/**
	 * Checks if schema represents primitive type - int or string
	 * @param currentSchema
	 * @return
	 */
	private static boolean IsPrimitiveSchema(Schema currentSchema) {
		// TODO - to check if all primitive types are 
		return currentSchema.getType()==Schema.Type.INT || currentSchema.getType()==Schema.Type.BOOLEAN || currentSchema.getType()==Schema.Type.STRING || currentSchema.getType()==Schema.Type.FLOAT || currentSchema.getType()==Schema.Type.DOUBLE || currentSchema.getType()==Schema.Type.LONG;	
	}
	
	private static List<String> getTail(List<String> inputList) {
		boolean isFirst = true;;
		List<String> result = new LinkedList<String>();
		for(String nextElement : inputList)
		{
			if(!isFirst)
			{
				result.add(nextElement);
			}
			isFirst=false;
		}
		return result;
	}
	
}

