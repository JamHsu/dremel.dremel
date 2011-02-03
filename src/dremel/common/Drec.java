/**
 * Copyright 2010, Petascan Ltd.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.util.Utf8;

/**
*
* This class represents Dremel Nested Columnar Dataset and encapsulates all related functionality.
* Currently this class is heavily intervened with Avro. In fact it is fair to say it is a Dremel 
* extension to Avro framework.
* 
* Therefore in order to understand the code in this file one must first master Avro and 
* particularly generic access (GenericRecord and GenericArray and etc.) 
* 
* TODO: This class must be separated completely from Avro artifacts in future and support 
* for other serialization frameworks must be added. Particularly support for Protobuf framework
* is very wanted to be able to support examples from Dremel paper written in Protobuf format. 
*
* @author camuelg
*/
public final class Drec {
	enum ColumnType {
		INT, STRING
	}

	//SIngle element of fully qualified column name.  
	static final class SubName {

		final boolean isArray;

		final String name;

		SubName(String name, boolean isArray) {
			this.name = name;
			this.isArray = isArray;
		}
	}

	static final class Table {
		Table(String table) {};
	}

	//File encodings for Avro
	public static enum FileEncoding {
		JSON, BIN
	}

	//Dremel dataset functionality consists of few distinct sets called facades. For example
	//facade for scanning data, facade for writing data and etc. This class is a base class 
	//for such facades and contains common functionality among all facades
	static abstract class AbstractFacade {
		// TODO override toString() to ease debugging
		
		//This class correspond to the field in the Dremel paper example
		static abstract class Column {

			GenericRecord drecColumn;
			final Schema drecColumnSchema;
			boolean isArray;
			int maxDefenitionLevel;
			List<SubName> name; // name of this field and all enclosing fields
			GenericArray<GenericRecord> nameArray; // name of this field and all enclosing fields
			String nameString;
			GenericArray<Integer> rep; // repetition "sub column", references to
										// the drecColumn
			GenericArray<Integer> def; // definition "sub column", references to
										// the drecColumn

			// pointer to the current typed array. it is referanced or valInt or
			// valString.
			GenericArray<Object> val;

			// only one of the typed array is actually used
			GenericArray<Integer> valInt;
			GenericArray<Utf8> valString;
			ColumnType valType;

			Column(Schema drecColumnSchema) {
				this.drecColumnSchema = drecColumnSchema;
			}

			/**
			 * Calculate maximum definition level for the given column It is
			 * calculated as number of "array elements" in the path. According
			 * to the paper the definition level is number of undefined values
			 * in the path that are actually defined, So maximum possible value
			 * - it is number of values that are optional or repeated. Both of
			 * them are represented as arrays. Missing value will be represented
			 * as array of size 0.
			 * 
			 * @param namePath
			 *            - array of field definitions
			 * @return - max definition level
			 */
			protected static int getMaxDefinitionLevelFromColumnName(
					GenericArray<GenericRecord> namePath) {
				int resultLevel = -1;
				for (GenericRecord nextNamePart : namePath) {
					Boolean isArray = ((Integer) nextNamePart.get("IsArray")) == 1;
					if (isArray) {
						resultLevel++;
					}
				}

		//		assert(resultLevel>=0);

				return resultLevel;
			}

			@Override
			public String toString() {
				return null;// "AbstractFacade.Column. name: " + formatName +
			}

			@SuppressWarnings("unchecked")
			protected void identifyType() {
				String type = ((Utf8) drecColumn.get("ValType")).toString();
				if (type.equalsIgnoreCase("string")) {
					valType = ColumnType.STRING;
					val = (GenericArray<Object>) (Object) valString;
				} else if (type.equalsIgnoreCase("int")) {
					valType = ColumnType.INT;
					val = (GenericArray<Object>) (Object) valInt;
				} else
					throw new RuntimeException("Unsupported Drec type - "
							+ type);
			}
		};

		static abstract class ColumnTree<SCALAR, TREE> {
			final HashMap<String, TREE> children = new LinkedHashMap<String, TREE>();
			final int level;
			final boolean isArray;
			final HashMap<String, SCALAR> scalars = new LinkedHashMap<String, SCALAR>();

			ColumnTree(int level, boolean isArray) {
				this.level = level;
				this.isArray = isArray;
			};

			public Column getColumnByName(String columnName) {
				// TODO Auto-generated method stub
				Column res = getColumnByNameRecursive(columnName, this);
				return res;
			}

			private Column getColumnByNameRecursive(String columnName,
					ColumnTree<SCALAR, TREE> subtree) {

				/*
				 * for(String scalarName : subtree.scalars.keySet()) {
				 * System.out.println("scalar found: "+scalarName); }
				 * 
				 * System.out.println("");
				 */

				if (subtree.scalars.containsKey(columnName)) {
					return (Column) subtree.scalars.get(columnName);
				}

				for (String childSubtreeName : subtree.children.keySet()) {
					Column columnFound = getColumnByNameRecursive(columnName,
							(ColumnTree<SCALAR, TREE>) subtree.children
									.get(childSubtreeName));
					if (columnFound != null) {
						return columnFound;
					}
				}
				return null;
			}
					
		}

						
		@SuppressWarnings("serial")
		static final class ColumnList<COLUMN> extends ArrayList<COLUMN> {
		};

		final List<SubName> currentNamePath = new ArrayList<SubName>();

		transient int currentNestingLevel;

		File drecDataFile;
		Array<GenericRecord> drecData;

		Schema drecSchema;

		public AbstractFacade(Schema drecSchema, Array<GenericRecord> drecData) {
			currentNestingLevel = 0;
			this.drecSchema = drecSchema;
			this.drecData = drecData;
		}

		public AbstractFacade(File drecSchema, File drecDataFile)
				throws IOException {
			currentNestingLevel = 0;
			this.drecSchema = getSchema(drecSchema);
			this.drecDataFile = drecDataFile;
		}

		public AbstractFacade(Schema drecSchema, File drecDataFile) {
			currentNestingLevel = 0;
			this.drecSchema = drecSchema;
			this.drecDataFile = drecDataFile;
		}

		SubName peek() {
			assert (currentNamePath.size() > 0);
			return currentNamePath.get(currentNamePath.size() - 1);
		}

		void pop() {
			assert (currentNamePath.size() > 0);
			SubName last = currentNamePath.remove(currentNamePath.size() - 1);
			currentNestingLevel -= last.isArray ? 1 : 0;
		}

		void push(String subName, boolean isArray) {
			currentNamePath.add(new SubName(subName, isArray));
			currentNestingLevel += isArray ? 1 : 0;
		}

	}

	/**
	 * Encapsulates Dremel encoding defined in the paper. Contain the input data
	 * in form of the tree of column scanners.
	 * 
	 * @author David.Gruzman
	 * 
	 */
	public static class ScannerFacade extends AbstractFacade {

		// It is a tree of the ColumnScanner objects.
		ColumnScannersTree rtree = new ColumnScannersTree(0, true);
		// Schema of the whole dataset accessed through the ScannerFacade
		Schema dataSetSchema = null;

		public Schema getDataSetSchema() {
			return dataSetSchema;
		}

		// iterate over the tree of columns and print it.
		@Override
		public String toString() {
			StringBuilder result = new StringBuilder();
			prettyPrintRecursive(rtree, result);

			return result.toString();
		}

		public void prettyPrintRecursive(ColumnScannersTree subtree,
				StringBuilder result) {
			for (String columnName : subtree.scalars.keySet()) {
				result.append("Column in the level " + subtree.level + " is "
						+ columnName + "\n");
			}
			for (String child : subtree.children.keySet()) {
				ColumnScannersTree childTree = subtree.children.get(child);
				prettyPrintRecursive(childTree, result);
			}

		}

		final static public class ColumnScanner extends Column {

			private Iterator<Integer> defIterator;
			private Iterator<Integer> repIterator;
			private Iterator<Object> valIterator;

			Integer curDef;
			Integer curRep;
			public Object curVal;
			private Integer nextDef;
			Integer nextRep;
			private boolean nextRepDefExists;
			private Object nextVal;
			boolean isChanged;

			/**
			 * 
			 * @param drecColumnSchema
			 * @param drecColumn_
			 *            Dremel column with string or int value + columns of
			 *            definitions and repetition levels
			 */
			@SuppressWarnings("unchecked")
			ColumnScanner(Schema drecColumnSchema, GenericRecord drecColumn_) {
				super(drecColumnSchema);
				this.drecColumn = drecColumn_;

				// the construction order is critical, since the object is
				// not consistent throughout constructor body

				nameArray = ((GenericArray<GenericRecord>) drecColumn
						.get("Name"));
				valInt = ((GenericArray<Integer>) drecColumn.get("ValInt"));
				valString = ((GenericArray<Utf8>) drecColumn.get("ValString"));
				rep = ((GenericArray<Integer>) drecColumn.get("Rep"));
				def = ((GenericArray<Integer>) drecColumn.get("Def"));

				identifyType();

				repIterator = rep.iterator();
				defIterator = def.iterator();
				valIterator = val.iterator();

				identifyName();

				maxDefenitionLevel = getMaxDefinitionLevelFromColumnName(nameArray);

				// prime rep and def columns
				preloadNext();

			}

			void ensureConsistency(boolean isConsistent) {
				if (isConsistent)
					return;
				throw new RuntimeException(
						"Drec data or API use is inconsistent");
			}

			// internal method
			void preloadNext() {

				// check if rep and def is available
				boolean nextRepHasNext = repIterator.hasNext();
				boolean nextDefHasNext = defIterator.hasNext();
				ensureConsistency(nextRepHasNext == nextDefHasNext);
				nextRepDefExists = nextRepHasNext && nextDefHasNext;

				if (nextRepDefExists) {
					// retrieve rep and def
					nextRep = repIterator.next();
					nextDef = defIterator.next();
					ensureConsistency(nextRep != null);
					ensureConsistency(nextDef != null);
					ensureConsistency(nextRep <= nextDef);
				} else {
					nextRep = 0;
					nextDef = 0;
				}

				// retrieve val if needed
				if (nextRepDefExists && (nextDef == maxDefenitionLevel)) {
					// retrieve value, must be there
					ensureConsistency(valIterator.hasNext());
					nextVal = valIterator.next();
				} else {
					// either end of column or a missing branch
					nextVal = null;
				}
			}

			@Override
			public String toString() {
				return nameString + "=" + "(rep=" + curRep + ", def=" + curDef
						+ ", val=" + curVal + ")";
			}

			public int getNextRep() {
				return nextRep;
			}

			// next element of a branch
			public Object next(int repLev) {
				if (nextRep == repLev) {
					isChanged = nextRepDefExists; // end of column
					curVal = nextVal;
					curRep = nextRep;
					curDef = nextDef;
					preloadNext();
					return curVal;
				} else {
					isChanged = false;
					return null;
				}
			}

			void identifyName() {
				name = new ArrayList<SubName>();
				// level = -1;
				StringBuffer buf = new StringBuffer();
				for (GenericRecord subName : nameArray) {
					if (buf.length() != 0)
						buf.append('.');
					isArray = ((Integer) subName.get("IsArray")) == 1;
					String s = subName.get(0).toString();
					name.add(new SubName(s, isArray));
					buf.append(s);
					if (isArray) {
						// level++;
						buf.append("[]");
					}
				}
				nameString = buf.toString();
				// QUESTION - why level can not be left 01, if there are no
				// arrays
				/*
				 * if (level < 0) // throw new RuntimeException(
				 * "Outer enclosing array is missing");
				 */
			};
		};

		static class ColumnScannersTree extends
				AbstractFacade.ColumnTree<ColumnScanner, ColumnScannersTree> {
			ColumnScannersTree(int level, boolean isArray) {
				super(level, isArray);
			}
		};

		ScannerFacade(Schema drecSchema, Schema orecSchema,
				Array<GenericRecord> drecData) {
			super(drecSchema, drecData);
			dataSetSchema = orecSchema;
			populateIsomorphicReadersTree(orecSchema,
					drecSchema.getElementType(), drecData.iterator(), rtree,
					orecSchema.getElementType().getName(), false);
		}

		ScannerFacade(Schema drecSchema, Schema orecSchema, File drecDataFile,
				FileEncoding encoding) throws IOException {
			super(drecSchema, getData(drecSchema, drecDataFile, encoding));
			dataSetSchema = orecSchema;
			populateIsomorphicReadersTree(orecSchema,
					drecSchema.getElementType(), drecData.iterator(), rtree,
					orecSchema.getElementType().getName(), false);
		}

		@SuppressWarnings("unchecked")
		private static Object exportToOrecRecursive(String name,
				ColumnScannersTree rtree, Schema orecSchema, int rep) {
			switch (orecSchema.getType()) {
			case RECORD:
				boolean isFieldsEmpty = true;
				GenericRecord record = new GenericData.Record(orecSchema);
				ColumnScannersTree subTree = rtree.children.get(name);
				for (Schema.Field field : orecSchema.getFields()) {
					Object o = exportToOrecRecursive(field.name(), subTree,
							field.schema(), rep);
					if (o != null) {
						record.put(field.name(), o);
						isFieldsEmpty = isFieldsEmpty
								&& o instanceof GenericArray
								&& ((GenericArray<Object>) o).size() == 0;
					}
				}
				return isFieldsEmpty ? null : record;
			case ARRAY:
				GenericArray<Object> array = new GenericData.Array<Object>(10,
						orecSchema);
				Object o;
				boolean repeated = false;
				while (null != (o = exportToOrecRecursive(name, rtree,
						orecSchema.getElementType(), repeated ? rtree.level
								: rep))) {
					array.add(o);
					repeated = true;
				}

				return array;
			case INT:
			case STRING:
				return rtree.scalars.get(name).next(rep);
			default:
				throw new UnsupportedOperationException();
			}
		}

		@SuppressWarnings("unchecked")
		public void exportToOrec(Schema orecSchema, File orecFile,
				FileEncoding encoding) throws IOException {
			Array<GenericRecord> orecData = (Array<GenericRecord>) exportToOrecRecursive(
					orecSchema.getElementType().getName(), rtree, orecSchema, 0);

			writeData(orecData, orecSchema, orecFile, encoding);
		}

		private void populateIsomorphicReadersTree(Schema orecSchema,
				Schema drecColumnSchema,
				Iterator<GenericRecord> drecDataIterator,
				ColumnScannersTree rtree, String fromName, boolean fromArray) {
			switch (orecSchema.getType()) {
			case RECORD:
				push(fromName, fromArray);
				ColumnScannersTree subTree = new ColumnScannersTree(
						currentNestingLevel, fromArray);
				for (Schema.Field field : orecSchema.getFields()) {
					populateIsomorphicReadersTree(field.schema(),
							drecColumnSchema, drecDataIterator, subTree,
							field.name(), false);
				}
				pop();
				rtree.children.put(fromName, subTree);
				return;
			case ARRAY:
				if (fromArray)
					throw new UnsupportedOperationException(
							"Arrays of arrays are not supported");
				populateIsomorphicReadersTree(orecSchema.getElementType(),
						drecColumnSchema, drecDataIterator, rtree, fromName,
						true);
				return;
			case STRING:
			case INT:
				push(fromName, fromArray);
				ColumnScanner r = new ColumnScanner(drecColumnSchema,
						drecDataIterator.next());
				pop();
				rtree.scalars.put(fromName, r);
				return;
			default:
				throw new UnsupportedOperationException();
			}
		}
/*
		public ColumnScanner getColumnByName(String columnName) {
			// TODO Auto-generated method stub
			ColumnScanner res = getColumnByNameRecursive(columnName, rtree);
			return res;
		}

						
		private ColumnScanner getColumnByNameRecursive(String columnName,
				ColumnScannersTree subtree) {
			
			for(String scalarName : subtree.scalars.keySet())
			{
				System.out.println("scalar found: "+scalarName);
			}
				
			System.out.println("");
			
			if(subtree.scalars.containsKey(columnName))
			{
				return subtree.scalars.get(columnName); 
		*/
	}

	/**
	 * Read given avro file as array of avro records.
	 * 
	 * @param schema
	 *            - schema of the data
	 * @param file
	 *            - file to read from
	 * @param encoding
	 *            - encoding to be used (JSON or BIN)
	 * @return array of all records in the file
	 * @throws IOException
	 */
	public static Array<GenericRecord> getData(Schema schema, File file,
			FileEncoding encoding) throws IOException {
		DatumReader<Array<GenericRecord>> reader = new GenericDatumReader<Array<GenericRecord>>(
				schema);
		Decoder decoder;
		switch (encoding) {
		case JSON:
			decoder = new JsonDecoder(schema, new FileInputStream(file));
			break;
		case BIN:
		default:
			decoder = new DecoderFactory().createBinaryDecoder(
					new FileInputStream(file), null);
		}
		Array<GenericRecord> inData = reader.read(null, decoder);
		return inData;
	}

	/**
	 * Saves given data under given schema, with given encoding, using Avro
	 * framework
	 * 
	 * @param data
	 *            - array of avro records.
	 * @param schema
	 *            - schem to which records conform
	 * @param file
	 *            = file to save data
	 * @param encoding
	 *            - incoding o be used. it can be JSON or BIN
	 * @throws IOException
	 */
	public static void writeData(Array<GenericRecord> data, Schema schema,
			File file, FileEncoding encoding) throws IOException {
		Encoder encoder;
		switch (encoding) {
		case JSON:
			encoder = new JsonEncoder(schema, new FileOutputStream(file));
			break;
		case BIN:
		default:
			encoder = new BinaryEncoder(new FileOutputStream(file));
		}
		DatumWriter<Array<GenericRecord>> writer = new GenericDatumWriter<Array<GenericRecord>>(
				schema);
		writer.write(data, encoder);
		encoder.flush();
	}

	static public Array<GenericRecord> getData(Schema inSchema,
			String filename, FileEncoding encoding) throws IOException {
		return getData(inSchema, getFile(filename), encoding);
	}

	static public File getFile(String filename) {
		return new File("testdata" + File.separatorChar + filename);
	}

	static public Schema getSchema(File file) throws IOException {
		return Schema.parse(new FileInputStream(file));
	}

	static public Schema getSchema(String filename) {

		try {
			return Schema.parse(new FileInputStream(getFile(filename)));
		} catch (Exception e) {
			throw new RuntimeException("Loading schema from the file "
					+ filename + " failed ", e);
		}
	}

	static public File getTempFile(String filename) {
		return new File("testdatatemp" + File.separatorChar + filename);
	}

}
