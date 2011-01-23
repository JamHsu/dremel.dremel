package dremel.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.RecognitionException;
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

import dremel.common.Drec.ScannerFacade.ColumnScanner;


public final class Drec {
	enum ColumnType {
		INT, STRING
	}

	static final class Expression<T> {
		JavaLangScript script;
		ColumnScanner params[];
		ColumnScanner param;
		
		@SuppressWarnings("unchecked")
		public T evaluate() throws InvocationTargetException {
			return (T) script.evaluate(params);
		}
		
		Expression(AstNode expr, ScannerFacade scanner) {
			System.out.println("Expression "+ expr.toStringTree());
			//NEXTTODO
			// Query parse and resolve parameters by finding respective columnScanner in 
			// scannerFacade and making a reference to it directly.
		}
	}
	static final class Table {
		Table(String table) {};
	}


	static final class SubName {

		final boolean isArray;

		final String name;

		SubName(String name, boolean isArray) {
			this.name = name;
			this.isArray = isArray;
		}
	}

	public static enum FileEncoding {
		JSON, BIN
	}

	static abstract class AbstractFacade {
		// TODO override toString() to ease debugging
		/**
		 * This class correspond to the  field in the Dremel paper example 	
		 */
		static abstract class Column {
			
			GenericRecord drecColumn;
			final Schema drecColumnSchema;
			boolean isArray;
			int maxDefenitionLevel;
			List<SubName> name; // name of this field and all above fields in reverse order
			GenericArray<GenericRecord> nameArray; // name of this field and all above fields in reverse order
			String nameString;
			GenericArray<Integer> rep; // repetition "sub column"
			GenericArray<Integer> def; // definition "sub column"
			
			// pointer to the current typed array.
			GenericArray<Object> val;
			
			// only one of the typed  array is actually used
			GenericArray<Integer> valInt;
			GenericArray<Utf8> valString;
			ColumnType valType;

			Column(Schema drecColumnSchema) {
				this.drecColumnSchema = drecColumnSchema;
			}

			/**
			 * Calculate maximum definition level for the given column
			 * It is calculated as number of "array elements" in the path.  According to the paper
			 * the definition level is number of undefined values in the path that are actually defined,
			 * So maximum possible value - it is number of values that are optional or repeated. Both of them
			 * are represented as arrays. Missing value will be represented as array of size 0. 
			 * 
			 * @param namePath - array of field definitions
			 * @return - max definition level
			 */
			protected static int getDefinitionLevelFromColumnName(GenericArray<GenericRecord> namePath)
			{
				int resultLevel = -1;
				for(GenericRecord  nextNamePart : namePath)
				{
					Boolean isArray = ((Integer) nextNamePart.get("IsArray")) == 1;
					if (isArray) {
						resultLevel++;						
					}
				}
				
				assert(resultLevel>=0);
				
				return resultLevel;
			}
			public String toString()
			{
				return  null;//"AbstractFacade.Column. name: " + formatName + 
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
// QUESTION - why the class is abstract ?
		static abstract class ColumnTree<SCALAR, TREE> {
			final HashMap<String, TREE> children = new LinkedHashMap<String, TREE>();
			final int level;
			final boolean isArray;
			final HashMap<String, SCALAR> scalars = new LinkedHashMap<String, SCALAR>();

			ColumnTree(int level, boolean isArray) {
				this.level = level;
				this.isArray = isArray;
			};
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

	public static class WriterFacade extends AbstractFacade {

		// TODO override toString() to ease debugging
		public final class ColumnWriter extends Column {
			
		    // not integrated for now. 
			Expression<Object> expression;
			/**
			 * 
			 * @param drecColumnSchema - defines column structure. Column might be scalar or array or record.
			 * @param type - textual representation of the column data type
			 */
			ColumnWriter(Schema drecColumnSchema, String type) {
				super(drecColumnSchema);

				name = new ArrayList<SubName>();
				name.addAll(currentNamePath);
				Schema drecNameSchema = drecColumnSchema.getField("Name")
						.schema().getElementType();
				nameArray = new GenericData.Array<GenericRecord>(10,
						drecColumnSchema.getField("Name").schema());
					
				maxDefenitionLevel = getDefinitionLevelFromColumnName(nameArray);
				
				StringBuffer buf = new StringBuffer();
				for (SubName subName : name) {
					GenericRecord rec = new GenericData.Record(drecNameSchema);
					rec.put("SubName", new Utf8(subName.name));
					rec.put("IsArray", new Integer(subName.isArray ? 1 : 0));
					nameArray.add(rec);
					if (buf.length() != 0)
						buf.append('.');
					buf.append(subName.name);
					if (subName.isArray) {
						buf.append("[]");
					}
				}
				nameString = buf.toString();

				drecColumn = new GenericData.Record(drecColumnSchema);
				drecColumn.put("Name", nameArray);
				drecColumn.put("ValType", new Utf8(type));

				valInt = new GenericData.Array<Integer>(10, drecColumnSchema
						.getField("ValInt").schema());
				drecColumn.put("ValInt", valInt);

				valString = new GenericData.Array<Utf8>(10, drecColumnSchema
						.getField("ValString").schema());
				drecColumn.put("ValString", valString);

				rep = new GenericData.Array<Integer>(10, drecColumnSchema
						.getField("Rep").schema());
				drecColumn.put("Rep", rep);

				def = new GenericData.Array<Integer>(10, drecColumnSchema
						.getField("Def").schema());
				drecColumn.put("Def", def);
				drecData.add(drecColumn);
				identifyType();
			}
		}

		static class Tree extends AbstractFacade.ColumnTree<ColumnWriter, Tree> {

			public Tree(int level, boolean isArray) {
				super(level, isArray);
			}

		};

		public WriterFacade(Schema drecSchema, Array<GenericRecord> drecData) {
			super(drecSchema, drecData);
		}

		public WriterFacade(Schema drecSchema, File drecDataFile)
				throws IOException {
			super(drecSchema, drecDataFile);
		}

		public WriterFacade(File drecSchemaFile, File drecDataFile)
				throws IOException {
			super(drecSchemaFile, drecDataFile);
		}

		@SuppressWarnings("unchecked")
		private void importFromOrecRecursive(String name, Schema orecSchema,
				Object orecData, Tree wtree, int repLevel, int defLevel) {
			switch (orecSchema.getType()) {
			case RECORD:
				Tree subTree = wtree.children.get(name);
				for (Schema.Field field : orecSchema.getFields()) {
					importFromOrecRecursive(
							field.name(),
							field.schema(),
							orecData == null ? null
									: ((GenericRecord) orecData).get(field
											.pos()), subTree, repLevel,
							defLevel);
				}
				return;
			case ARRAY:
				boolean repeated = false;
				if (orecData != null) {
					for (Object elem : ((Array<Object>) orecData)) {
						importFromOrecRecursive(name,
								orecSchema.getElementType(), elem, wtree,
								repeated ? wtree.level : repLevel, wtree.level);
						repeated = true;
					}
				}
				if (!repeated)
					importFromOrecRecursive(name, orecSchema.getElementType(),
							null, wtree, repLevel, defLevel);

				return;
			case STRING:
			case INT:
				ColumnWriter columnWriter = wtree.scalars.get(name);

				columnWriter.rep.add(repLevel);
				columnWriter.def.add(defLevel);
				if (orecData != null)
					columnWriter.val.add(orecData);
				return;
			default:
				throw new UnsupportedOperationException();
			}
		}

		public void importFromOrec(Schema orecSchema, File orecFile,
				FileEncoding orecEncoding, FileEncoding drecEncoding)
				throws IOException {

			Array<GenericRecord> orecData = getData(orecSchema, orecFile,
					orecEncoding);
			drecData = new GenericData.Array<GenericRecord>(10, drecSchema);
			Tree wtree = new Tree(0, true);

			// traverse orecSchema and create isomorphic tree of array writers
			populateIsomorphicWritersTree(orecSchema, wtree, orecSchema
					.getElementType().getName(), false);

			// convert the data
			importFromOrecRecursive(orecSchema.getElementType().getName(),
					orecSchema, orecData, wtree, 0, 0);
			writeData(drecData, drecSchema, drecDataFile, drecEncoding);
		}

		public void importFromDrec(Schema orecSourceSchema,
				File drecSourceFile, FileEncoding drecSourceEncoding,
				Schema orecDestSchema, FileEncoding drecDestEncoding)
				throws IOException, InvocationTargetException {

			ScannerFacade scanner = new ScannerFacade(drecSchema,
					orecSourceSchema, drecSourceFile, drecSourceEncoding);
			drecData = new GenericData.Array<GenericRecord>(10, drecSchema);
			Tree wtree = new Tree(0, true);

			populateIsomorphicWritersTree(orecDestSchema, wtree, orecDestSchema
					.getElementType().getName(), false);

			ScannerFacade.Tree rtree = scanner.rtree;

			Context ctx = new Context();
			do {
				ctx.level = ctx.nextLevel;
				ctx.nextLevel = 0;
				ctx.isEmpty = true;
				advanceRtree(rtree, ctx);

				if (ctx.isEmpty && ctx.level == 0)
					break;
				
				/* copy vrec - one layer of the data - current elements from each column) */
				/* For the non array columns - there is no advance*/
				
				copy(rtree, wtree);

			} while (true);

			writeData(drecData, drecSchema, drecDataFile, drecDestEncoding);
		}
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
		public void importFromQuery(Schema orecSourceSchema,
				Query query, Schema orecDestSchema, FileEncoding drecDestEncoding)
				throws IOException, InvocationTargetException {
			drecData = new GenericData.Array<GenericRecord>(10, drecSchema);
			Tree wtree = new Tree(0, true);
			populateIsomorphicWritersTree(query.outSchema, wtree, orecDestSchema
					.getElementType().getName(), false);
			ScannerFacade.Tree rtree = query.scanner.rtree;

			//NEXTTODO: this context and advance rtree (reader tree) code snippet must be transfered into query
			//class
			Context ctx = new Context();
			do {
				ctx.level = ctx.nextLevel;
				ctx.nextLevel = 0;
				ctx.isEmpty = true;
				advanceRtree(rtree, ctx);

				if (ctx.isEmpty && ctx.level == 0)
					break;
				
				process(wtree); //evaluates the query and adds one record to final result set 

			} while (true);
			writeData(drecData, drecSchema, drecDataFile, drecDestEncoding);
		}

		public static final class Context {
			boolean isEmpty = true;
			boolean isChanged = false;
			int level = 0;
			int nextLevel = 0;
		};

		private void advanceRtree(ScannerFacade.Tree rtree, Context ctx) {

			for (ScannerFacade.Tree t : rtree.children.values()) {
				advanceRtree(t, ctx);
			}

			for (ColumnScanner s : rtree.scalars.values()) {
				s.next(ctx.level);
				if((ctx.nextLevel < s.nextRep))
					ctx.nextLevel =  s.nextRep;
				ctx.isEmpty = ctx.isEmpty && !s.isChanged;
			}
		}

		
		//NEXTTODO this function is never tested, the copy function is tested very well and should
		//be consulted to understand how this function must work. In future copy function
		//must be deleted because copy function is just particular simple case of process function
		private boolean process(Tree wtree) throws InvocationTargetException {
			for (Map.Entry<String, ColumnWriter> ws : wtree.scalars.entrySet()) {
				Expression<Object> expr = ws.getValue().expression;
				if (expr.param.isChanged) {
					if (expr.param.curVal != null) {
						ws.getValue().val.add(expr.evaluate());
					}
					ws.getValue().rep.add(expr.param.curRep);
					ws.getValue().def.add(expr.param.curDef);
				}
			}
			for (Map.Entry<String, Tree> wt : wtree.children.entrySet()) {
				process(wt.getValue());
			}
			return false;
		}

		private void copy(ScannerFacade.Tree rtree, Tree wtree) {
			for (Map.Entry<String, ColumnWriter> ws : wtree.scalars.entrySet()) {
				ColumnScanner s = rtree.scalars.get(ws.getKey());
				if (s.isChanged) {
					if (s.curVal != null) {
						ws.getValue().val.add(s.curVal);
					}
					ws.getValue().rep.add(s.curRep);
					ws.getValue().def.add(s.curDef);
				}
			}
			for (Map.Entry<String, Tree> wt : wtree.children.entrySet()) {
				ScannerFacade.Tree rt = rtree.children.get(wt.getKey());
				copy(rt, wt.getValue());
			}
		}

		private void populateIsomorphicWritersTree(Schema orecSchema,
				Tree wtree, String fromName, boolean fromArray) {
			switch (orecSchema.getType()) {
			case RECORD:
				push(fromName, fromArray);
				Tree subTree = new Tree(currentNestingLevel, fromArray);
				for (Schema.Field field : orecSchema.getFields()) {
					populateIsomorphicWritersTree(field.schema(), subTree,
							field.name(), false);
				}
				wtree.children.put(fromName, subTree);
				pop();
				return;
			case ARRAY:
				if (fromArray)
					throw new UnsupportedOperationException(
							"Arrays of arrays are not supported");
				populateIsomorphicWritersTree(orecSchema.getElementType(),
						wtree, fromName, true);
				return;
			case STRING:
			case INT:
				ColumnWriter w;
				push(fromName, fromArray);
				w = new ColumnWriter(drecSchema.getElementType(), orecSchema
						.getType().toString());
				pop();
				wtree.scalars.put(fromName, w);
				return;
			default:
				throw new UnsupportedOperationException();
			}
		}

	}

	/**
	 * Incapsulates Dremel encoding defined in the paper. Contain the
	 * input data in form of the tree of column scanners.
	 * @author David.Gruzman
	 *
	 */
	public static class ScannerFacade extends AbstractFacade {

		// It is a tree of the ColumnScanner objects. 
		Tree rtree = new Tree(0, true);


		// iterate over the tree of columns and print it.
		public String toString()
		{
			StringBuilder result = new StringBuilder();
			prettyPrintRecursive(rtree, result);
			
			return result.toString();				
		}
		
		public void prettyPrintRecursive(Tree subtree, StringBuilder result)
		{
			for(String columnName : subtree.scalars.keySet())
			{
				result.append("Column in the level " + subtree.level + " is "+columnName + "\n");								
			}
			for(String child : subtree.children.keySet())
			{
				Tree childTree = subtree.children.get(child);
				prettyPrintRecursive(childTree, result);
			}
			
		}
		
		final static public class ColumnScanner extends Column {

			private Iterator<Integer> defIterator;
			private Iterator<Integer> repIterator;
			private Iterator<Object> valIterator;

			private Integer curDef;
			private Integer curRep;
			private Object curVal;
			private Integer nextDef;
			private Integer nextRep;
			private boolean nextRepDefExists;
			private Object nextVal;
			private boolean isChanged;

			/**
			 * 	
			 * @param drecColumnSchema 
			 * @param drecColumn_  Dremel column with string or int value + columns of definitions and repetition levels
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
				
				maxDefenitionLevel = getDefinitionLevelFromColumnName(nameArray); 

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
				return nameString + "=" + "(rep=" + curRep + ", def="
						+ curDef + ", val=" + curVal + ")";
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
				//level = -1;
				StringBuffer buf = new StringBuffer();
				for (GenericRecord subName : nameArray) {
					if (buf.length() != 0)
						buf.append('.');
					isArray = ((Integer) subName.get("IsArray")) == 1;
					String s = subName.get(0).toString();
					name.add(new SubName(s, isArray));
					buf.append(s);
					if (isArray) {
					//	level++;
						buf.append("[]");
					}
				}
				nameString = buf.toString();
				// QUESTION - why level can not be left 01, if there are no arrays
				/*if (level < 0)  // 
					throw new RuntimeException( 
							"Outer enclosing array is missing");
							*/
			};
		};

		static private class Tree extends
				AbstractFacade.ColumnTree<ColumnScanner, Tree> {
			Tree(int level, boolean isArray) {
				super(level, isArray);
			}
		};

		ScannerFacade(Schema drecSchema, Schema orecSchema,
				Array<GenericRecord> drecData) {
			super(drecSchema, drecData);
			populateIsomorphicReadersTree(orecSchema,
					drecSchema.getElementType(), drecData.iterator(), rtree,
					orecSchema.getElementType().getName(), false);
		}

		ScannerFacade(Schema drecSchema, Schema orecSchema, File drecDataFile,
				FileEncoding encoding) throws IOException {
			super(drecSchema, getData(drecSchema, drecDataFile, encoding));
			populateIsomorphicReadersTree(orecSchema,
					drecSchema.getElementType(), drecData.iterator(), rtree,
					orecSchema.getElementType().getName(), false);
		}

		@SuppressWarnings("unchecked")
		private static Object exportToOrecRecursive(String name, Tree rtree,
				Schema orecSchema, int rep) {
			switch (orecSchema.getType()) {
			case RECORD:
				boolean isFieldsEmpty = true;
				GenericRecord record = new GenericData.Record(orecSchema);
				Tree subTree = rtree.children.get(name);
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
				Iterator<GenericRecord> drecDataIterator, Tree rtree,
				String fromName, boolean fromArray) {
			switch (orecSchema.getType()) {
			case RECORD:
				push(fromName, fromArray);
				Tree subTree = new Tree(currentNestingLevel, fromArray);
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
	}

	/**
	 * Read given avro file as array of avro records.
	 * @param schema - schema of the data
	 * @param file - file to read from
	 * @param encoding - encoding to be used (JSON or BIN)
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
	 * Saves given data under given schema, with given encoding, using Avro framework
	 * @param data - array of avro records.
	 * @param schema - schem to which records conform
	 * @param file = file to save data
	 * @param encoding - incoding o be used. it can be JSON or BIN
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

	static public Schema getSchema(String filename) throws IOException {
		return Schema.parse(new FileInputStream(getFile(filename)));
	}

	static public File getTempFile(String filename) {
		return new File("testdatatemp" + File.separatorChar + filename);
	}

}
