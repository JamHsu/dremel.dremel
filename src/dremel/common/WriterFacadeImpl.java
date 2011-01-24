package dremel.common;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.util.Utf8;

import dremel.common.Drec.AbstractFacade;
import dremel.common.Drec.Expression;
import dremel.common.Drec.FileEncoding;
import dremel.common.Drec.ScannerFacade;
import dremel.common.Drec.SubName;
import dremel.common.Drec.AbstractFacade.Column;
import dremel.common.Drec.ScannerFacade.ColumnScanner;
//import dremel.common.Drec.WriterFacade.ColumnWriter;
//import dremel.common.Drec.WriterFacade.Context;
//import dremel.common.Drec.WriterFacade.WriterTree;

public class WriterFacadeImpl extends AbstractFacade implements WriterFacade {

	// TODO override toString() to ease debugging
	public final class ColumnWriter extends AbstractFacade.Column {
		
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
				
			maxDefenitionLevel = getMaxDefinitionLevelFromColumnName(nameArray);
			
			// initialize field name if form of nameArray and nameString. 
			initNameStrcutures(drecNameSchema);

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
		private void initNameStrcutures(Schema drecNameSchema) {
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
		}
	}

	static class WriterTree extends AbstractFacade.ColumnTree<ColumnWriter, WriterFacadeImpl.WriterTree> {

		public WriterTree(int level, boolean isArray) {
			super(level, isArray);
		}

	};

	public WriterFacadeImpl(Schema drecSchema, Array<GenericRecord> drecData) {
		super(drecSchema, drecData);
	}

	public WriterFacadeImpl(Schema drecSchema, File drecDataFile)
			throws IOException {
		super(drecSchema, drecDataFile);
	}

	public WriterFacadeImpl(File drecSchemaFile, File drecDataFile)
			throws IOException {
		super(drecSchemaFile, drecDataFile);
	}

	@SuppressWarnings("unchecked")
	private void importFromOrecRecursive(String name, Schema orecSchema,
			Object orecData, WriterFacadeImpl.WriterTree wtree, int repLevel, int defLevel) {
		switch (orecSchema.getType()) {
		case RECORD:
			WriterFacadeImpl.WriterTree subTree = wtree.children.get(name);
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

	/* (non-Javadoc)
	 * @see dremel.common.WriterFacede#importFromOrec(org.apache.avro.Schema, java.io.File, dremel.common.Drec.FileEncoding, dremel.common.Drec.FileEncoding)
	 */
	@Override
	public void importFromOrec(Schema orecSchema, File orecFile,
			FileEncoding orecEncoding, FileEncoding drecEncoding)
			throws IOException {

		Array<GenericRecord> orecData = Drec.getData(orecSchema, orecFile,
				orecEncoding);
		drecData = new GenericData.Array<GenericRecord>(10, drecSchema);
		WriterFacadeImpl.WriterTree wtree = new WriterTree(0, true);

		// traverse orecSchema and create isomorphic tree of array writers
		populateIsomorphicWritersTreeRecursive(orecSchema, wtree, orecSchema
				.getElementType().getName(), false);

		// convert the data
		importFromOrecRecursive(orecSchema.getElementType().getName(),
				orecSchema, orecData, wtree, 0, 0);
		Drec.writeData(drecData, drecSchema, drecDataFile, drecEncoding);
	}

	/* (non-Javadoc)
	 * @see dremel.common.WriterFacede#importFromDrec(org.apache.avro.Schema, java.io.File, dremel.common.Drec.FileEncoding, org.apache.avro.Schema, dremel.common.Drec.FileEncoding)
	 */
	@Override
	public void importFromDrec(Schema orecSourceSchema,
			File drecSourceFile, FileEncoding drecSourceEncoding,
			Schema orecDestSchema, FileEncoding drecDestEncoding)
			throws IOException, InvocationTargetException {

		ScannerFacade scanner = new ScannerFacade(drecSchema,
				orecSourceSchema, drecSourceFile, drecSourceEncoding);
		drecData = new GenericData.Array<GenericRecord>(10, drecSchema);
		WriterFacadeImpl.WriterTree wtree = new WriterTree(0, true);

		populateIsomorphicWritersTreeRecursive(orecDestSchema, wtree, orecDestSchema
				.getElementType().getName(), false);

		ScannerFacade.Tree rtree = scanner.rtree;

		WriterFacadeImpl.Context ctx = new Context();
		do {
			ctx.level = ctx.nextLevel;
			ctx.nextLevel = 0;
			ctx.isEmpty = true;
			advanceRtreeRecursive(rtree, ctx);

			if (ctx.isEmpty && ctx.level == 0)
				break;
			
			/* copy vrec - one layer of the data - current elements from each column) */
			/* For the non array columns - there is no advance*/
			
			copy(rtree, wtree);

		} while (true);

		Drec.writeData(drecData, drecSchema, drecDataFile, drecDestEncoding);
	}
	/* (non-Javadoc)
	 * @see dremel.common.WriterFacede#importFromQuery(org.apache.avro.Schema, dremel.common.Query, org.apache.avro.Schema, dremel.common.Drec.FileEncoding)
	 */
	@Override
	public void importFromQuery(Schema orecSourceSchema,
			Query query, Schema orecDestSchema, FileEncoding drecDestEncoding)
			throws IOException, InvocationTargetException {
		drecData = new GenericData.Array<GenericRecord>(10, drecSchema);
		
		WriterFacadeImpl.WriterTree wtree = new WriterTree(0, true);
		
		populateIsomorphicWritersTreeRecursive(query.outSchema, wtree, orecDestSchema
				.getElementType().getName(), false);
		
		ScannerFacade.Tree rtree = query.scanner.rtree;

		//NEXTTODO: this context and advance rtree (reader tree) code snippet must be transfered into query
		//class
		WriterFacadeImpl.Context ctx = new Context();
		do {
			ctx.level = ctx.nextLevel; 
			ctx.nextLevel = 0;
			ctx.isEmpty = true;
			advanceRtreeRecursive(rtree, ctx);

			if (ctx.isEmpty && ctx.level == 0)
				break;
			
			process(wtree); //evaluates the query and adds one record to final result set 

		} while (true);
		Drec.writeData(drecData, drecSchema, drecDataFile, drecDestEncoding);
	}

	public static final class Context {
		boolean isEmpty = true;
		boolean isChanged = false;
		int level = 0;
		int nextLevel = 0;
	};

	private void advanceRtreeRecursive(ScannerFacade.Tree rtree, WriterFacadeImpl.Context ctx) {

		for (ScannerFacade.Tree t : rtree.children.values()) {
			advanceRtreeRecursive(t, ctx);
		}

		for (ScannerFacade.ColumnScanner s : rtree.scalars.values()) {
			s.next(ctx.level);
			if((ctx.nextLevel < s.nextRep))
				ctx.nextLevel =  s.nextRep;
			ctx.isEmpty = ctx.isEmpty && !s.isChanged;
		}
	}

	
	//NEXTTODO this function is never tested, the copy function is tested very well and should
	//be consulted to understand how this function must work. In future copy function
	//must be deleted because copy function is just particular simple case of process function
	
	/**
	 * Copy single slice (VRec)
	 */
	private static boolean process(WriterFacadeImpl.WriterTree wtree) throws InvocationTargetException {
		
		
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
		for (Map.Entry<String, WriterFacadeImpl.WriterTree> wt : wtree.children.entrySet()) {
			process(wt.getValue());
		}
		
		return false;
	}

	private void copy(ScannerFacade.Tree rtree, WriterFacadeImpl.WriterTree wtree) {
		for (Map.Entry<String, ColumnWriter> ws : wtree.scalars.entrySet()) {
			ScannerFacade.ColumnScanner s = rtree.scalars.get(ws.getKey());
			if (s.isChanged) {
				if (s.curVal != null) {
					ws.getValue().val.add(s.curVal);
				}
				ws.getValue().rep.add(s.curRep);
				ws.getValue().def.add(s.curDef);
			}
		}
		for (Map.Entry<String, WriterFacadeImpl.WriterTree> wt : wtree.children.entrySet()) {
			ScannerFacade.Tree rt = rtree.children.get(wt.getKey());
			copy(rt, wt.getValue());
		}
	}

	private void populateIsomorphicWritersTreeRecursive(Schema orecSchema,
			WriterFacadeImpl.WriterTree wtree, String fromName, boolean fromArray) {
		switch (orecSchema.getType()) {
		case RECORD:
			push(fromName, fromArray);
			WriterFacadeImpl.WriterTree subTree = new WriterTree(currentNestingLevel, fromArray);
			for (Schema.Field field : orecSchema.getFields()) {
				populateIsomorphicWritersTreeRecursive(field.schema(), subTree,
						field.name(), false);
			}
			wtree.children.put(fromName, subTree);
			pop();
			return;
		case ARRAY:
			if (fromArray)
				throw new UnsupportedOperationException(
						"Arrays of arrays are not supported");
			populateIsomorphicWritersTreeRecursive(orecSchema.getElementType(),
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