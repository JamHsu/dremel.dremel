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

import static dremel.common.Drec.getData;
import static dremel.common.Drec.getFile;
import static dremel.common.Drec.getSchema;
import static dremel.common.Drec.getTempFile;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Random;

import org.antlr.runtime.RecognitionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificCompiler;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import dremel.common.Drec.FileEncoding;
import dremel.common.Drec.ScannerFacade;

/**
 * 
 * @author camuelg
 */
public class AvroTest {

	
	@Test
	public void compileSchema() throws IOException {
		SpecificCompiler.compileSchema(getFile("OrecSchema.avpr.json"), 
			getTempFile(""));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void deepCopyTest() throws IOException {

		Schema inSchema = getSchema("OrecSchema.avpr.json");
		
		File inFile = getFile("OrecDremelPaperData.avro.json");
		Array<GenericRecord> inData = getData(inSchema, inFile,
				FileEncoding.JSON);
		File outFile = getTempFile("AvroTestCopyDeep.avro.json");
		FileUtils.deleteQuietly(outFile);

		Encoder encoder = new JsonEncoder(inSchema, new FileOutputStream(
				outFile));

		DatumWriter<Array<GenericRecord>> writer = new GenericDatumWriter<Array<GenericRecord>>(
				inSchema);

		Array<GenericRecord> outData = (Array<GenericRecord>) deepCopyRecursive(
				inSchema, inData);

		writer.write(outData, encoder);

		encoder.flush();

		assertTrue("Copied files differs",
				FileUtils.contentEquals(inFile, outFile));

		// FileUtils.forceDelete(outFile);
		FileUtils.forceDeleteOnExit(outFile); // TODO fucking file hasn't being
		// deleted for some reason
	}

	@Test
	public void shallowCopyTest() throws IOException {

		Schema inSchema = getSchema("OrecSchema.avpr.json");
		File inFile = getFile("OrecDremelPaperData.avro.json");
		Array<GenericRecord> inData = getData(inSchema, inFile,
				FileEncoding.JSON);
		File outFile = getTempFile("AvroTestCopyShallow.avro.json");
		FileUtils.deleteQuietly(outFile);

		Encoder encoder = new JsonEncoder(inSchema, new FileOutputStream(
				outFile));

		DatumWriter<Array<GenericRecord>> writer = new GenericDatumWriter<Array<GenericRecord>>(
				inSchema);

		writer.write(inData, encoder);

		encoder.flush();

		assertTrue("Copied files differs",
				FileUtils.contentEquals(inFile, outFile));

		// FileUtils.forceDelete(outFile);
		FileUtils.forceDeleteOnExit(outFile); // TODO fucking file hasn't being
		// deleted for some reason
	}

	@SuppressWarnings("unchecked")
	protected boolean deepCompareRecursive(Schema schema, Object a, Object b) {
		switch (schema.getType()) {
		case RECORD:
			for (Schema.Field field : schema.getFields()) {
				if (!deepCompareRecursive(field.schema(),
						((GenericRecord) a).get(field.pos()),
						((GenericRecord) b).get(field.pos())))
					return false;
			} // comparison only by schema defined fields
			return true;
		case ARRAY:
			Iterator<Object> bb = ((Array<Object>) b).iterator();
			for (Object aa : ((Array<Object>) a)) {
				if (!deepCompareRecursive(schema.getElementType(), aa, bb.next()))
					return false;
			}
			return !bb.hasNext();
		case STRING:
		case INT:
			return a.equals(b);
		default:
			throw new UnsupportedOperationException();
		}
	}

	@SuppressWarnings("unchecked")
	protected Object deepCopyRecursive(Schema schema, Object thing) {
		switch (schema.getType()) {
		case RECORD:
			GenericRecord record = new GenericData.Record(schema);
			for (Schema.Field field : schema.getFields()) {
				record.put(
						field.name(),
						deepCopyRecursive(field.schema(),
								((GenericRecord) thing).get(field.pos())));
			}
			return record;
		case ARRAY:
			int length = (int) ((Array<Object>) thing).size();
			GenericArray<Object> array = new GenericData.Array<Object>(length,
					schema);
			for (Object r : ((Array<Object>) thing)) {
				array.add(deepCopyRecursive(schema.getElementType(), r));
			}
			return array;
		case STRING:
			return new Utf8(thing.toString());
		case INT:
			return new Integer((Integer) thing);
		default:
			throw new UnsupportedOperationException();
		}
	}

	private void orecToDrecConversion(Schema orecSchema, Schema drecSchema,
			File originalFile, File resultFile, File tempDrecFile,
			FileEncoding drecEncoding, FileEncoding orecEncoding)
			throws IOException {
		FileUtils.deleteQuietly(resultFile);
		FileUtils.deleteQuietly(tempDrecFile);
		WriterFacade writer = new WriterFacadeImpl(drecSchema,
				tempDrecFile);
		writer.importFromOrec(orecSchema, originalFile, orecEncoding,
				drecEncoding);
		writer = null;
		ScannerFacade scanner = new ScannerFacade(drecSchema, orecSchema,
				tempDrecFile, drecEncoding);
		scanner.exportToOrec(orecSchema, resultFile, orecEncoding);
		scanner = null;
		Array<GenericRecord> orecData1 = getData(orecSchema, originalFile,
				orecEncoding);
		Array<GenericRecord> orecData2 = getData(orecSchema, resultFile,
				orecEncoding);
		assertTrue(deepCompareRecursive(orecSchema, orecData1, orecData2));
		// FileUtils.forceDelete(orecFile2);
	}

	// TODO Currently working to make implement this test
	@Test
	public void queryStarDremelPaperDataTest()
			throws IOException, RecognitionException, InvocationTargetException {
		// AvroTestTraceAspect.activate(System.err);
		FileEncoding encoding = FileEncoding.JSON;
		Schema orecSchema = getSchema("OrecSchema.avpr.json");
		Schema drecSchema = getSchema("DrecSchema.avpr.json"); // it is common schema for all cases
		File originalFile = getFile("OrecDremelPaperData.avro.json");
		File tempDrecFile1 = getTempFile("AvroTestStarQuerySource.avro.json");
		File tempDrecFile2 = getTempFile("AvroTestStarQueryDest.avro.json");
		File resultFile = getTempFile("AvroTestStarQueryResult.avro.json");
		FileUtils.deleteQuietly(tempDrecFile1);
		FileUtils.deleteQuietly(tempDrecFile2);
		
		// create empty columnar table
		WriterFacade writer1 = new WriterFacadeImpl(drecSchema, tempDrecFile1);
		
		// read record oriented data into columnar
		writer1.importFromOrec(orecSchema, originalFile, encoding, encoding);
		writer1 = null;
		
		// create empty columnar table
		WriterFacade writer2 = new WriterFacadeImpl(drecSchema, tempDrecFile2);
				
		/**
		 * Each columnar data file contains two schemas: Fixed dremel schema and
		 * object schema which defines actual object records  
		 */
		ScannerFacade scanner1 = new ScannerFacade(drecSchema, orecSchema,
				/*input*/ tempDrecFile1, encoding);
		System.out.println("Scanner is "+ scanner1.toString());
		
		writer2.importFromQuery(orecSchema, new Query(scanner1, "select Forward from table"), 
				orecSchema, encoding);
		writer2 = null;
	
		//TODO to implement proper comparison.
		
		ScannerFacade scanner2 = new ScannerFacade(drecSchema, orecSchema,
				tempDrecFile2, encoding);
		scanner2.exportToOrec(orecSchema, resultFile, encoding);
		scanner2 = null;
		assertTrue(deepCompareRecursive(drecSchema, originalFile, resultFile));
	}

	@Test
	public void orecToDrecRoundtripConversionDremelPaperDataTest()
			throws IOException {
		// AvroTestTraceAspect.activate(System.err);
		Schema orecSchema = getSchema("OrecSchema.avpr.json");
		Schema drecSchema = getSchema("DrecSchema.avpr.json");
		File originalFile = getFile("OrecDremelPaperData.avro.json");
		File resultFile = getTempFile("AvroTestDrecToOrec.avro.json");
		File tempDrecFile = getTempFile("AvroTestOrecToDrec.avro.json");
		orecToDrecConversion(orecSchema, drecSchema, originalFile, resultFile,
				tempDrecFile, FileEncoding.JSON, FileEncoding.JSON);
	}

	@Test
	public void DrecToDrecRoundtripConversionDremelPaperDataTest()
			throws IOException, InvocationTargetException {
		// AvroTestTraceAspect.activate(System.err);
		FileEncoding encoding = FileEncoding.JSON;
		Schema orecSchema = getSchema("OrecSchema.avpr.json");
		Schema drecSchema = getSchema("DrecSchema.avpr.json");
		File originalFile = getFile("OrecDremelPaperData.avro.json");
		File tempDrecFile1 = getTempFile("AvroTestDrec1.avro.json");
		File tempDrecFile2 = getTempFile("AvroTestDrec2.avro.json");
		FileUtils.deleteQuietly(tempDrecFile1);
		FileUtils.deleteQuietly(tempDrecFile2);
		WriterFacade writer1 = new WriterFacadeImpl(drecSchema, tempDrecFile1);
		writer1.importFromOrec(orecSchema, originalFile, encoding, encoding);
		writer1 = null;
		WriterFacade writer2 = new WriterFacadeImpl(drecSchema, tempDrecFile2);
		writer2.importFromDrec(orecSchema, tempDrecFile1, encoding, orecSchema,
				encoding);
		writer2 = null;
		Array<GenericRecord> data1 = getData(drecSchema, tempDrecFile1,
				encoding);
		Array<GenericRecord> data2 = getData(drecSchema, tempDrecFile2,
				encoding);
		assertTrue(deepCompareRecursive(drecSchema, data1, data2));
	}

	@Test
	public void drecToDrecRoundtripConversionRandomDataJsonTest()
			throws IOException, InvocationTargetException {
		drecToDrecRoundtripRandomConvert(getSchema("OrecSchema.avpr.json"), FileEncoding.JSON);
	}

	@Test
	public void drecToDrecRoundtripConversionRandomDataBinaryTest()
			throws IOException, InvocationTargetException {
		drecToDrecRoundtripRandomConvert(getSchema("OrecSchema.avpr.json"), FileEncoding.BIN);
	}
	@Test
	public void drecToDrecConversionRandomSchemaAndDataJsonTest()
			throws IOException, InvocationTargetException {
		File schemaFile = getTempFile("OrecRandomSchema.avpr.json");
		generateRandomOrecSchema(schemaFile,
				new Random(System.currentTimeMillis()), 3); // 5
		drecToDrecRoundtripRandomConvert(getSchema(schemaFile), FileEncoding.JSON);
	}

	// TODO  this test ocassionaly fails. It happens once.
	@Test
	public void drecToDrecConversionRandomSchemaAndDataBinaryTest()
			throws IOException, InvocationTargetException {
		File schemaFile = getTempFile("OrecRandomSchema.avpr.json");
		generateRandomOrecSchema(schemaFile,
				new Random(System.currentTimeMillis()), 3); // 5
		drecToDrecRoundtripRandomConvert(getSchema(schemaFile), FileEncoding.BIN);
	}

	public void drecToDrecRoundtripRandomConvert(Schema orecSchema, FileEncoding encoding)
			throws IOException, InvocationTargetException {
		// AvroTestTraceAspect.activate(System.err);
		Schema drecSchema = getSchema("DrecSchema.avpr.json");
		File orecRandomFile = getTempFile("OrecRandomData.avro.json");
		FileUtils.deleteQuietly(orecRandomFile);
		generateRandomData(orecSchema, orecRandomFile, 9, encoding);
		File tempDrecFile1 = getTempFile("AvroTestDrec1.avro.json");
		File tempDrecFile2 = getTempFile("AvroTestDrec2.avro.json");
		FileUtils.deleteQuietly(tempDrecFile1);
		FileUtils.deleteQuietly(tempDrecFile2);
		WriterFacade writer1 = new WriterFacadeImpl(drecSchema, tempDrecFile1);
		writer1.importFromOrec(orecSchema, orecRandomFile, encoding, encoding);
		writer1 = null;
		WriterFacade writer2 = new WriterFacadeImpl(drecSchema, tempDrecFile2);
		writer2.importFromDrec(orecSchema, tempDrecFile1, encoding, orecSchema,
				encoding);
		writer2 = null;
		Array<GenericRecord> data1 = getData(drecSchema, tempDrecFile1,
				encoding);
		Array<GenericRecord> data2 = getData(drecSchema, tempDrecFile2,
				encoding);
		assertTrue(deepCompareRecursive(drecSchema, data1, data2));
	}

	@Test
	public void orecToDrecRoundtripConversionRandomDataJsonTest()
			throws IOException {
		orecToDrecRoundtripRandomDataConvert(FileEncoding.JSON);
	}

	@Test
	public void orecToDrecRoundtripConversionRandomDataBinaryTest()
			throws IOException {
		orecToDrecRoundtripRandomDataConvert(FileEncoding.BIN);
	}
	
/*	@Test
	public void specificQueryTest()
	{
			
	} */

	public void orecToDrecRoundtripRandomDataConvert(FileEncoding encoding)
			throws IOException {
		// AvroTestTraceAspect.activate(System.err);
		Schema orecSchema = getSchema("OrecSchema.avpr.json");
		Schema drecSchema = getSchema("DrecSchema.avpr.json");
		File originalFile = getTempFile("OrecRandomData.avro.json");
		File resultFile = getTempFile("AvroTestDrecToOrec.avro.json");
		File tempDrecFile = getTempFile("AvroTestOrecToDrec.avro.json");
		generateRandomData(orecSchema, originalFile, 10, encoding);
		orecToDrecConversion(orecSchema, drecSchema, originalFile, resultFile,
				tempDrecFile, encoding, encoding);
	}

	@Test
	public void orecToDrecConversionRandomSchemaAndDataJsonTest()
			throws IOException {
		orecToDrecConvertRandomSchemaAndDataConvert(FileEncoding.JSON);
	}

	@Test
	public void orecToDrecConversionRandomSchemaAndDataBinaryTest()
			throws IOException {
		orecToDrecConvertRandomSchemaAndDataConvert(FileEncoding.BIN);
	}

	public void orecToDrecConvertRandomSchemaAndDataConvert(
			FileEncoding encoding) throws IOException {
		// AvroTestTraceAspect.activate(System.err);
		File schemaFile = getTempFile("OrecRandomSchema.avpr.json");
		generateRandomOrecSchema(schemaFile,
				new Random(System.currentTimeMillis()), 3); // 5
		Schema orecSchema = getSchema(schemaFile);
		Schema drecSchema = getSchema("DrecSchema.avpr.json");
		File originalFile = getTempFile("OrecRandomData.avro.json");
		File resultFile = getTempFile("AvroTestDrecToOrec.avro.json");
		File tempDrecFile = getTempFile("AvroTestOrecToDrec.avro.json");
		generateRandomData(orecSchema, originalFile, 3, encoding); // 5
		orecToDrecConversion(orecSchema, drecSchema, originalFile, resultFile,
				tempDrecFile, encoding, encoding);
	}


	private void generateRandomSchemaForScalar(StringBuffer sch, Random random,
			String type) {
		sch.append("{\"name\": \"");
		generateRandomSchemaForName(sch, random);
		sch.append("\", \"type\": \"");
		sch.append(type);
		sch.append("\"}");
	}

	private void generateRandomSchemaForName(StringBuffer schema, Random random) {
		int i = 5 + random.nextInt(15);
		schema.append("a");
		do
			schema.append((char) (random.nextInt('z' - 'a') + 'a'));
		while (i-- > 0);
	}

	private void generateRandomSchemaForRecord(StringBuffer schema,
			Random random) {
		schema.append("{\"name\": \"");
		generateRandomSchemaForName(schema, random);
		schema.append("\", \"type\":{\"name\":\"");
		generateRandomSchemaForName(schema, random);
		schema.append("\", \"type\":\"record\", \"fields\" : [");
	}

	private void generateRandomSchemaForArrayedRecord(StringBuffer schema,
			Random random) {
		schema.append("{\"name\": \"");
		generateRandomSchemaForName(schema, random);
		schema.append("\", \"type\":\"record\", \"fields\" : [");
	}

	private void generateRandomSchemaForArray(StringBuffer schema, Random random) {
		schema.append("{\"name\":\"");
		generateRandomSchemaForName(schema, random);
		schema.append("\", \"type\": {\"type\": \"array\", \"items\":");
	}

	private void generateRandomOrecSchemaRecursive(StringBuffer schema,
			Random random, int depth) {
		// we are here under a record and record may have other records, arrays
		// or scalara as children
		for (int i = 5; i >= 0; i--) {
			// array?
			if ((depth-- > 0)
					|| (random.nextBoolean() && random.nextBoolean() && random
							.nextBoolean())) {
				generateRandomSchemaForArray(schema, random);
				if (random.nextBoolean()) {
					generateRandomSchemaForArrayedRecord(schema, random);
					generateRandomOrecSchemaRecursive(schema, random, depth);
					schema.append("]}");
				} else if (random.nextBoolean())
					generateRandomSchemaForScalar(schema, random, "int");
				else
					generateRandomSchemaForScalar(schema, random, "string");
				schema.append("}}");
			} else if (((depth > 0)) && random.nextBoolean()
					&& random.nextBoolean()) { // record?
				generateRandomSchemaForRecord(schema, random);
				generateRandomOrecSchemaRecursive(schema, random, depth);
				schema.append("]}}");
			} else { // scalar?
				if (random.nextBoolean())
					generateRandomSchemaForScalar(schema, random, "int");
				else
					generateRandomSchemaForScalar(schema, random, "string");
			}
			if (i > 0)
				schema.append(',');
		}
	}

	private void generateRandomOrecSchema(File schemaFile, Random random,
			int depth) throws IOException {
		StringBuffer sch = new StringBuffer();
		sch.append("{\"type\": \"array\", \"items\": {\"name\": \"Document\", \"type\": \"record\", \"fields\" : [ ");
		generateRandomOrecSchemaRecursive(sch, random, depth);
		sch.append("]}}");
		FileUtils.writeStringToFile(schemaFile, sch.toString());
	}

	@SuppressWarnings(value = "unchecked")
	private static Object generateRandomDataRecursive(Schema schema,
			Random random, int size) {
		switch (schema.getType()) {
		case RECORD:
			GenericRecord record = new GenericData.Record(schema);
			boolean isFieldsEmpty = true;
			for (Schema.Field field : schema.getFields()) {
				Object o = generateRandomDataRecursive(field.schema(), random,
						size);
				if (o != null) {
					record.put(field.name(), o);
					isFieldsEmpty = isFieldsEmpty && o instanceof GenericArray
							&& ((GenericArray<Object>) o).size() == 0;
				}
			}
			return isFieldsEmpty ? null : record;
		case ARRAY:
			int length = size + (random.nextInt(10));
			GenericArray<Object> array = new GenericData.Array<Object>(
					length <= 0 ? 0 : length, schema);
			Object o;
			for (int i = 0; i < length; i++) {
				o = generateRandomDataRecursive(schema.getElementType(),
						random, size > 0 ? size - 1 : 0);
				if (o != null)
					array.add(o);
			}
			return array;
		case STRING:
			return generateRandomUtf8(random, 40);
		case INT:
			return random.nextInt();
		case LONG:
			return random.nextLong();
		case FLOAT:
			return random.nextFloat();
		case DOUBLE:
			return random.nextDouble();
		case BOOLEAN:
			return random.nextBoolean();
		default:
			throw new RuntimeException("Unknown type: " + schema);
		}
	}

	private static Utf8 generateRandomUtf8(Random rand, int maxLength) {
		Utf8 utf8 = new Utf8().setLength(rand.nextInt(maxLength));
		for (int i = 0; i < utf8.getLength(); i++) {
			utf8.getBytes()[i] = (byte) ('a' + rand.nextInt('z' - 'a'));
		}
		return utf8;
	}

	@SuppressWarnings("unchecked")
	private void generateRandomData(Schema inSchema, File outFile, int size,
			Drec.FileEncoding encoding) throws IOException {

		FileUtils.deleteQuietly(outFile);

		Encoder encoder;
		switch (encoding) {
		case JSON:
			encoder = new JsonEncoder(inSchema, new FileOutputStream(outFile));
			break;
		case BIN:
		default:
			encoder = new BinaryEncoder(new FileOutputStream(outFile));

		}
		DatumWriter<Array<GenericRecord>> writer = new GenericDatumWriter<Array<GenericRecord>>(
				inSchema);
		writer.write(
				(Array<GenericRecord>) generateRandomDataRecursive(inSchema,
						new Random(System.currentTimeMillis()), size), encoder);
		encoder.flush();
	}
}
