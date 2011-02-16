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

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.Schema.Type;

import dremel.common.Drec.ScannerFacade;
import dremel.common.Drec.ScannerFacade.ColumnScanner;

/**
 * 
 * class Expression represents value expressions in BQL. For example in query
 * "select a+b+sin(c)-sum(d)*avg(e) from table1" the "a+b+sin(c)-sum(d)*avg(e)"
 * is an expression.
 * 
 * Expressions are parsed and then regenerated into java valid expression string
 * and then a newly generated java expression in string form is dynamically
 * compiled to bytecode and the bytecode is executed
 * 
 * 
 * @author camuelg
 */
final class Expression<T> {
	JavaLangScript script;
	ScannerFacade.ColumnScanner params[];
	ScannerFacade.ColumnScanner param;

	
	List<String> expressionColumnNames = new LinkedList<String>();
	
	public List<String> getExpressionColumnNames()
	{
		return expressionColumnNames;
	}
	
	public Type getExpressionType()
	{
		// TODO to make proper implementation, for now - hard coded INT
		return Type.INT;
	}
	
	@SuppressWarnings("unchecked")
	public T evaluate() throws InvocationTargetException {
		return (T) script.evaluate(params);
	}

	Expression(AstNode expr, ScannerFacade scanner) {
		// System.out.println("Expression "+ expr.toStringTree());
		// NEXTTODO
		// Query parse and resolve parameters by finding respective
		// columnScanner in
		// scannerFacade and making a reference to it directly.

		// TODO - make real implementation
		// Url column

		// Script is compiled java implementation of the BQL expression
		// input parameters is data from relevant columns. Output is scalar.

		String columnName = Query
				.extractColumnNameFromSingleColumnExpression(expr);
		
		expressionColumnNames.add(columnName);

		ColumnScanner selectedColumn = (ColumnScanner) scanner.rtree
				.getColumnByName(columnName);
		params = new ScannerFacade.ColumnScanner[1];
		params[0] = selectedColumn;
		param = selectedColumn;

		// create script
		try {
			script = new JavaLangScript(
					AvroExperiments.buildIdentityScriptForIntColumn());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("create script failed", e);
		}

	}
}