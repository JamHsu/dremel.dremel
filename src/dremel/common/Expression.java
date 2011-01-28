package dremel.common;

import java.lang.reflect.InvocationTargetException;

import org.codehaus.commons.compiler.CompileException;

import dremel.common.Drec.ScannerFacade;
import dremel.common.Drec.ScannerFacade.ColumnScanner;
import dremel.playground.AvroExperiments;

final class Expression<T> {
	JavaLangScript script;
	ScannerFacade.ColumnScanner params[];
	ScannerFacade.ColumnScanner param;
	
	@SuppressWarnings("unchecked")
	public T evaluate() throws InvocationTargetException {
		return (T) script.evaluate(params);
	}
	
	Expression(AstNode expr, ScannerFacade scanner) {
	//System.out.println("Expression "+ expr.toStringTree());
	//NEXTTODO
	// Query parse and resolve parameters by finding respective columnScanner in 
	// scannerFacade and making a reference to it directly.
	
	//TODO - make real implementation
	// Url column
	
					
	//Script is compiled java implementation of the BQL expression 
	// input parameters is data from relevant columns. Output is scalar.
		
		String columnName = Query.extractColumnNameFromSingleColumnExpression(expr);
		
		
		ColumnScanner selectedColumn = (ColumnScanner) scanner.rtree.getColumnByName(columnName);
		params = new ScannerFacade.ColumnScanner[1];
		params[0] = selectedColumn;
		param=selectedColumn;
		
		// create script
		try {
			script = new JavaLangScript(AvroExperiments.buildIdentityScriptForIntColumn());
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("create script failed", e);
		}
		
		
		
	}
}