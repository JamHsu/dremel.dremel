package dremel.common;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.RecognitionException;
import org.apache.avro.Schema;

import dremel.common.Drec.Expression;
import dremel.common.Drec.ScannerFacade;
import dremel.common.Drec.Table;


/**
 * Build and compile execution plan with resolved column references. Can be named executionPlan Builder.
 * @author David.Gruzman
 *
 */
public final class Query {
	ScannerFacade scanner = null;
    Schema outSchema = null;
    AstNode queryTreeRootNode = null;
    
    Set<Table> tables = new LinkedHashSet<Table>();
    Set<Query> subqueries = new LinkedHashSet<Query>();
    Map<String, Expression<Object>> expressions = new LinkedHashMap<String, Expression<Object>>();
    Expression<Boolean> filter;
       
    void error(String mes, AstNode node) {
    	throw new RuntimeException(mes + "  in line: "+node.getLine()+" at position "+node.getCharPositionInLine());
    }
    
    void parseSelectStatement(AstNode node) {
    	//System.out.println(node.toStringTree());
    	
    	assert(node.getType() == BqlParser.N_SELECT_STATEMENT);
    	int count = node.getChildCount();
    	assert((count >= 2) && (count <= 3));
    	parseFromClause((AstNode)node.getChild(0));
    	parseSelectClause((AstNode)node.getChild(1));
		parseWhereClause((AstNode)node.getChild(2));
    };
    void parseFromClause(AstNode node) {
    	assert(node.getType() == BqlParser.N_FROM);
    	int count = node.getChildCount();
    	assert(count > 0);
    	for(int i = 0; i < count; i++) {
    		AstNode node2 = (AstNode)node.getChild(i);
    		if(node2.getType() == BqlParser.N_TABLE_NAME) {
    			tables.add(new Table(node2.getText()));
    		} else if (node2.getType() == BqlParser.N_SELECT_STATEMENT){ // Subqueries support 
    			subqueries.add(new Query(scanner, node2));
    		} else assert(false);
    	}
    };
    void parseSelectClause(AstNode node) {
    	assert(node.getType() == BqlParser.N_SELECT);
    	int count = node.getChildCount();
    	assert(count > 0);
    	for(int i = 0; i < count; i++) {
    		parseCreateColumn((AstNode)node.getChild(i));
    	}
    };
    void parseWhereClause(AstNode node) {
    	if(node == null) {
    		filter = null;
    		return;
    	}
    	assert(node.getType() == BqlParser.N_WHERE);
    	int count = node.getChildCount();
    	assert(count == 1);
    	filter = new Expression<Boolean>((AstNode)node.getChild(0), scanner);
    };
    void parseCreateColumn(AstNode node) {
    	StringBuffer alias = new StringBuffer();
    	
    	StringBuffer within = new StringBuffer();
    	boolean isWithinRecord = false;
    	assert(node.getType() == BqlParser.N_CREATE_COLUMN);
    	int count = node.getChildCount();
    	assert((count >= 1) && (count <= 3));
    	if(count == 3) {
    		parseColumnAlias((AstNode)node.getChild(1), alias);
    		parseWithinClause((AstNode)node.getChild(2), within);
    	} else if(count == 2) {
    		if(node.getChild(1).getType() == BqlParser.N_ALIAS)
    			parseColumnAlias((AstNode)node.getChild(1), alias);
    		else if(node.getChild(1).getType() == BqlParser.N_WITHIN)
    			parseWithinClause((AstNode)node.getChild(1), within);
    		else if(node.getChild(1).getType() == BqlParser.N_WITHIN_RECORD)
    			isWithinRecord = parseWithinRecordClause((AstNode)node.getChild(1));
    		else assert(true);
    	}
		expressions.put(alias.toString(), new Expression<Object>((AstNode)node.getChild(0), scanner));
    };

    private boolean parseWithinRecordClause(AstNode node) {
    	assert(node.getType() == BqlParser.N_WITHIN_RECORD);
    	return true;
	}

	private void parseWithinClause(AstNode node, StringBuffer within) {
    	assert(node.getType() == BqlParser.N_WITHIN);
    	int count = node.getChildCount();
    	assert((count == 1));
    	within.append(node.getChild(0).getText());
	}

	private void parseColumnAlias(AstNode node, StringBuffer alias) {
    	assert(node.getType() == BqlParser.N_ALIAS);
    	int count = node.getChildCount();
    	assert((count == 1));
    	alias.append(node.getChild(0).getText());
	}

	Query(ScannerFacade scanner, AstNode root) {
	//	System.out.print(scanner);
        init(scanner, root);
    }
	/**
	 * 
	 * @param scanner - data source, will be used in the linking process.
	 * @param queryString - actually the query to be implemented.
	 * @throws RecognitionException
	 */
    Query(ScannerFacade scanner, String queryString) throws RecognitionException {
    	//System.out.println("Query is created for the scanner "+ scanner.toString());
    	AstNode tmpQueryTreeRootNode = DremelParser.parseBql(queryString);
        init(scanner, tmpQueryTreeRootNode);        
    }
    
    private void init(ScannerFacade scanner, AstNode root)
    {
    	this.scanner = scanner;        
        queryTreeRootNode = root;
        parseSelectStatement(queryTreeRootNode);
        outSchema = inferOutSchema();
    }
    
	private Schema inferOutSchema() {
		// NEXTTODO infer output schema
		return null;
	}
	
	/**
	 * Executes this query, and write results into the given WriteFacade.
	 * @param writeFacade
	 */
	public void executeQuery(WriterFacadeImpl writeFacade)
	{
		// calculate mapping between expressions and writeFacade columns.
		// perform the binding???
		// iterate over input data until the end.
	} 
	
}
