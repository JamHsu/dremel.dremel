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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.RecognitionException;
import org.apache.avro.Schema;

import dremel.common.Drec.ScannerFacade;
import dremel.common.Drec.Table;
import dremel.common.WriterFacadeImpl.WriterTree;


/**
 * Build and compile execution plan with resolved column references. Can be named executionPlan Builder.
 * @author David.Gruzman
 *
 */
public final class Query {
	ScannerFacade scanner = null;
    Schema outSchema = null;
    AstNode queryTreeRootNode = null;
    
    WriterFacadeImpl.WriterTree wtree = new WriterTree(0, true);
		   
    Set<Table> tables = new LinkedHashSet<Table>();
    Set<Query> subqueries = new LinkedHashSet<Query>();
    Map<String, Expression<Object>> expressions = new LinkedHashMap<String, Expression<Object>>(); // map from the alias to the expression
    Expression<Boolean> filter;
       
    void error(String mes, AstNode node) {
    	throw new RuntimeException(mes + "  in line: "+node.getLine()+" at position "+node.getCharPositionInLine());
    }
    
    
    void parseSelectStatement(AstNode node) {
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
    			 parseWithinRecordClause((AstNode)node.getChild(1));
    		else {
    				assert(false);
    	  		}
    	}else {
			// there is only column name
			alias.append(extractColumnNameFromSingleColumnExpression(node));
		}
    	System.out.println(" alias is "+ alias.toString());
    	System.out.println(" column AST is "+ node.toStringTree());
    	    	    	
		expressions.put(alias.toString(), new Expression<Object>((AstNode)node, scanner));
    };

    /**
     * Takes AST of the part of the select which contains only one column. It looks like this:
     * (N_CREATE_COLUMN (N_EXPRESSION (N_COLUMN (N_COLUMN_NAME Url))))
     * And return the name of the column. In this case: Url
     * @param node - AST node to look for the column name
     * @return column name
     */
    public static String extractColumnNameFromSingleColumnExpression(AstNode node) {
    	System.out.println(node.toStringTree());
    	AstNode expressionNode = (AstNode) node.getChild(0);
    	assert(expressionNode.getType() == BqlParser.N_EXPRESSION);
    
    	AstNode columnNode = (AstNode) expressionNode.getChild(0);
    	assert(columnNode.getType() == BqlParser.N_COLUMN);
    	StringBuilder dotSeparatedColumnName = new StringBuilder();
    	
    	for(int i=0; i<columnNode.getChildCount(); i++)
    	{
    		AstNode nextChild = (AstNode) columnNode.getChild(i);
    	
    		assert(nextChild.getType() == BqlParser.N_COLUMN_NAME);
    		dotSeparatedColumnName.append(nextChild.getChild(0));
    
    		if(i !=columnNode.getChildCount()-1)
    		{
    			dotSeparatedColumnName.append(".");
    		}
    	}
    	    	
		return dotSeparatedColumnName.toString();
	}

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
        
        outSchema = inferOutSchema();
        parseSelectStatement(queryTreeRootNode);
    }
    
	private Schema inferOutSchema() {
		// NEXTTODO infer output schema
		// HARDCODED OUTPUT Schema equals to input schema
		return  InferSchemaAlgorithm.inferSchema(scanner.getDataSetSchema(), queryTreeRootNode);
	}
	
	/**
	 * Executes this query, and write results into the given WriteFacade.
	 * @param writeFacade
	 */
	public void executeQuery(WriterFacade writeFacade)
	{
		// induce output schema 
		// calculate mapping between expressions and writeFacade columns.
		
		// Expression 
		// to define trigger events for the aggregation calculation. 
		// iterate over input data until the end.
	}
	
    public static AstNode getSelectClause(AstNode selectQueryNode) {
    	assert(selectQueryNode.getType() == BqlParser.N_SELECT_STATEMENT);
    	AstNode selectClause = (AstNode)selectQueryNode.getChild(1);
    	assert(selectClause.getType() == BqlParser.N_SELECT);
		return selectClause;
	}
    
	
}
