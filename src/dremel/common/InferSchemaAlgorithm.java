package dremel.common;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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


import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

public class InferSchemaAlgorithm {
		
	/**
	 * This is main method of this class. It takes input Avro schema and create avro schema which
	 * matches the result of the given query
	 * @param inputSchema - Avro schema to be processed
	 * @param query - query to be used
	 * @return Avro schema of the query result
	 */
	public static Schema inferSchema(Schema inputSchema, String query)
	{
		// parse query		
		AstNode tmpQueryTreeRootNode = DremelParser.parseBql(query);
		
		// delegate to inferSchema
		return inferSchema(inputSchema, tmpQueryTreeRootNode);
	}
	
	 /**
	 * @param inputSchema
	 * @param query
	 */
	public static Schema inferSchema(Schema inputSchema, AstNode query)
	{ 		
		// split select clause into separate expression.
		Map<String, AstNode> selectClauseExpressions = getSelectClauseExpressions(query);
		
		Map<String, Schema.Type> expressionTypes = induceExpressionTypes(selectClauseExpressions);
		
		// maps each expression to the path in the schema where the result of this expression should be emitted
		// by path in schema we mean "slim" schema from the root to the given column
		Map<String, SchemaTree> aliasToMostSignificantColumnPaths =  mapExpressionsToSignificanSchemaNodePaths(inputSchema, selectClauseExpressions);
		
		// build schema as a merge of paths from the expressions
		Schema outSchema = buildSchemaAsMergeOfPaths(aliasToMostSignificantColumnPaths, expressionTypes);
							
		return outSchema;
	}
		
	/**
	 * This method should build schema which includes all paths. In the end of each path it should 
	 * build node with names -> aliases of the expression, and types given is the expression types map.
	 * @param aliasToMostSignificantColumnPaths - map from the alias to the path from the aliased exception to the root.
	 * @param expressionTypes - map from the expression aliases to their avro schema types.
	 * @return avro schema with all paths + expressions.
	 */
	private static Schema buildSchemaAsMergeOfPaths(
			Map<String, SchemaTree> aliasToMostSignificantColumnPaths,
			Map<String, Type> expressionTypes) {

		// build schema objects for all expression nodes - as a result we should get paths ready for the merge
			
		// on each level - group equal schemas - and have a map from the node to its group
		SchemaTree tree = buildTreeFromPaths(aliasToMostSignificantColumnPaths);
						
		Schema result = tree.getAsAvroSchema();

		return result;
	}

	private static SchemaTree buildTreeFromPaths(
			Map<String, SchemaTree> aliasToMostSignificantColumnPaths) {
		
		SchemaTree resultTree = null;
		
		boolean isFirst = true;
		
		for(String nextExpressionAlias : aliasToMostSignificantColumnPaths.keySet())
		{
			SchemaTree nextPath = aliasToMostSignificantColumnPaths.get(nextExpressionAlias);
			if(isFirst)
			{
				resultTree = nextPath;// pathToTree(nextPath,"DATASET");
			}else
			{
				mergePathToTree(resultTree, nextPath);
			}
			isFirst = false;
		}

		return resultTree;
	}

	/**
	 * Merge the given path to the given Schema Tree. 
	 * @param resultTree
	 * @param nextPath
	 */
	private static void mergePathToTree(SchemaTree resultTree,
			SchemaTree pathTree) {
			//SchemaTree pathTree = pathToTree(path, "DATASET");
			
			resultTree.mergeWithTree(pathTree);							
	}


	private static Map<String, Type> induceExpressionTypes(
			Map<String, AstNode> selectClauseExpressions) {
		
		Map<String, Type> result = new HashMap<String, Type>();
		
		for(String alias : selectClauseExpressions.keySet())
		{
			// can be only primitive schema types
			Schema.Type expressionType = induceExpressionType(selectClauseExpressions.get(alias));
			result.put(alias, expressionType);
		}

		return result;
	}
	
	/**
	 * This method should analyse the expression in order to determine the place in the input schema  where result of this
	 * expression should be emitted.
	 * Known cases are:
	 * a) Expression. In this case we should select most repeated column mentioned in the expression.
	 * b) Aggregation. In this case within clause should define the parent node of the result. In this case we do have to add new node to
	 * @param inputSchema
	 * @param selectClauseExpressions
	 * @return map from expression aliases to the paths in the inputSchema from the root to.
	 */
	private static Map<String, SchemaTree> mapExpressionsToSignificanSchemaNodePaths(			
			Schema inputSchema, Map<String, AstNode> selectClauseExpressions) {
		
		Map<String, SchemaTree> result = new HashMap<String, SchemaTree>();
		
		for(String alias : selectClauseExpressions.keySet())
		{
			SchemaTree significantColumn = getSignificantColumnPathForExpression(inputSchema, selectClauseExpressions.get(alias), alias);
			result.put(alias, significantColumn);
		}
		return result;
	}

			
	/**
	 * This method should analyse the given expression,find the column which determine the place where expression should 
	 * emit its output. 
	 * @param inputSchema - schema of the input data for the query, which contain the given expression
	 * @param astNode - AST node for the expression to be analysed
	 * @param alias - alias of the expression, used in the query. (probabbly redundant - to delete if can be read from the AST node)
	 * @return node in the inputSchemam which corresponds to the result of the given expression
	 */
	private static SchemaTree getSignificantColumnPathForExpression(Schema inputSchema,
			AstNode expressionNode, String alias) {
		// to deduce expression's type
		 
		// to get list of all columns in the expression
		List<String> columnNames = getColumnNamesFromExpression(expressionNode);
		// for each column to find corresponding place in the schema
		Map<String, SchemaTree> columnToPathInSchema = mapColumnsToPathInSchema(inputSchema, columnNames);
		// for each column to find repetition level
		Map<String, Integer> columnToRepetitionLevel = mapColumnsToRepetitionLevels(columnToPathInSchema);
		// to find most repeated column,
		String mostRepeatedColumnName = getMostRepeatedColumn(columnToRepetitionLevel);
		
		// to create node under the most repeated column with
		SchemaTree mostRepeatedPath = columnToPathInSchema.get(mostRepeatedColumnName);
		assert(mostRepeatedPath != null);
						
		return mostRepeatedPath;
	}

	/**
	 * Finds column with maximum repetition level
	 * @param columnToRepetitionLevel map from column name to its tepetition level
	 * @return name of the column with max repetition level
	 */
	private static String getMostRepeatedColumn(
			Map<String, Integer> columnToRepetitionLevel) {
		assert(columnToRepetitionLevel.size()>0);
		
		int maxRepetitionLevel = -1;
		String maxRepetitionColumn = null;
		
		for(String columnName : columnToRepetitionLevel.keySet())
		{			
			if(maxRepetitionLevel < columnToRepetitionLevel.get(columnName).intValue())
			{
				maxRepetitionLevel = columnToRepetitionLevel.get(columnName).intValue();
				maxRepetitionColumn = columnName;
			}
		}
		
		assert(maxRepetitionColumn != null);
		return maxRepetitionColumn;
	}

	/**
	 * Find for each column what is its maximum repetition level
	 * @param columnToPathInSchema - map from the column name to the path in the schema to reach this column.
	 * @return
	 */
	private static Map<String, Integer> mapColumnsToRepetitionLevels(
			Map<String, SchemaTree> columnToPathInSchema) {
		
		Map<String, Integer> result = new HashMap<String, Integer>();
		for(String columnName : columnToPathInSchema.keySet())
		{
			//TODO to make real calculation of the repetition level
			int repetitionLevel = 3;//calculateRepetitionLevelFromPath(columnToPathInSchema.get(columnName));
			result.put(columnName, repetitionLevel);
		}
		return result;
	}

	private static Map<String, SchemaTree> mapColumnsToPathInSchema(
			Schema inputSchema, List<String> columnNames) {
		
		Map<String, SchemaTree> columnNameToPointInSchema = new HashMap<String, SchemaTree>();
		for(String columnName : columnNames)
		{
			SchemaTree pointInSchema = findColumnInSchema2(inputSchema, columnName);
			columnNameToPointInSchema.put(columnName, pointInSchema);
		}
		
		return columnNameToPointInSchema;
	}

	private static SchemaTree findColumnInSchema2(Schema inputSchema,
			String columnName)
		{
			String recordType = inputSchema.getElementType().getName();
			String columnNameWithRecordName = recordType+"."+columnName; 
			String[] nameParts  = columnNameWithRecordName.split("\\.");
			
			List<String> namePartsList = arrayToList(nameParts);
		
			SchemaTree inputAsSchemTree = SchemaTree.createFromAvroSchema(inputSchema);
															
			inputAsSchemTree.makeProjection(namePartsList);			
			
			return inputAsSchemTree; 
	}
	
	private static List<String> arrayToList(String[] stringArray) {
		List<String> result = new LinkedList<String>();
		for(String element : stringArray)
		{
			result.add(element);
		}
		return result;
	}
	
	private static List<String> getColumnNamesFromExpression(
			AstNode expressionNode) {
		// TODO To make proper implementation - for now - works with single columns only
		List<String> columnNames = new LinkedList<String>();
		columnNames.add(Query.extractColumnNameFromSingleColumnExpression(expressionNode));
		
		return columnNames;
	}

	private static Type induceExpressionType(AstNode expressionNode) {
		// TODO To make proper implementation for now - hard coded int
		return Schema.Type.INT;
	}

	// TODO Make real implementation. For now - will process correctly only single columns as expressions
	private static Map<String, AstNode> getSelectClauseExpressions(AstNode query) {
		
		AstNode selectClause = Query.getSelectClause(query);
		
		Map<String, AstNode> aliasToSelectExpressions = new HashMap<String, AstNode>();
		
		int count = selectClause.getChildCount();
    	assert(count > 0);
    	for(int i = 0; i < count; i++) {
    		AstNode currentExpression = (AstNode)selectClause.getChild(i);    		
    		String columnname = Query.extractColumnNameFromSingleColumnExpression(currentExpression);	
    		aliasToSelectExpressions.put(columnname, currentExpression);
    	}
		
    	return aliasToSelectExpressions;
	}

}
