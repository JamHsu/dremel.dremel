/**
   Copyright 2010, Petascan Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.Ope
*/

grammar Bql;

options {output=AST; ASTLabelType=CommonTree; backtrack=false; k=1;}

tokens {
N_SELECT_STATEMENT; N_FROM; N_SELECT; N_WHERE; N_GROUPBY; N_ORDERBY; N_LIMIT; N_COLUMN; N_CREATE_COLUMN; 
N_ALIAS; N_WITHIN; N_TABLE; N_WITHIN_RECORD;N_ASC;N_DESC;N_TABLE_NAME;N_COLUMN_NAME;
N_IN; N_CALL; N_OP;N_LOGICAL_OR;N_LOGICAL_AND;N_BITWISE_OR;N_BITWISE_XOR;N_BITWISE_AND;N_EQUAL;N_NOT_EQUAL;
N_LESS_THAN;N_LESS_THAN_OR_EQUAL;N_GREATER_THAN;N_LOGICAL_NOT;N_BITWISE_NOT;N_CONTAINS;N_REMAINDER; 
N_GREATER_THAN_OR_EQUAL;N_BITWISE_RIGHT_SHIFT;N_DIVIDE;N_MULTIPLY;N_SUBSTRUCT;N_ADD;N_BITWISE_RIGHT_SHIFT;
N_BITWISE_LEFT_SHIFT;N_IN_PARAMS;N_CALL_PARAMS;N_INT; N_FLOAT; N_STRING;N_EXPRESSION;}

@header {package dremel.common;}

@lexer::header {package dremel.common;}

//Starting production 
request 				: 	selectStatement (SEMICOLON!)? EOF!;					//embedded rewrite

//Select statement
selectStatement 			: 	selectClause fromClause whereClause? 
							groupbyClause? orderbyClause? limitClause?			-> 	^(N_SELECT_STATEMENT fromClause selectClause whereClause? groupbyClause? orderbyClause? limitClause?);

selectClause  				:	SELECT columnExpr (COMMA columnExpr)*	 				-> 	^(N_SELECT columnExpr+);
columnExpr		 		:	expression withinClause? (AS columnName)? 				-> 	^(N_CREATE_COLUMN expression ^(N_ALIAS columnName)? withinClause? );
withinClause				:	(WITHIN (RECORD  							-> 	^(N_WITHIN_RECORD)
							| columnPath							->	^(N_WITHIN columnPath)
							));

fromClause 				:	FROM subSelectStatement (COMMA subSelectStatement)*	 		-> 	^(N_FROM subSelectStatement+);
subSelectStatement			:	(tableName | (LPAREN! selectStatement RPAREN!));			//embedded rewrite

whereClause				:	WHERE expression 							-> 	^(N_WHERE expression);

groupbyClause  				:	GROUPBY columnName (COMMA columnName)* 					-> 	^(N_GROUPBY columnName+);

orderbyClause  				:	( ORDERBY orderbyColumnName (COMMA orderbyColumnName)*)			-> 	^(N_ORDERBY orderbyColumnName);
orderbyColumnName			:	columnName (ASC 							-> 	^(N_ASC columnName)
							| DESC								->	^(N_DESC columnName)
							|				/* default sort order */	->	^(N_ASC columnName) 
							);						

limitClause  				:	( LIMIT INT )								->	^(N_LIMIT INT);

///Table names
columnPath				:	columnPath2 								-> 	^(N_COLUMN columnPath2);
columnPath2				:	columnName (DOT! columnName)*;						//embedded rewrite
columnName 				:	ID -> ^(N_COLUMN_NAME ID) | STAR -> ^(N_COLUMN_NAME STAR);		//embedded rewrite

//Table names
tablePath				:	tablePath2 								->	^(N_TABLE tablePath2);
tablePath2				:	tableName (DOT! tableName)*;						//embedded rewrite
tableName 				:	ID -> ^(N_TABLE_NAME ID);						//embedded rewrite

//Expressions
expression				:	expression2 								-> 	^(N_EXPRESSION expression2);
expression2				:	(a=binary10thPrcdExpr->$a) (o=binary11thPrcdOp b=binary10thPrcdExpr 	-> 	^($o $expression2 $b))*;
binary10thPrcdExpr			:	(a=binary9thPrcdExpr->$a) (o=binary10thPrcdOp b=binary9thPrcdExpr 	-> 	^($o $binary10thPrcdExpr $b))*;
binary9thPrcdExpr			:	(a=binary8thPrcdExpr->$a) (o=binary9thPrcdOp b=binary8thPrcdExpr 	-> 	^($o $binary9thPrcdExpr $b))*;
binary8thPrcdExpr			:	(a=binary7thPrcdExpr->$a) (o=binary8thPrcdOp b=binary7thPrcdExpr 	-> 	^($o $binary8thPrcdExpr $b))*;
binary7thPrcdExpr			:	(a=binary6thPrcdExpr->$a) (o=binary7thPrcdOp b=binary6thPrcdExpr 	-> 	^($o $binary7thPrcdExpr $b))*;
binary6thPrcdExpr			:	(a=binary5thPrcdExpr->$a) (o=binary6thPrcdOp b=binary5thPrcdExpr 	-> 	^($o $binary6thPrcdExpr $b))*;
binary5thPrcdExpr			:	(a=binary4thPrcdExpr->$a) (o=binary5thPrcdOp b=binary4thPrcdExpr 	-> 	^($o $binary5thPrcdExpr $b))*;
binary4thPrcdExpr			:	(a=binary3rdPrcdExpr->$a) (o=binary4thPrcdOp b=binary3rdPrcdExpr 	-> 	^($o $binary4thPrcdExpr $b))*;
binary3rdPrcdExpr			:	(a=binary2ndPrcdExpr->$a) (o=binary3rdPrcdOp b=binary2ndPrcdExpr 	-> 	^($o $binary3rdPrcdExpr $b))*;
binary2ndPrcdExpr			:	(a=binary1stPrcdExpr->$a) (o=binary2ndPrcdOp b=binary1stPrcdExpr 	-> 	^($o $binary2ndPrcdExpr $b))*;
binary1stPrcdExpr			:	(a=unaryPrefixExpr->$a)   (o=binary1stPrcdOp b=unaryPrefixExpr 		-> 	^($o $binary1stPrcdExpr $b))*;
unaryPrefixExpr				:	(o=unaryPrefixOp->^($o $unaryPrefixExpr))* (a=unaryPostfixExpr		->	$a); 
unaryPostfixExpr			:	(a=atomExpr->$a) (o=unaryPostfixOp					-> 	^($o $unaryPostfixExpr))*;
atomExpr	 			:	INT 									-> 	^(N_INT INT)
						| FLOAT 								-> 	^(N_FLOAT FLOAT) 
						| STRING 								-> 	^(N_STRING STRING)  
						| (LPAREN expression RPAREN) 						->   	expression
						| columnPath;

//Ops


binary11thPrcdOp			:	LOGICAL_OR								->	N_LOGICAL_OR;	
binary10thPrcdOp			:	LOGICAL_AND								->	N_LOGICAL_AND;
binary9thPrcdOp				:	BITWISE_OR								->	N_BITWISE_OR;
binary8thPrcdOp				:	BITWISE_XOR								->	N_BITWISE_XOR;
binary7thPrcdOp				:	BITWISE_AND								->	N_BITWISE_AND;
binary6thPrcdOp				:	EQUAL									->	N_EQUAL 
						| NOT_EQUAL								->	N_NOT_EQUAL;
binary5thPrcdOp				:	LESS_THAN								->	N_LESS_THAN 
						| LESS_THAN_OR_EQUAL							->	N_LESS_THAN_OR_EQUAL 
						| GREATER_THAN								->	N_GREATER_THAN 
						| GREATER_THAN_OR_EQUAL							->	N_GREATER_THAN_OR_EQUAL;
binary4thPrcdOp				:	BITWISE_LEFT_SHIFT							->	N_BITWISE_LEFT_SHIFT 
						| BITWISE_RIGHT_SHIFT							->	N_BITWISE_RIGHT_SHIFT;
binary3rdPrcdOp				:	ADD									->	N_ADD 
						| SUBSTRUCT->N_SUBSTRUCT;
binary2ndPrcdOp				:	multiplyOp								->	N_MULTIPLY 
						| divideOp								->	N_DIVIDE 
						| REMAINDER								->	N_REMAINDER;
binary1stPrcdOp				:	CONTAINS								->	N_CONTAINS; 
unaryPrefixOp				:	BITWISE_NOT								->	N_BITWISE_NOT 
						| LOGICAL_NOT								->	N_LOGICAL_NOT;
unaryPostfixOp				:	unaryPostfixOpIn | unaryPostfixOpCall;
unaryPostfixOpIn			:	IN LPAREN (expression (COMMA expression)*)? RPAREN			->	^(N_IN_PARAMS expression*);
unaryPostfixOpCall			:	LPAREN (expression (COMMA expression)*)? RPAREN 			-> 	^(N_CALL_PARAMS expression*);

//Name clash grapheme ops
divideOp				:	SLASH | DIV;
multiplyOp				:	STAR;


//Keywords
SELECT	 				:	S E L E C T ;
WITHIN					:	W I T H I N ;
RECORD					:	R E C O R D ;
AS					:	A S ;
FROM					:	F R O M ;
WHERE					:	W H E R E ;
GROUPBY					:	G R O U P WS B Y ;	
ORDERBY					:	O R D E R WS B Y;
DESC					:	D E S C;
ASC					:	A S C;
LIMIT					:	L I M I T;

LOGICAL_OR 				:	O R;
LOGICAL_AND				:	A N D;
LOGICAL_NOT				:	N O T;
CONTAINS 				:	C O N T A I N S ;
IN					:	I N;

//Graphemes
BITWISE_AND 				: 	'&';
BITWISE_NOT				: 	'~';
BITWISE_OR				: 	'|';
BITWISE_XOR 				: 	'^';
EQUAL 					: 	'=' | '==';
NOT_EQUAL 				: 	'<>' | '!=';
LESS_THAN_OR_EQUAL 			:	'<=';
LESS_THAN 				: 	'<';
GREATER_THAN_OR_EQUAL 			: 	'>=';
GREATER_THAN 				: 	'>';
SLASH 					: 	'/';
DIV					:	' D I V';
STAR					: 	'*';
ADD 					: 	'+';
SUBSTRUCT 				:	'-';
REMAINDER				: 	'%';
BITWISE_LEFT_SHIFT 			: 	'<<';
BITWISE_RIGHT_SHIFT 			: 	'>>';
DOT 					: 	'.'; 
COLON 					: 	':';
COMMA 					: 	',';
SEMICOLON				: 	';';
LPAREN 					: 	'(';
RPAREN 					: 	')';
LSQUARE 				: 	'[' ;
RSQUARE 				: 	']' ;
LCURLY 					: 	'{';
RCURLY 					: 	'}';

//Lexemes
ID					:	F_ID1 | F_ID2;
fragment F_ID1 				:	('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_')* ;
fragment F_ID2				: 	'[' ('a'..'z'|'A'..'Z'|'0'..'9'|'_'|' '|'\\'|'/'|'-'|'+'|'*'|'.'|':'|'$')* ']';
INT 					:	'0'..'9'+ ;
FLOAT    				:   	('0'..'9')+ '.' ('0'..'9')* F_EXPONENT?    |   '.' ('0'..'9')+ F_EXPONENT?    |   ('0'..'9')+ F_EXPONENT    ;
STRING		    			:	('"' ( ~('"') )* '"') | ('\'' ( ~('\'') )* '\'');
fragment F_EXPONENT 			: 	('e'|'E') ('+'|'-')? ('0'..'9')+ ;
fragment F_HEX_DIGIT 			: 	('0'..'9'|'a'..'f'|'A'..'F') ;

//Whitespace
WS  					:   	( ' ' | '\t' | '\r' | '\n' ) {$channel=HIDDEN;};
    
//Comments
COMMENT					:   	'//' ~('\n'|'\r')* '\r'? '\n' {$channel=HIDDEN;} | '/*' ( options {greedy=false;} : . )* '*/' {$channel=HIDDEN;};

//Case insensetive letters, pretty ugly (or beautiful) but I haven't found a better way, sorry
fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');    
