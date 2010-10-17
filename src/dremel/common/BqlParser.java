// $ANTLR 3.2 Sep 23, 2009 12:02:23 C:\\work5\\src\\dremel\\common\\Bql.g 2010-09-20 09:15:41
package dremel.common;

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;


import org.antlr.runtime.tree.*;

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
public class BqlParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "N_SELECT_STATEMENT", "N_FROM", "N_SELECT", "N_WHERE", "N_GROUPBY", "N_ORDERBY", "N_LIMIT", "N_COLUMN", "N_CREATE_COLUMN", "N_ALIAS", "N_WITHIN", "N_TABLE", "N_WITHIN_RECORD", "N_ASC", "N_DESC", "N_TABLE_NAME", "N_COLUMN_NAME", "N_IN", "N_CALL", "N_OP", "N_LOGICAL_OR", "N_LOGICAL_AND", "N_BITWISE_OR", "N_BITWISE_XOR", "N_BITWISE_AND", "N_EQUAL", "N_NOT_EQUAL", "N_LESS_THAN", "N_LESS_THAN_OR_EQUAL", "N_GREATER_THAN", "N_LOGICAL_NOT", "N_BITWISE_NOT", "N_CONTAINS", "N_REMAINDER", "N_GREATER_THAN_OR_EQUAL", "N_BITWISE_RIGHT_SHIFT", "N_DIVIDE", "N_MULTIPLY", "N_SUBSTRUCT", "N_ADD", "N_BITWISE_LEFT_SHIFT", "N_IN_PARAMS", "N_CALL_PARAMS", "N_INT", "N_FLOAT", "N_STRING", "N_EXPRESSION", "SEMICOLON", "SELECT", "COMMA", "AS", "WITHIN", "RECORD", "FROM", "LPAREN", "RPAREN", "WHERE", "GROUPBY", "ORDERBY", "ASC", "DESC", "LIMIT", "INT", "DOT", "ID", "STAR", "FLOAT", "STRING", "LOGICAL_OR", "LOGICAL_AND", "BITWISE_OR", "BITWISE_XOR", "BITWISE_AND", "EQUAL", "NOT_EQUAL", "LESS_THAN", "LESS_THAN_OR_EQUAL", "GREATER_THAN", "GREATER_THAN_OR_EQUAL", "BITWISE_LEFT_SHIFT", "BITWISE_RIGHT_SHIFT", "ADD", "SUBSTRUCT", "REMAINDER", "CONTAINS", "BITWISE_NOT", "LOGICAL_NOT", "IN", "SLASH", "DIV", "S", "E", "L", "C", "T", "W", "I", "H", "N", "R", "O", "D", "A", "F", "M", "G", "U", "P", "WS", "B", "Y", "COLON", "LSQUARE", "RSQUARE", "LCURLY", "RCURLY", "F_ID1", "F_ID2", "F_EXPONENT", "F_HEX_DIGIT", "COMMENT", "J", "K", "Q", "V", "X", "Z"
    };
    public static final int STAR=69;
    public static final int LSQUARE=116;
    public static final int N_CALL=22;
    public static final int N_LIMIT=10;
    public static final int CONTAINS=88;
    public static final int N_DESC=18;
    public static final int BITWISE_LEFT_SHIFT=83;
    public static final int N_COLUMN=11;
    public static final int N_LOGICAL_NOT=34;
    public static final int N_CREATE_COLUMN=12;
    public static final int EOF=-1;
    public static final int N_NOT_EQUAL=30;
    public static final int LOGICAL_AND=73;
    public static final int N_GROUPBY=8;
    public static final int RPAREN=59;
    public static final int N_EXPRESSION=50;
    public static final int N_ADD=43;
    public static final int N_BITWISE_NOT=35;
    public static final int BITWISE_XOR=75;
    public static final int NOT_EQUAL=78;
    public static final int N_MULTIPLY=41;
    public static final int N_IN=21;
    public static final int COMMENT=124;
    public static final int SELECT=52;
    public static final int N_BITWISE_XOR=27;
    public static final int GROUPBY=61;
    public static final int N_SUBSTRUCT=42;
    public static final int N_INT=47;
    public static final int D=105;
    public static final int E=95;
    public static final int F=107;
    public static final int G=109;
    public static final int N_WHERE=7;
    public static final int RECORD=56;
    public static final int N_LESS_THAN=31;
    public static final int A=106;
    public static final int B=113;
    public static final int C=97;
    public static final int ASC=63;
    public static final int L=96;
    public static final int M=108;
    public static final int N=102;
    public static final int O=104;
    public static final int H=101;
    public static final int I=100;
    public static final int N_COLUMN_NAME=20;
    public static final int J=125;
    public static final int K=126;
    public static final int U=110;
    public static final int T=98;
    public static final int W=99;
    public static final int V=128;
    public static final int LCURLY=118;
    public static final int Q=127;
    public static final int N_CALL_PARAMS=46;
    public static final int SEMICOLON=51;
    public static final int INT=66;
    public static final int F_ID2=121;
    public static final int P=111;
    public static final int F_ID1=120;
    public static final int S=94;
    public static final int R=103;
    public static final int Y=114;
    public static final int X=129;
    public static final int Z=130;
    public static final int F_HEX_DIGIT=123;
    public static final int WS=112;
    public static final int REMAINDER=87;
    public static final int N_STRING=49;
    public static final int LESS_THAN=79;
    public static final int WITHIN=55;
    public static final int N_WITHIN=14;
    public static final int N_TABLE=15;
    public static final int FROM=57;
    public static final int N_LESS_THAN_OR_EQUAL=32;
    public static final int N_BITWISE_AND=28;
    public static final int N_CONTAINS=36;
    public static final int WHERE=60;
    public static final int BITWISE_AND=76;
    public static final int BITWISE_NOT=89;
    public static final int LIMIT=65;
    public static final int N_BITWISE_RIGHT_SHIFT=39;
    public static final int BITWISE_OR=74;
    public static final int LOGICAL_NOT=90;
    public static final int FLOAT=70;
    public static final int ORDERBY=62;
    public static final int N_DIVIDE=40;
    public static final int F_EXPONENT=122;
    public static final int ID=68;
    public static final int N_LOGICAL_AND=25;
    public static final int LPAREN=58;
    public static final int N_BITWISE_OR=26;
    public static final int N_SELECT=6;
    public static final int AS=54;
    public static final int N_ORDERBY=9;
    public static final int SLASH=92;
    public static final int IN=91;
    public static final int N_TABLE_NAME=19;
    public static final int COMMA=53;
    public static final int EQUAL=77;
    public static final int LOGICAL_OR=72;
    public static final int DOT=67;
    public static final int N_FROM=5;
    public static final int N_SELECT_STATEMENT=4;
    public static final int ADD=85;
    public static final int N_REMAINDER=37;
    public static final int LESS_THAN_OR_EQUAL=80;
    public static final int N_EQUAL=29;
    public static final int SUBSTRUCT=86;
    public static final int N_WITHIN_RECORD=16;
    public static final int GREATER_THAN=81;
    public static final int N_GREATER_THAN_OR_EQUAL=38;
    public static final int N_ALIAS=13;
    public static final int N_IN_PARAMS=45;
    public static final int N_LOGICAL_OR=24;
    public static final int N_FLOAT=48;
    public static final int RSQUARE=117;
    public static final int N_BITWISE_LEFT_SHIFT=44;
    public static final int BITWISE_RIGHT_SHIFT=84;
    public static final int COLON=115;
    public static final int GREATER_THAN_OR_EQUAL=82;
    public static final int N_ASC=17;
    public static final int RCURLY=119;
    public static final int N_OP=23;
    public static final int DIV=93;
    public static final int DESC=64;
    public static final int N_GREATER_THAN=33;
    public static final int STRING=71;

    // delegates
    // delegators


        public BqlParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public BqlParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return BqlParser.tokenNames; }
    public String getGrammarFileName() { return "C:\\work5\\src\\dremel\\common\\Bql.g"; }


    public static class request_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "request"
    // C:\\work5\\src\\dremel\\common\\Bql.g:34:1: request : selectStatement ( SEMICOLON )? EOF ;
    public final BqlParser.request_return request() throws RecognitionException {
        BqlParser.request_return retval = new BqlParser.request_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token SEMICOLON2=null;
        Token EOF3=null;
        BqlParser.selectStatement_return selectStatement1 = null;


        CommonTree SEMICOLON2_tree=null;
        CommonTree EOF3_tree=null;

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:34:13: ( selectStatement ( SEMICOLON )? EOF )
            // C:\\work5\\src\\dremel\\common\\Bql.g:34:16: selectStatement ( SEMICOLON )? EOF
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_selectStatement_in_request180);
            selectStatement1=selectStatement();

            state._fsp--;

            adaptor.addChild(root_0, selectStatement1.getTree());
            // C:\\work5\\src\\dremel\\common\\Bql.g:34:32: ( SEMICOLON )?
            int alt1=2;
            int LA1_0 = input.LA(1);

            if ( (LA1_0==SEMICOLON) ) {
                alt1=1;
            }
            switch (alt1) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:34:33: SEMICOLON
                    {
                    SEMICOLON2=(Token)match(input,SEMICOLON,FOLLOW_SEMICOLON_in_request183); 

                    }
                    break;

            }

            EOF3=(Token)match(input,EOF,FOLLOW_EOF_in_request188); 

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "request"

    public static class selectStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "selectStatement"
    // C:\\work5\\src\\dremel\\common\\Bql.g:37:1: selectStatement : selectClause fromClause ( whereClause )? ( groupbyClause )? ( orderbyClause )? ( limitClause )? -> ^( N_SELECT_STATEMENT fromClause selectClause ( whereClause )? ( groupbyClause )? ( orderbyClause )? ( limitClause )? ) ;
    public final BqlParser.selectStatement_return selectStatement() throws RecognitionException {
        BqlParser.selectStatement_return retval = new BqlParser.selectStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.selectClause_return selectClause4 = null;

        BqlParser.fromClause_return fromClause5 = null;

        BqlParser.whereClause_return whereClause6 = null;

        BqlParser.groupbyClause_return groupbyClause7 = null;

        BqlParser.orderbyClause_return orderbyClause8 = null;

        BqlParser.limitClause_return limitClause9 = null;


        RewriteRuleSubtreeStream stream_whereClause=new RewriteRuleSubtreeStream(adaptor,"rule whereClause");
        RewriteRuleSubtreeStream stream_orderbyClause=new RewriteRuleSubtreeStream(adaptor,"rule orderbyClause");
        RewriteRuleSubtreeStream stream_limitClause=new RewriteRuleSubtreeStream(adaptor,"rule limitClause");
        RewriteRuleSubtreeStream stream_selectClause=new RewriteRuleSubtreeStream(adaptor,"rule selectClause");
        RewriteRuleSubtreeStream stream_fromClause=new RewriteRuleSubtreeStream(adaptor,"rule fromClause");
        RewriteRuleSubtreeStream stream_groupbyClause=new RewriteRuleSubtreeStream(adaptor,"rule groupbyClause");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:37:20: ( selectClause fromClause ( whereClause )? ( groupbyClause )? ( orderbyClause )? ( limitClause )? -> ^( N_SELECT_STATEMENT fromClause selectClause ( whereClause )? ( groupbyClause )? ( orderbyClause )? ( limitClause )? ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:37:23: selectClause fromClause ( whereClause )? ( groupbyClause )? ( orderbyClause )? ( limitClause )?
            {
            pushFollow(FOLLOW_selectClause_in_selectStatement207);
            selectClause4=selectClause();

            state._fsp--;

            stream_selectClause.add(selectClause4.getTree());
            pushFollow(FOLLOW_fromClause_in_selectStatement209);
            fromClause5=fromClause();

            state._fsp--;

            stream_fromClause.add(fromClause5.getTree());
            // C:\\work5\\src\\dremel\\common\\Bql.g:37:47: ( whereClause )?
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==WHERE) ) {
                alt2=1;
            }
            switch (alt2) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:37:47: whereClause
                    {
                    pushFollow(FOLLOW_whereClause_in_selectStatement211);
                    whereClause6=whereClause();

                    state._fsp--;

                    stream_whereClause.add(whereClause6.getTree());

                    }
                    break;

            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:38:8: ( groupbyClause )?
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==GROUPBY) ) {
                alt3=1;
            }
            switch (alt3) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:38:8: groupbyClause
                    {
                    pushFollow(FOLLOW_groupbyClause_in_selectStatement222);
                    groupbyClause7=groupbyClause();

                    state._fsp--;

                    stream_groupbyClause.add(groupbyClause7.getTree());

                    }
                    break;

            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:38:23: ( orderbyClause )?
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==ORDERBY) ) {
                alt4=1;
            }
            switch (alt4) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:38:23: orderbyClause
                    {
                    pushFollow(FOLLOW_orderbyClause_in_selectStatement225);
                    orderbyClause8=orderbyClause();

                    state._fsp--;

                    stream_orderbyClause.add(orderbyClause8.getTree());

                    }
                    break;

            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:38:38: ( limitClause )?
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==LIMIT) ) {
                alt5=1;
            }
            switch (alt5) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:38:38: limitClause
                    {
                    pushFollow(FOLLOW_limitClause_in_selectStatement228);
                    limitClause9=limitClause();

                    state._fsp--;

                    stream_limitClause.add(limitClause9.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: limitClause, selectClause, groupbyClause, whereClause, fromClause, orderbyClause
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 38:53: -> ^( N_SELECT_STATEMENT fromClause selectClause ( whereClause )? ( groupbyClause )? ( orderbyClause )? ( limitClause )? )
            {
                // C:\\work5\\src\\dremel\\common\\Bql.g:38:57: ^( N_SELECT_STATEMENT fromClause selectClause ( whereClause )? ( groupbyClause )? ( orderbyClause )? ( limitClause )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_SELECT_STATEMENT, "N_SELECT_STATEMENT"), root_1);

                adaptor.addChild(root_1, stream_fromClause.nextTree());
                adaptor.addChild(root_1, stream_selectClause.nextTree());
                // C:\\work5\\src\\dremel\\common\\Bql.g:38:102: ( whereClause )?
                if ( stream_whereClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_whereClause.nextTree());

                }
                stream_whereClause.reset();
                // C:\\work5\\src\\dremel\\common\\Bql.g:38:115: ( groupbyClause )?
                if ( stream_groupbyClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_groupbyClause.nextTree());

                }
                stream_groupbyClause.reset();
                // C:\\work5\\src\\dremel\\common\\Bql.g:38:130: ( orderbyClause )?
                if ( stream_orderbyClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_orderbyClause.nextTree());

                }
                stream_orderbyClause.reset();
                // C:\\work5\\src\\dremel\\common\\Bql.g:38:145: ( limitClause )?
                if ( stream_limitClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_limitClause.nextTree());

                }
                stream_limitClause.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "selectStatement"

    public static class selectClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "selectClause"
    // C:\\work5\\src\\dremel\\common\\Bql.g:40:1: selectClause : SELECT columnExpr ( COMMA columnExpr )* -> ^( N_SELECT ( columnExpr )+ ) ;
    public final BqlParser.selectClause_return selectClause() throws RecognitionException {
        BqlParser.selectClause_return retval = new BqlParser.selectClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token SELECT10=null;
        Token COMMA12=null;
        BqlParser.columnExpr_return columnExpr11 = null;

        BqlParser.columnExpr_return columnExpr13 = null;


        CommonTree SELECT10_tree=null;
        CommonTree COMMA12_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_SELECT=new RewriteRuleTokenStream(adaptor,"token SELECT");
        RewriteRuleSubtreeStream stream_columnExpr=new RewriteRuleSubtreeStream(adaptor,"rule columnExpr");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:40:19: ( SELECT columnExpr ( COMMA columnExpr )* -> ^( N_SELECT ( columnExpr )+ ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:40:21: SELECT columnExpr ( COMMA columnExpr )*
            {
            SELECT10=(Token)match(input,SELECT,FOLLOW_SELECT_in_selectClause267);  
            stream_SELECT.add(SELECT10);

            pushFollow(FOLLOW_columnExpr_in_selectClause269);
            columnExpr11=columnExpr();

            state._fsp--;

            stream_columnExpr.add(columnExpr11.getTree());
            // C:\\work5\\src\\dremel\\common\\Bql.g:40:39: ( COMMA columnExpr )*
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( (LA6_0==COMMA) ) {
                    alt6=1;
                }


                switch (alt6) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:40:40: COMMA columnExpr
            	    {
            	    COMMA12=(Token)match(input,COMMA,FOLLOW_COMMA_in_selectClause272);  
            	    stream_COMMA.add(COMMA12);

            	    pushFollow(FOLLOW_columnExpr_in_selectClause274);
            	    columnExpr13=columnExpr();

            	    state._fsp--;

            	    stream_columnExpr.add(columnExpr13.getTree());

            	    }
            	    break;

            	default :
            	    break loop6;
                }
            } while (true);



            // AST REWRITE
            // elements: columnExpr
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 40:64: -> ^( N_SELECT ( columnExpr )+ )
            {
                // C:\\work5\\src\\dremel\\common\\Bql.g:40:68: ^( N_SELECT ( columnExpr )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_SELECT, "N_SELECT"), root_1);

                if ( !(stream_columnExpr.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_columnExpr.hasNext() ) {
                    adaptor.addChild(root_1, stream_columnExpr.nextTree());

                }
                stream_columnExpr.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "selectClause"

    public static class columnExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "columnExpr"
    // C:\\work5\\src\\dremel\\common\\Bql.g:41:1: columnExpr : expression ( withinClause )? ( AS columnName )? -> ^( N_CREATE_COLUMN expression ( ^( N_ALIAS columnName ) )? ( withinClause )? ) ;
    public final BqlParser.columnExpr_return columnExpr() throws RecognitionException {
        BqlParser.columnExpr_return retval = new BqlParser.columnExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token AS16=null;
        BqlParser.expression_return expression14 = null;

        BqlParser.withinClause_return withinClause15 = null;

        BqlParser.columnName_return columnName17 = null;


        CommonTree AS16_tree=null;
        RewriteRuleTokenStream stream_AS=new RewriteRuleTokenStream(adaptor,"token AS");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        RewriteRuleSubtreeStream stream_withinClause=new RewriteRuleSubtreeStream(adaptor,"rule withinClause");
        RewriteRuleSubtreeStream stream_columnName=new RewriteRuleSubtreeStream(adaptor,"rule columnName");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:41:16: ( expression ( withinClause )? ( AS columnName )? -> ^( N_CREATE_COLUMN expression ( ^( N_ALIAS columnName ) )? ( withinClause )? ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:41:18: expression ( withinClause )? ( AS columnName )?
            {
            pushFollow(FOLLOW_expression_in_columnExpr302);
            expression14=expression();

            state._fsp--;

            stream_expression.add(expression14.getTree());
            // C:\\work5\\src\\dremel\\common\\Bql.g:41:29: ( withinClause )?
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0==WITHIN) ) {
                alt7=1;
            }
            switch (alt7) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:41:29: withinClause
                    {
                    pushFollow(FOLLOW_withinClause_in_columnExpr304);
                    withinClause15=withinClause();

                    state._fsp--;

                    stream_withinClause.add(withinClause15.getTree());

                    }
                    break;

            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:41:43: ( AS columnName )?
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0==AS) ) {
                alt8=1;
            }
            switch (alt8) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:41:44: AS columnName
                    {
                    AS16=(Token)match(input,AS,FOLLOW_AS_in_columnExpr308);  
                    stream_AS.add(AS16);

                    pushFollow(FOLLOW_columnName_in_columnExpr310);
                    columnName17=columnName();

                    state._fsp--;

                    stream_columnName.add(columnName17.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: expression, withinClause, columnName
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 41:64: -> ^( N_CREATE_COLUMN expression ( ^( N_ALIAS columnName ) )? ( withinClause )? )
            {
                // C:\\work5\\src\\dremel\\common\\Bql.g:41:68: ^( N_CREATE_COLUMN expression ( ^( N_ALIAS columnName ) )? ( withinClause )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_CREATE_COLUMN, "N_CREATE_COLUMN"), root_1);

                adaptor.addChild(root_1, stream_expression.nextTree());
                // C:\\work5\\src\\dremel\\common\\Bql.g:41:97: ( ^( N_ALIAS columnName ) )?
                if ( stream_columnName.hasNext() ) {
                    // C:\\work5\\src\\dremel\\common\\Bql.g:41:97: ^( N_ALIAS columnName )
                    {
                    CommonTree root_2 = (CommonTree)adaptor.nil();
                    root_2 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_ALIAS, "N_ALIAS"), root_2);

                    adaptor.addChild(root_2, stream_columnName.nextTree());

                    adaptor.addChild(root_1, root_2);
                    }

                }
                stream_columnName.reset();
                // C:\\work5\\src\\dremel\\common\\Bql.g:41:120: ( withinClause )?
                if ( stream_withinClause.hasNext() ) {
                    adaptor.addChild(root_1, stream_withinClause.nextTree());

                }
                stream_withinClause.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "columnExpr"

    public static class withinClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "withinClause"
    // C:\\work5\\src\\dremel\\common\\Bql.g:42:1: withinClause : ( WITHIN ( RECORD -> ^( N_WITHIN_RECORD ) | columnPath -> ^( N_WITHIN columnPath ) ) ) ;
    public final BqlParser.withinClause_return withinClause() throws RecognitionException {
        BqlParser.withinClause_return retval = new BqlParser.withinClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token WITHIN18=null;
        Token RECORD19=null;
        BqlParser.columnPath_return columnPath20 = null;


        CommonTree WITHIN18_tree=null;
        CommonTree RECORD19_tree=null;
        RewriteRuleTokenStream stream_WITHIN=new RewriteRuleTokenStream(adaptor,"token WITHIN");
        RewriteRuleTokenStream stream_RECORD=new RewriteRuleTokenStream(adaptor,"token RECORD");
        RewriteRuleSubtreeStream stream_columnPath=new RewriteRuleSubtreeStream(adaptor,"rule columnPath");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:42:17: ( ( WITHIN ( RECORD -> ^( N_WITHIN_RECORD ) | columnPath -> ^( N_WITHIN columnPath ) ) ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:42:19: ( WITHIN ( RECORD -> ^( N_WITHIN_RECORD ) | columnPath -> ^( N_WITHIN columnPath ) ) )
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:42:19: ( WITHIN ( RECORD -> ^( N_WITHIN_RECORD ) | columnPath -> ^( N_WITHIN columnPath ) ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:42:20: WITHIN ( RECORD -> ^( N_WITHIN_RECORD ) | columnPath -> ^( N_WITHIN columnPath ) )
            {
            WITHIN18=(Token)match(input,WITHIN,FOLLOW_WITHIN_in_withinClause347);  
            stream_WITHIN.add(WITHIN18);

            // C:\\work5\\src\\dremel\\common\\Bql.g:42:27: ( RECORD -> ^( N_WITHIN_RECORD ) | columnPath -> ^( N_WITHIN columnPath ) )
            int alt9=2;
            int LA9_0 = input.LA(1);

            if ( (LA9_0==RECORD) ) {
                alt9=1;
            }
            else if ( ((LA9_0>=ID && LA9_0<=STAR)) ) {
                alt9=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 9, 0, input);

                throw nvae;
            }
            switch (alt9) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:42:28: RECORD
                    {
                    RECORD19=(Token)match(input,RECORD,FOLLOW_RECORD_in_withinClause350);  
                    stream_RECORD.add(RECORD19);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 42:43: -> ^( N_WITHIN_RECORD )
                    {
                        // C:\\work5\\src\\dremel\\common\\Bql.g:42:47: ^( N_WITHIN_RECORD )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_WITHIN_RECORD, "N_WITHIN_RECORD"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:43:10: columnPath
                    {
                    pushFollow(FOLLOW_columnPath_in_withinClause376);
                    columnPath20=columnPath();

                    state._fsp--;

                    stream_columnPath.add(columnPath20.getTree());


                    // AST REWRITE
                    // elements: columnPath
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 43:27: -> ^( N_WITHIN columnPath )
                    {
                        // C:\\work5\\src\\dremel\\common\\Bql.g:43:30: ^( N_WITHIN columnPath )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_WITHIN, "N_WITHIN"), root_1);

                        adaptor.addChild(root_1, stream_columnPath.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;

            }


            }


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "withinClause"

    public static class fromClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "fromClause"
    // C:\\work5\\src\\dremel\\common\\Bql.g:46:1: fromClause : FROM subSelectStatement ( COMMA subSelectStatement )* -> ^( N_FROM ( subSelectStatement )+ ) ;
    public final BqlParser.fromClause_return fromClause() throws RecognitionException {
        BqlParser.fromClause_return retval = new BqlParser.fromClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token FROM21=null;
        Token COMMA23=null;
        BqlParser.subSelectStatement_return subSelectStatement22 = null;

        BqlParser.subSelectStatement_return subSelectStatement24 = null;


        CommonTree FROM21_tree=null;
        CommonTree COMMA23_tree=null;
        RewriteRuleTokenStream stream_FROM=new RewriteRuleTokenStream(adaptor,"token FROM");
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleSubtreeStream stream_subSelectStatement=new RewriteRuleSubtreeStream(adaptor,"rule subSelectStatement");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:46:16: ( FROM subSelectStatement ( COMMA subSelectStatement )* -> ^( N_FROM ( subSelectStatement )+ ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:46:18: FROM subSelectStatement ( COMMA subSelectStatement )*
            {
            FROM21=(Token)match(input,FROM,FOLLOW_FROM_in_fromClause412);  
            stream_FROM.add(FROM21);

            pushFollow(FOLLOW_subSelectStatement_in_fromClause414);
            subSelectStatement22=subSelectStatement();

            state._fsp--;

            stream_subSelectStatement.add(subSelectStatement22.getTree());
            // C:\\work5\\src\\dremel\\common\\Bql.g:46:42: ( COMMA subSelectStatement )*
            loop10:
            do {
                int alt10=2;
                int LA10_0 = input.LA(1);

                if ( (LA10_0==COMMA) ) {
                    alt10=1;
                }


                switch (alt10) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:46:43: COMMA subSelectStatement
            	    {
            	    COMMA23=(Token)match(input,COMMA,FOLLOW_COMMA_in_fromClause417);  
            	    stream_COMMA.add(COMMA23);

            	    pushFollow(FOLLOW_subSelectStatement_in_fromClause419);
            	    subSelectStatement24=subSelectStatement();

            	    state._fsp--;

            	    stream_subSelectStatement.add(subSelectStatement24.getTree());

            	    }
            	    break;

            	default :
            	    break loop10;
                }
            } while (true);



            // AST REWRITE
            // elements: subSelectStatement
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 46:73: -> ^( N_FROM ( subSelectStatement )+ )
            {
                // C:\\work5\\src\\dremel\\common\\Bql.g:46:77: ^( N_FROM ( subSelectStatement )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_FROM, "N_FROM"), root_1);

                if ( !(stream_subSelectStatement.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_subSelectStatement.hasNext() ) {
                    adaptor.addChild(root_1, stream_subSelectStatement.nextTree());

                }
                stream_subSelectStatement.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "fromClause"

    public static class subSelectStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "subSelectStatement"
    // C:\\work5\\src\\dremel\\common\\Bql.g:47:1: subSelectStatement : ( tableName | ( LPAREN selectStatement RPAREN ) ) ;
    public final BqlParser.subSelectStatement_return subSelectStatement() throws RecognitionException {
        BqlParser.subSelectStatement_return retval = new BqlParser.subSelectStatement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token LPAREN26=null;
        Token RPAREN28=null;
        BqlParser.tableName_return tableName25 = null;

        BqlParser.selectStatement_return selectStatement27 = null;


        CommonTree LPAREN26_tree=null;
        CommonTree RPAREN28_tree=null;

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:47:22: ( ( tableName | ( LPAREN selectStatement RPAREN ) ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:47:24: ( tableName | ( LPAREN selectStatement RPAREN ) )
            {
            root_0 = (CommonTree)adaptor.nil();

            // C:\\work5\\src\\dremel\\common\\Bql.g:47:24: ( tableName | ( LPAREN selectStatement RPAREN ) )
            int alt11=2;
            int LA11_0 = input.LA(1);

            if ( (LA11_0==ID) ) {
                alt11=1;
            }
            else if ( (LA11_0==LPAREN) ) {
                alt11=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 11, 0, input);

                throw nvae;
            }
            switch (alt11) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:47:25: tableName
                    {
                    pushFollow(FOLLOW_tableName_in_subSelectStatement444);
                    tableName25=tableName();

                    state._fsp--;

                    adaptor.addChild(root_0, tableName25.getTree());

                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:47:37: ( LPAREN selectStatement RPAREN )
                    {
                    // C:\\work5\\src\\dremel\\common\\Bql.g:47:37: ( LPAREN selectStatement RPAREN )
                    // C:\\work5\\src\\dremel\\common\\Bql.g:47:38: LPAREN selectStatement RPAREN
                    {
                    LPAREN26=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_subSelectStatement449); 
                    pushFollow(FOLLOW_selectStatement_in_subSelectStatement452);
                    selectStatement27=selectStatement();

                    state._fsp--;

                    adaptor.addChild(root_0, selectStatement27.getTree());
                    RPAREN28=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_subSelectStatement454); 

                    }


                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "subSelectStatement"

    public static class whereClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "whereClause"
    // C:\\work5\\src\\dremel\\common\\Bql.g:49:1: whereClause : WHERE expression -> ^( N_WHERE expression ) ;
    public final BqlParser.whereClause_return whereClause() throws RecognitionException {
        BqlParser.whereClause_return retval = new BqlParser.whereClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token WHERE29=null;
        BqlParser.expression_return expression30 = null;


        CommonTree WHERE29_tree=null;
        RewriteRuleTokenStream stream_WHERE=new RewriteRuleTokenStream(adaptor,"token WHERE");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:49:16: ( WHERE expression -> ^( N_WHERE expression ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:49:18: WHERE expression
            {
            WHERE29=(Token)match(input,WHERE,FOLLOW_WHERE_in_whereClause471);  
            stream_WHERE.add(WHERE29);

            pushFollow(FOLLOW_expression_in_whereClause473);
            expression30=expression();

            state._fsp--;

            stream_expression.add(expression30.getTree());


            // AST REWRITE
            // elements: expression
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 49:42: -> ^( N_WHERE expression )
            {
                // C:\\work5\\src\\dremel\\common\\Bql.g:49:46: ^( N_WHERE expression )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_WHERE, "N_WHERE"), root_1);

                adaptor.addChild(root_1, stream_expression.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "whereClause"

    public static class groupbyClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "groupbyClause"
    // C:\\work5\\src\\dremel\\common\\Bql.g:51:1: groupbyClause : GROUPBY columnName ( COMMA columnName )* -> ^( N_GROUPBY ( columnName )+ ) ;
    public final BqlParser.groupbyClause_return groupbyClause() throws RecognitionException {
        BqlParser.groupbyClause_return retval = new BqlParser.groupbyClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token GROUPBY31=null;
        Token COMMA33=null;
        BqlParser.columnName_return columnName32 = null;

        BqlParser.columnName_return columnName34 = null;


        CommonTree GROUPBY31_tree=null;
        CommonTree COMMA33_tree=null;
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_GROUPBY=new RewriteRuleTokenStream(adaptor,"token GROUPBY");
        RewriteRuleSubtreeStream stream_columnName=new RewriteRuleSubtreeStream(adaptor,"rule columnName");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:51:20: ( GROUPBY columnName ( COMMA columnName )* -> ^( N_GROUPBY ( columnName )+ ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:51:22: GROUPBY columnName ( COMMA columnName )*
            {
            GROUPBY31=(Token)match(input,GROUPBY,FOLLOW_GROUPBY_in_groupbyClause502);  
            stream_GROUPBY.add(GROUPBY31);

            pushFollow(FOLLOW_columnName_in_groupbyClause504);
            columnName32=columnName();

            state._fsp--;

            stream_columnName.add(columnName32.getTree());
            // C:\\work5\\src\\dremel\\common\\Bql.g:51:41: ( COMMA columnName )*
            loop12:
            do {
                int alt12=2;
                int LA12_0 = input.LA(1);

                if ( (LA12_0==COMMA) ) {
                    alt12=1;
                }


                switch (alt12) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:51:42: COMMA columnName
            	    {
            	    COMMA33=(Token)match(input,COMMA,FOLLOW_COMMA_in_groupbyClause507);  
            	    stream_COMMA.add(COMMA33);

            	    pushFollow(FOLLOW_columnName_in_groupbyClause509);
            	    columnName34=columnName();

            	    state._fsp--;

            	    stream_columnName.add(columnName34.getTree());

            	    }
            	    break;

            	default :
            	    break loop12;
                }
            } while (true);



            // AST REWRITE
            // elements: columnName
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 51:66: -> ^( N_GROUPBY ( columnName )+ )
            {
                // C:\\work5\\src\\dremel\\common\\Bql.g:51:70: ^( N_GROUPBY ( columnName )+ )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_GROUPBY, "N_GROUPBY"), root_1);

                if ( !(stream_columnName.hasNext()) ) {
                    throw new RewriteEarlyExitException();
                }
                while ( stream_columnName.hasNext() ) {
                    adaptor.addChild(root_1, stream_columnName.nextTree());

                }
                stream_columnName.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "groupbyClause"

    public static class orderbyClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "orderbyClause"
    // C:\\work5\\src\\dremel\\common\\Bql.g:53:1: orderbyClause : ( ORDERBY orderbyColumnName ( COMMA orderbyColumnName )* ) -> ^( N_ORDERBY orderbyColumnName ) ;
    public final BqlParser.orderbyClause_return orderbyClause() throws RecognitionException {
        BqlParser.orderbyClause_return retval = new BqlParser.orderbyClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ORDERBY35=null;
        Token COMMA37=null;
        BqlParser.orderbyColumnName_return orderbyColumnName36 = null;

        BqlParser.orderbyColumnName_return orderbyColumnName38 = null;


        CommonTree ORDERBY35_tree=null;
        CommonTree COMMA37_tree=null;
        RewriteRuleTokenStream stream_ORDERBY=new RewriteRuleTokenStream(adaptor,"token ORDERBY");
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleSubtreeStream stream_orderbyColumnName=new RewriteRuleSubtreeStream(adaptor,"rule orderbyColumnName");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:53:20: ( ( ORDERBY orderbyColumnName ( COMMA orderbyColumnName )* ) -> ^( N_ORDERBY orderbyColumnName ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:53:22: ( ORDERBY orderbyColumnName ( COMMA orderbyColumnName )* )
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:53:22: ( ORDERBY orderbyColumnName ( COMMA orderbyColumnName )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:53:24: ORDERBY orderbyColumnName ( COMMA orderbyColumnName )*
            {
            ORDERBY35=(Token)match(input,ORDERBY,FOLLOW_ORDERBY_in_orderbyClause541);  
            stream_ORDERBY.add(ORDERBY35);

            pushFollow(FOLLOW_orderbyColumnName_in_orderbyClause543);
            orderbyColumnName36=orderbyColumnName();

            state._fsp--;

            stream_orderbyColumnName.add(orderbyColumnName36.getTree());
            // C:\\work5\\src\\dremel\\common\\Bql.g:53:50: ( COMMA orderbyColumnName )*
            loop13:
            do {
                int alt13=2;
                int LA13_0 = input.LA(1);

                if ( (LA13_0==COMMA) ) {
                    alt13=1;
                }


                switch (alt13) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:53:51: COMMA orderbyColumnName
            	    {
            	    COMMA37=(Token)match(input,COMMA,FOLLOW_COMMA_in_orderbyClause546);  
            	    stream_COMMA.add(COMMA37);

            	    pushFollow(FOLLOW_orderbyColumnName_in_orderbyClause548);
            	    orderbyColumnName38=orderbyColumnName();

            	    state._fsp--;

            	    stream_orderbyColumnName.add(orderbyColumnName38.getTree());

            	    }
            	    break;

            	default :
            	    break loop13;
                }
            } while (true);


            }



            // AST REWRITE
            // elements: orderbyColumnName
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 53:80: -> ^( N_ORDERBY orderbyColumnName )
            {
                // C:\\work5\\src\\dremel\\common\\Bql.g:53:84: ^( N_ORDERBY orderbyColumnName )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_ORDERBY, "N_ORDERBY"), root_1);

                adaptor.addChild(root_1, stream_orderbyColumnName.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "orderbyClause"

    public static class orderbyColumnName_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "orderbyColumnName"
    // C:\\work5\\src\\dremel\\common\\Bql.g:54:1: orderbyColumnName : columnName ( ASC -> ^( N_ASC columnName ) | DESC -> ^( N_DESC columnName ) | -> ^( N_ASC columnName ) ) ;
    public final BqlParser.orderbyColumnName_return orderbyColumnName() throws RecognitionException {
        BqlParser.orderbyColumnName_return retval = new BqlParser.orderbyColumnName_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ASC40=null;
        Token DESC41=null;
        BqlParser.columnName_return columnName39 = null;


        CommonTree ASC40_tree=null;
        CommonTree DESC41_tree=null;
        RewriteRuleTokenStream stream_ASC=new RewriteRuleTokenStream(adaptor,"token ASC");
        RewriteRuleTokenStream stream_DESC=new RewriteRuleTokenStream(adaptor,"token DESC");
        RewriteRuleSubtreeStream stream_columnName=new RewriteRuleSubtreeStream(adaptor,"rule columnName");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:54:21: ( columnName ( ASC -> ^( N_ASC columnName ) | DESC -> ^( N_DESC columnName ) | -> ^( N_ASC columnName ) ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:54:23: columnName ( ASC -> ^( N_ASC columnName ) | DESC -> ^( N_DESC columnName ) | -> ^( N_ASC columnName ) )
            {
            pushFollow(FOLLOW_columnName_in_orderbyColumnName571);
            columnName39=columnName();

            state._fsp--;

            stream_columnName.add(columnName39.getTree());
            // C:\\work5\\src\\dremel\\common\\Bql.g:54:34: ( ASC -> ^( N_ASC columnName ) | DESC -> ^( N_DESC columnName ) | -> ^( N_ASC columnName ) )
            int alt14=3;
            switch ( input.LA(1) ) {
            case ASC:
                {
                alt14=1;
                }
                break;
            case DESC:
                {
                alt14=2;
                }
                break;
            case EOF:
            case SEMICOLON:
            case COMMA:
            case RPAREN:
            case LIMIT:
                {
                alt14=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 14, 0, input);

                throw nvae;
            }

            switch (alt14) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:54:35: ASC
                    {
                    ASC40=(Token)match(input,ASC,FOLLOW_ASC_in_orderbyColumnName574);  
                    stream_ASC.add(ASC40);



                    // AST REWRITE
                    // elements: columnName
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 54:46: -> ^( N_ASC columnName )
                    {
                        // C:\\work5\\src\\dremel\\common\\Bql.g:54:50: ^( N_ASC columnName )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_ASC, "N_ASC"), root_1);

                        adaptor.addChild(root_1, stream_columnName.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:55:10: DESC
                    {
                    DESC41=(Token)match(input,DESC,FOLLOW_DESC_in_orderbyColumnName601);  
                    stream_DESC.add(DESC41);



                    // AST REWRITE
                    // elements: columnName
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 55:22: -> ^( N_DESC columnName )
                    {
                        // C:\\work5\\src\\dremel\\common\\Bql.g:55:25: ^( N_DESC columnName )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_DESC, "N_DESC"), root_1);

                        adaptor.addChild(root_1, stream_columnName.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 3 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:56:38: 
                    {

                    // AST REWRITE
                    // elements: columnName
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 56:38: -> ^( N_ASC columnName )
                    {
                        // C:\\work5\\src\\dremel\\common\\Bql.g:56:41: ^( N_ASC columnName )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_ASC, "N_ASC"), root_1);

                        adaptor.addChild(root_1, stream_columnName.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "orderbyColumnName"

    public static class limitClause_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "limitClause"
    // C:\\work5\\src\\dremel\\common\\Bql.g:59:1: limitClause : ( LIMIT INT ) -> ^( N_LIMIT INT ) ;
    public final BqlParser.limitClause_return limitClause() throws RecognitionException {
        BqlParser.limitClause_return retval = new BqlParser.limitClause_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token LIMIT42=null;
        Token INT43=null;

        CommonTree LIMIT42_tree=null;
        CommonTree INT43_tree=null;
        RewriteRuleTokenStream stream_INT=new RewriteRuleTokenStream(adaptor,"token INT");
        RewriteRuleTokenStream stream_LIMIT=new RewriteRuleTokenStream(adaptor,"token LIMIT");

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:59:18: ( ( LIMIT INT ) -> ^( N_LIMIT INT ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:59:20: ( LIMIT INT )
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:59:20: ( LIMIT INT )
            // C:\\work5\\src\\dremel\\common\\Bql.g:59:22: LIMIT INT
            {
            LIMIT42=(Token)match(input,LIMIT,FOLLOW_LIMIT_in_limitClause669);  
            stream_LIMIT.add(LIMIT42);

            INT43=(Token)match(input,INT,FOLLOW_INT_in_limitClause671);  
            stream_INT.add(INT43);


            }



            // AST REWRITE
            // elements: INT
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 59:41: -> ^( N_LIMIT INT )
            {
                // C:\\work5\\src\\dremel\\common\\Bql.g:59:44: ^( N_LIMIT INT )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_LIMIT, "N_LIMIT"), root_1);

                adaptor.addChild(root_1, stream_INT.nextNode());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "limitClause"

    public static class columnPath_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "columnPath"
    // C:\\work5\\src\\dremel\\common\\Bql.g:62:1: columnPath : columnPath2 -> ^( N_COLUMN columnPath2 ) ;
    public final BqlParser.columnPath_return columnPath() throws RecognitionException {
        BqlParser.columnPath_return retval = new BqlParser.columnPath_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.columnPath2_return columnPath244 = null;


        RewriteRuleSubtreeStream stream_columnPath2=new RewriteRuleSubtreeStream(adaptor,"rule columnPath2");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:62:15: ( columnPath2 -> ^( N_COLUMN columnPath2 ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:62:17: columnPath2
            {
            pushFollow(FOLLOW_columnPath2_in_columnPath700);
            columnPath244=columnPath2();

            state._fsp--;

            stream_columnPath2.add(columnPath244.getTree());


            // AST REWRITE
            // elements: columnPath2
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 62:37: -> ^( N_COLUMN columnPath2 )
            {
                // C:\\work5\\src\\dremel\\common\\Bql.g:62:41: ^( N_COLUMN columnPath2 )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_COLUMN, "N_COLUMN"), root_1);

                adaptor.addChild(root_1, stream_columnPath2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "columnPath"

    public static class columnPath2_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "columnPath2"
    // C:\\work5\\src\\dremel\\common\\Bql.g:63:1: columnPath2 : columnName ( DOT columnName )* ;
    public final BqlParser.columnPath2_return columnPath2() throws RecognitionException {
        BqlParser.columnPath2_return retval = new BqlParser.columnPath2_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token DOT46=null;
        BqlParser.columnName_return columnName45 = null;

        BqlParser.columnName_return columnName47 = null;


        CommonTree DOT46_tree=null;

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:63:16: ( columnName ( DOT columnName )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:63:18: columnName ( DOT columnName )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_columnName_in_columnPath2727);
            columnName45=columnName();

            state._fsp--;

            adaptor.addChild(root_0, columnName45.getTree());
            // C:\\work5\\src\\dremel\\common\\Bql.g:63:29: ( DOT columnName )*
            loop15:
            do {
                int alt15=2;
                int LA15_0 = input.LA(1);

                if ( (LA15_0==DOT) ) {
                    alt15=1;
                }


                switch (alt15) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:63:30: DOT columnName
            	    {
            	    DOT46=(Token)match(input,DOT,FOLLOW_DOT_in_columnPath2730); 
            	    pushFollow(FOLLOW_columnName_in_columnPath2733);
            	    columnName47=columnName();

            	    state._fsp--;

            	    adaptor.addChild(root_0, columnName47.getTree());

            	    }
            	    break;

            	default :
            	    break loop15;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "columnPath2"

    public static class columnName_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "columnName"
    // C:\\work5\\src\\dremel\\common\\Bql.g:64:1: columnName : ( ID -> ^( N_COLUMN_NAME ID ) | STAR -> ^( N_COLUMN_NAME STAR ) );
    public final BqlParser.columnName_return columnName() throws RecognitionException {
        BqlParser.columnName_return retval = new BqlParser.columnName_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ID48=null;
        Token STAR49=null;

        CommonTree ID48_tree=null;
        CommonTree STAR49_tree=null;
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:64:16: ( ID -> ^( N_COLUMN_NAME ID ) | STAR -> ^( N_COLUMN_NAME STAR ) )
            int alt16=2;
            int LA16_0 = input.LA(1);

            if ( (LA16_0==ID) ) {
                alt16=1;
            }
            else if ( (LA16_0==STAR) ) {
                alt16=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 16, 0, input);

                throw nvae;
            }
            switch (alt16) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:64:18: ID
                    {
                    ID48=(Token)match(input,ID,FOLLOW_ID_in_columnName752);  
                    stream_ID.add(ID48);



                    // AST REWRITE
                    // elements: ID
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 64:21: -> ^( N_COLUMN_NAME ID )
                    {
                        // C:\\work5\\src\\dremel\\common\\Bql.g:64:24: ^( N_COLUMN_NAME ID )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_COLUMN_NAME, "N_COLUMN_NAME"), root_1);

                        adaptor.addChild(root_1, stream_ID.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:64:46: STAR
                    {
                    STAR49=(Token)match(input,STAR,FOLLOW_STAR_in_columnName764);  
                    stream_STAR.add(STAR49);



                    // AST REWRITE
                    // elements: STAR
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 64:51: -> ^( N_COLUMN_NAME STAR )
                    {
                        // C:\\work5\\src\\dremel\\common\\Bql.g:64:54: ^( N_COLUMN_NAME STAR )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_COLUMN_NAME, "N_COLUMN_NAME"), root_1);

                        adaptor.addChild(root_1, stream_STAR.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "columnName"

    public static class tablePath_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "tablePath"
    // C:\\work5\\src\\dremel\\common\\Bql.g:67:1: tablePath : tablePath2 -> ^( N_TABLE tablePath2 ) ;
    public final BqlParser.tablePath_return tablePath() throws RecognitionException {
        BqlParser.tablePath_return retval = new BqlParser.tablePath_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.tablePath2_return tablePath250 = null;


        RewriteRuleSubtreeStream stream_tablePath2=new RewriteRuleSubtreeStream(adaptor,"rule tablePath2");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:67:14: ( tablePath2 -> ^( N_TABLE tablePath2 ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:67:16: tablePath2
            {
            pushFollow(FOLLOW_tablePath2_in_tablePath786);
            tablePath250=tablePath2();

            state._fsp--;

            stream_tablePath2.add(tablePath250.getTree());


            // AST REWRITE
            // elements: tablePath2
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 67:35: -> ^( N_TABLE tablePath2 )
            {
                // C:\\work5\\src\\dremel\\common\\Bql.g:67:38: ^( N_TABLE tablePath2 )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_TABLE, "N_TABLE"), root_1);

                adaptor.addChild(root_1, stream_tablePath2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "tablePath"

    public static class tablePath2_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "tablePath2"
    // C:\\work5\\src\\dremel\\common\\Bql.g:68:1: tablePath2 : tableName ( DOT tableName )* ;
    public final BqlParser.tablePath2_return tablePath2() throws RecognitionException {
        BqlParser.tablePath2_return retval = new BqlParser.tablePath2_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token DOT52=null;
        BqlParser.tableName_return tableName51 = null;

        BqlParser.tableName_return tableName53 = null;


        CommonTree DOT52_tree=null;

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:68:15: ( tableName ( DOT tableName )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:68:17: tableName ( DOT tableName )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_tableName_in_tablePath2812);
            tableName51=tableName();

            state._fsp--;

            adaptor.addChild(root_0, tableName51.getTree());
            // C:\\work5\\src\\dremel\\common\\Bql.g:68:27: ( DOT tableName )*
            loop17:
            do {
                int alt17=2;
                int LA17_0 = input.LA(1);

                if ( (LA17_0==DOT) ) {
                    alt17=1;
                }


                switch (alt17) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:68:28: DOT tableName
            	    {
            	    DOT52=(Token)match(input,DOT,FOLLOW_DOT_in_tablePath2815); 
            	    pushFollow(FOLLOW_tableName_in_tablePath2818);
            	    tableName53=tableName();

            	    state._fsp--;

            	    adaptor.addChild(root_0, tableName53.getTree());

            	    }
            	    break;

            	default :
            	    break loop17;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "tablePath2"

    public static class tableName_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "tableName"
    // C:\\work5\\src\\dremel\\common\\Bql.g:69:1: tableName : ID -> ^( N_TABLE_NAME ID ) ;
    public final BqlParser.tableName_return tableName() throws RecognitionException {
        BqlParser.tableName_return retval = new BqlParser.tableName_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ID54=null;

        CommonTree ID54_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:69:15: ( ID -> ^( N_TABLE_NAME ID ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:69:17: ID
            {
            ID54=(Token)match(input,ID,FOLLOW_ID_in_tableName837);  
            stream_ID.add(ID54);



            // AST REWRITE
            // elements: ID
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 69:20: -> ^( N_TABLE_NAME ID )
            {
                // C:\\work5\\src\\dremel\\common\\Bql.g:69:23: ^( N_TABLE_NAME ID )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_TABLE_NAME, "N_TABLE_NAME"), root_1);

                adaptor.addChild(root_1, stream_ID.nextNode());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "tableName"

    public static class expression_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expression"
    // C:\\work5\\src\\dremel\\common\\Bql.g:72:1: expression : expression2 -> ^( N_EXPRESSION expression2 ) ;
    public final BqlParser.expression_return expression() throws RecognitionException {
        BqlParser.expression_return retval = new BqlParser.expression_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.expression2_return expression255 = null;


        RewriteRuleSubtreeStream stream_expression2=new RewriteRuleSubtreeStream(adaptor,"rule expression2");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:72:15: ( expression2 -> ^( N_EXPRESSION expression2 ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:72:17: expression2
            {
            pushFollow(FOLLOW_expression2_in_expression863);
            expression255=expression2();

            state._fsp--;

            stream_expression2.add(expression255.getTree());


            // AST REWRITE
            // elements: expression2
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 72:37: -> ^( N_EXPRESSION expression2 )
            {
                // C:\\work5\\src\\dremel\\common\\Bql.g:72:41: ^( N_EXPRESSION expression2 )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_EXPRESSION, "N_EXPRESSION"), root_1);

                adaptor.addChild(root_1, stream_expression2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "expression"

    public static class expression2_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expression2"
    // C:\\work5\\src\\dremel\\common\\Bql.g:73:1: expression2 : (a= binary10thPrcdExpr -> $a) (o= binary11thPrcdOp b= binary10thPrcdExpr -> ^( $o $expression2 $b) )* ;
    public final BqlParser.expression2_return expression2() throws RecognitionException {
        BqlParser.expression2_return retval = new BqlParser.expression2_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.binary10thPrcdExpr_return a = null;

        BqlParser.binary11thPrcdOp_return o = null;

        BqlParser.binary10thPrcdExpr_return b = null;


        RewriteRuleSubtreeStream stream_binary11thPrcdOp=new RewriteRuleSubtreeStream(adaptor,"rule binary11thPrcdOp");
        RewriteRuleSubtreeStream stream_binary10thPrcdExpr=new RewriteRuleSubtreeStream(adaptor,"rule binary10thPrcdExpr");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:73:16: ( (a= binary10thPrcdExpr -> $a) (o= binary11thPrcdOp b= binary10thPrcdExpr -> ^( $o $expression2 $b) )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:73:18: (a= binary10thPrcdExpr -> $a) (o= binary11thPrcdOp b= binary10thPrcdExpr -> ^( $o $expression2 $b) )*
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:73:18: (a= binary10thPrcdExpr -> $a)
            // C:\\work5\\src\\dremel\\common\\Bql.g:73:19: a= binary10thPrcdExpr
            {
            pushFollow(FOLLOW_binary10thPrcdExpr_in_expression2893);
            a=binary10thPrcdExpr();

            state._fsp--;

            stream_binary10thPrcdExpr.add(a.getTree());


            // AST REWRITE
            // elements: a
            // token labels: 
            // rule labels: retval, a
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 73:39: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }

            retval.tree = root_0;
            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:73:45: (o= binary11thPrcdOp b= binary10thPrcdExpr -> ^( $o $expression2 $b) )*
            loop18:
            do {
                int alt18=2;
                int LA18_0 = input.LA(1);

                if ( (LA18_0==LOGICAL_OR) ) {
                    alt18=1;
                }


                switch (alt18) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:73:46: o= binary11thPrcdOp b= binary10thPrcdExpr
            	    {
            	    pushFollow(FOLLOW_binary11thPrcdOp_in_expression2902);
            	    o=binary11thPrcdOp();

            	    state._fsp--;

            	    stream_binary11thPrcdOp.add(o.getTree());
            	    pushFollow(FOLLOW_binary10thPrcdExpr_in_expression2906);
            	    b=binary10thPrcdExpr();

            	    state._fsp--;

            	    stream_binary10thPrcdExpr.add(b.getTree());


            	    // AST REWRITE
            	    // elements: o, expression2, b
            	    // token labels: 
            	    // rule labels: retval, b, o
            	    // token list labels: 
            	    // rule list labels: 
            	    // wildcard labels: 
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_b=new RewriteRuleSubtreeStream(adaptor,"rule b",b!=null?b.tree:null);
            	    RewriteRuleSubtreeStream stream_o=new RewriteRuleSubtreeStream(adaptor,"rule o",o!=null?o.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 73:87: -> ^( $o $expression2 $b)
            	    {
            	        // C:\\work5\\src\\dremel\\common\\Bql.g:73:91: ^( $o $expression2 $b)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(stream_o.nextNode(), root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());
            	        adaptor.addChild(root_1, stream_b.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }

            	    retval.tree = root_0;
            	    }
            	    break;

            	default :
            	    break loop18;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "expression2"

    public static class binary10thPrcdExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary10thPrcdExpr"
    // C:\\work5\\src\\dremel\\common\\Bql.g:74:1: binary10thPrcdExpr : (a= binary9thPrcdExpr -> $a) (o= binary10thPrcdOp b= binary9thPrcdExpr -> ^( $o $binary10thPrcdExpr $b) )* ;
    public final BqlParser.binary10thPrcdExpr_return binary10thPrcdExpr() throws RecognitionException {
        BqlParser.binary10thPrcdExpr_return retval = new BqlParser.binary10thPrcdExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.binary9thPrcdExpr_return a = null;

        BqlParser.binary10thPrcdOp_return o = null;

        BqlParser.binary9thPrcdExpr_return b = null;


        RewriteRuleSubtreeStream stream_binary10thPrcdOp=new RewriteRuleSubtreeStream(adaptor,"rule binary10thPrcdOp");
        RewriteRuleSubtreeStream stream_binary9thPrcdExpr=new RewriteRuleSubtreeStream(adaptor,"rule binary9thPrcdExpr");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:74:22: ( (a= binary9thPrcdExpr -> $a) (o= binary10thPrcdOp b= binary9thPrcdExpr -> ^( $o $binary10thPrcdExpr $b) )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:74:24: (a= binary9thPrcdExpr -> $a) (o= binary10thPrcdOp b= binary9thPrcdExpr -> ^( $o $binary10thPrcdExpr $b) )*
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:74:24: (a= binary9thPrcdExpr -> $a)
            // C:\\work5\\src\\dremel\\common\\Bql.g:74:25: a= binary9thPrcdExpr
            {
            pushFollow(FOLLOW_binary9thPrcdExpr_in_binary10thPrcdExpr935);
            a=binary9thPrcdExpr();

            state._fsp--;

            stream_binary9thPrcdExpr.add(a.getTree());


            // AST REWRITE
            // elements: a
            // token labels: 
            // rule labels: retval, a
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 74:44: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }

            retval.tree = root_0;
            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:74:50: (o= binary10thPrcdOp b= binary9thPrcdExpr -> ^( $o $binary10thPrcdExpr $b) )*
            loop19:
            do {
                int alt19=2;
                int LA19_0 = input.LA(1);

                if ( (LA19_0==LOGICAL_AND) ) {
                    alt19=1;
                }


                switch (alt19) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:74:51: o= binary10thPrcdOp b= binary9thPrcdExpr
            	    {
            	    pushFollow(FOLLOW_binary10thPrcdOp_in_binary10thPrcdExpr944);
            	    o=binary10thPrcdOp();

            	    state._fsp--;

            	    stream_binary10thPrcdOp.add(o.getTree());
            	    pushFollow(FOLLOW_binary9thPrcdExpr_in_binary10thPrcdExpr948);
            	    b=binary9thPrcdExpr();

            	    state._fsp--;

            	    stream_binary9thPrcdExpr.add(b.getTree());


            	    // AST REWRITE
            	    // elements: binary10thPrcdExpr, b, o
            	    // token labels: 
            	    // rule labels: retval, b, o
            	    // token list labels: 
            	    // rule list labels: 
            	    // wildcard labels: 
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_b=new RewriteRuleSubtreeStream(adaptor,"rule b",b!=null?b.tree:null);
            	    RewriteRuleSubtreeStream stream_o=new RewriteRuleSubtreeStream(adaptor,"rule o",o!=null?o.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 74:91: -> ^( $o $binary10thPrcdExpr $b)
            	    {
            	        // C:\\work5\\src\\dremel\\common\\Bql.g:74:95: ^( $o $binary10thPrcdExpr $b)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(stream_o.nextNode(), root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());
            	        adaptor.addChild(root_1, stream_b.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }

            	    retval.tree = root_0;
            	    }
            	    break;

            	default :
            	    break loop19;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary10thPrcdExpr"

    public static class binary9thPrcdExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary9thPrcdExpr"
    // C:\\work5\\src\\dremel\\common\\Bql.g:75:1: binary9thPrcdExpr : (a= binary8thPrcdExpr -> $a) (o= binary9thPrcdOp b= binary8thPrcdExpr -> ^( $o $binary9thPrcdExpr $b) )* ;
    public final BqlParser.binary9thPrcdExpr_return binary9thPrcdExpr() throws RecognitionException {
        BqlParser.binary9thPrcdExpr_return retval = new BqlParser.binary9thPrcdExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.binary8thPrcdExpr_return a = null;

        BqlParser.binary9thPrcdOp_return o = null;

        BqlParser.binary8thPrcdExpr_return b = null;


        RewriteRuleSubtreeStream stream_binary8thPrcdExpr=new RewriteRuleSubtreeStream(adaptor,"rule binary8thPrcdExpr");
        RewriteRuleSubtreeStream stream_binary9thPrcdOp=new RewriteRuleSubtreeStream(adaptor,"rule binary9thPrcdOp");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:75:21: ( (a= binary8thPrcdExpr -> $a) (o= binary9thPrcdOp b= binary8thPrcdExpr -> ^( $o $binary9thPrcdExpr $b) )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:75:23: (a= binary8thPrcdExpr -> $a) (o= binary9thPrcdOp b= binary8thPrcdExpr -> ^( $o $binary9thPrcdExpr $b) )*
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:75:23: (a= binary8thPrcdExpr -> $a)
            // C:\\work5\\src\\dremel\\common\\Bql.g:75:24: a= binary8thPrcdExpr
            {
            pushFollow(FOLLOW_binary8thPrcdExpr_in_binary9thPrcdExpr977);
            a=binary8thPrcdExpr();

            state._fsp--;

            stream_binary8thPrcdExpr.add(a.getTree());


            // AST REWRITE
            // elements: a
            // token labels: 
            // rule labels: retval, a
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 75:43: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }

            retval.tree = root_0;
            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:75:49: (o= binary9thPrcdOp b= binary8thPrcdExpr -> ^( $o $binary9thPrcdExpr $b) )*
            loop20:
            do {
                int alt20=2;
                int LA20_0 = input.LA(1);

                if ( (LA20_0==BITWISE_OR) ) {
                    alt20=1;
                }


                switch (alt20) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:75:50: o= binary9thPrcdOp b= binary8thPrcdExpr
            	    {
            	    pushFollow(FOLLOW_binary9thPrcdOp_in_binary9thPrcdExpr986);
            	    o=binary9thPrcdOp();

            	    state._fsp--;

            	    stream_binary9thPrcdOp.add(o.getTree());
            	    pushFollow(FOLLOW_binary8thPrcdExpr_in_binary9thPrcdExpr990);
            	    b=binary8thPrcdExpr();

            	    state._fsp--;

            	    stream_binary8thPrcdExpr.add(b.getTree());


            	    // AST REWRITE
            	    // elements: binary9thPrcdExpr, b, o
            	    // token labels: 
            	    // rule labels: retval, b, o
            	    // token list labels: 
            	    // rule list labels: 
            	    // wildcard labels: 
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_b=new RewriteRuleSubtreeStream(adaptor,"rule b",b!=null?b.tree:null);
            	    RewriteRuleSubtreeStream stream_o=new RewriteRuleSubtreeStream(adaptor,"rule o",o!=null?o.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 75:89: -> ^( $o $binary9thPrcdExpr $b)
            	    {
            	        // C:\\work5\\src\\dremel\\common\\Bql.g:75:93: ^( $o $binary9thPrcdExpr $b)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(stream_o.nextNode(), root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());
            	        adaptor.addChild(root_1, stream_b.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }

            	    retval.tree = root_0;
            	    }
            	    break;

            	default :
            	    break loop20;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary9thPrcdExpr"

    public static class binary8thPrcdExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary8thPrcdExpr"
    // C:\\work5\\src\\dremel\\common\\Bql.g:76:1: binary8thPrcdExpr : (a= binary7thPrcdExpr -> $a) (o= binary8thPrcdOp b= binary7thPrcdExpr -> ^( $o $binary8thPrcdExpr $b) )* ;
    public final BqlParser.binary8thPrcdExpr_return binary8thPrcdExpr() throws RecognitionException {
        BqlParser.binary8thPrcdExpr_return retval = new BqlParser.binary8thPrcdExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.binary7thPrcdExpr_return a = null;

        BqlParser.binary8thPrcdOp_return o = null;

        BqlParser.binary7thPrcdExpr_return b = null;


        RewriteRuleSubtreeStream stream_binary7thPrcdExpr=new RewriteRuleSubtreeStream(adaptor,"rule binary7thPrcdExpr");
        RewriteRuleSubtreeStream stream_binary8thPrcdOp=new RewriteRuleSubtreeStream(adaptor,"rule binary8thPrcdOp");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:76:21: ( (a= binary7thPrcdExpr -> $a) (o= binary8thPrcdOp b= binary7thPrcdExpr -> ^( $o $binary8thPrcdExpr $b) )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:76:23: (a= binary7thPrcdExpr -> $a) (o= binary8thPrcdOp b= binary7thPrcdExpr -> ^( $o $binary8thPrcdExpr $b) )*
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:76:23: (a= binary7thPrcdExpr -> $a)
            // C:\\work5\\src\\dremel\\common\\Bql.g:76:24: a= binary7thPrcdExpr
            {
            pushFollow(FOLLOW_binary7thPrcdExpr_in_binary8thPrcdExpr1019);
            a=binary7thPrcdExpr();

            state._fsp--;

            stream_binary7thPrcdExpr.add(a.getTree());


            // AST REWRITE
            // elements: a
            // token labels: 
            // rule labels: retval, a
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 76:43: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }

            retval.tree = root_0;
            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:76:49: (o= binary8thPrcdOp b= binary7thPrcdExpr -> ^( $o $binary8thPrcdExpr $b) )*
            loop21:
            do {
                int alt21=2;
                int LA21_0 = input.LA(1);

                if ( (LA21_0==BITWISE_XOR) ) {
                    alt21=1;
                }


                switch (alt21) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:76:50: o= binary8thPrcdOp b= binary7thPrcdExpr
            	    {
            	    pushFollow(FOLLOW_binary8thPrcdOp_in_binary8thPrcdExpr1028);
            	    o=binary8thPrcdOp();

            	    state._fsp--;

            	    stream_binary8thPrcdOp.add(o.getTree());
            	    pushFollow(FOLLOW_binary7thPrcdExpr_in_binary8thPrcdExpr1032);
            	    b=binary7thPrcdExpr();

            	    state._fsp--;

            	    stream_binary7thPrcdExpr.add(b.getTree());


            	    // AST REWRITE
            	    // elements: b, binary8thPrcdExpr, o
            	    // token labels: 
            	    // rule labels: retval, b, o
            	    // token list labels: 
            	    // rule list labels: 
            	    // wildcard labels: 
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_b=new RewriteRuleSubtreeStream(adaptor,"rule b",b!=null?b.tree:null);
            	    RewriteRuleSubtreeStream stream_o=new RewriteRuleSubtreeStream(adaptor,"rule o",o!=null?o.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 76:89: -> ^( $o $binary8thPrcdExpr $b)
            	    {
            	        // C:\\work5\\src\\dremel\\common\\Bql.g:76:93: ^( $o $binary8thPrcdExpr $b)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(stream_o.nextNode(), root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());
            	        adaptor.addChild(root_1, stream_b.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }

            	    retval.tree = root_0;
            	    }
            	    break;

            	default :
            	    break loop21;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary8thPrcdExpr"

    public static class binary7thPrcdExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary7thPrcdExpr"
    // C:\\work5\\src\\dremel\\common\\Bql.g:77:1: binary7thPrcdExpr : (a= binary6thPrcdExpr -> $a) (o= binary7thPrcdOp b= binary6thPrcdExpr -> ^( $o $binary7thPrcdExpr $b) )* ;
    public final BqlParser.binary7thPrcdExpr_return binary7thPrcdExpr() throws RecognitionException {
        BqlParser.binary7thPrcdExpr_return retval = new BqlParser.binary7thPrcdExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.binary6thPrcdExpr_return a = null;

        BqlParser.binary7thPrcdOp_return o = null;

        BqlParser.binary6thPrcdExpr_return b = null;


        RewriteRuleSubtreeStream stream_binary6thPrcdExpr=new RewriteRuleSubtreeStream(adaptor,"rule binary6thPrcdExpr");
        RewriteRuleSubtreeStream stream_binary7thPrcdOp=new RewriteRuleSubtreeStream(adaptor,"rule binary7thPrcdOp");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:77:21: ( (a= binary6thPrcdExpr -> $a) (o= binary7thPrcdOp b= binary6thPrcdExpr -> ^( $o $binary7thPrcdExpr $b) )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:77:23: (a= binary6thPrcdExpr -> $a) (o= binary7thPrcdOp b= binary6thPrcdExpr -> ^( $o $binary7thPrcdExpr $b) )*
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:77:23: (a= binary6thPrcdExpr -> $a)
            // C:\\work5\\src\\dremel\\common\\Bql.g:77:24: a= binary6thPrcdExpr
            {
            pushFollow(FOLLOW_binary6thPrcdExpr_in_binary7thPrcdExpr1061);
            a=binary6thPrcdExpr();

            state._fsp--;

            stream_binary6thPrcdExpr.add(a.getTree());


            // AST REWRITE
            // elements: a
            // token labels: 
            // rule labels: retval, a
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 77:43: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }

            retval.tree = root_0;
            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:77:49: (o= binary7thPrcdOp b= binary6thPrcdExpr -> ^( $o $binary7thPrcdExpr $b) )*
            loop22:
            do {
                int alt22=2;
                int LA22_0 = input.LA(1);

                if ( (LA22_0==BITWISE_AND) ) {
                    alt22=1;
                }


                switch (alt22) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:77:50: o= binary7thPrcdOp b= binary6thPrcdExpr
            	    {
            	    pushFollow(FOLLOW_binary7thPrcdOp_in_binary7thPrcdExpr1070);
            	    o=binary7thPrcdOp();

            	    state._fsp--;

            	    stream_binary7thPrcdOp.add(o.getTree());
            	    pushFollow(FOLLOW_binary6thPrcdExpr_in_binary7thPrcdExpr1074);
            	    b=binary6thPrcdExpr();

            	    state._fsp--;

            	    stream_binary6thPrcdExpr.add(b.getTree());


            	    // AST REWRITE
            	    // elements: o, binary7thPrcdExpr, b
            	    // token labels: 
            	    // rule labels: retval, b, o
            	    // token list labels: 
            	    // rule list labels: 
            	    // wildcard labels: 
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_b=new RewriteRuleSubtreeStream(adaptor,"rule b",b!=null?b.tree:null);
            	    RewriteRuleSubtreeStream stream_o=new RewriteRuleSubtreeStream(adaptor,"rule o",o!=null?o.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 77:89: -> ^( $o $binary7thPrcdExpr $b)
            	    {
            	        // C:\\work5\\src\\dremel\\common\\Bql.g:77:93: ^( $o $binary7thPrcdExpr $b)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(stream_o.nextNode(), root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());
            	        adaptor.addChild(root_1, stream_b.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }

            	    retval.tree = root_0;
            	    }
            	    break;

            	default :
            	    break loop22;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary7thPrcdExpr"

    public static class binary6thPrcdExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary6thPrcdExpr"
    // C:\\work5\\src\\dremel\\common\\Bql.g:78:1: binary6thPrcdExpr : (a= binary5thPrcdExpr -> $a) (o= binary6thPrcdOp b= binary5thPrcdExpr -> ^( $o $binary6thPrcdExpr $b) )* ;
    public final BqlParser.binary6thPrcdExpr_return binary6thPrcdExpr() throws RecognitionException {
        BqlParser.binary6thPrcdExpr_return retval = new BqlParser.binary6thPrcdExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.binary5thPrcdExpr_return a = null;

        BqlParser.binary6thPrcdOp_return o = null;

        BqlParser.binary5thPrcdExpr_return b = null;


        RewriteRuleSubtreeStream stream_binary5thPrcdExpr=new RewriteRuleSubtreeStream(adaptor,"rule binary5thPrcdExpr");
        RewriteRuleSubtreeStream stream_binary6thPrcdOp=new RewriteRuleSubtreeStream(adaptor,"rule binary6thPrcdOp");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:78:21: ( (a= binary5thPrcdExpr -> $a) (o= binary6thPrcdOp b= binary5thPrcdExpr -> ^( $o $binary6thPrcdExpr $b) )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:78:23: (a= binary5thPrcdExpr -> $a) (o= binary6thPrcdOp b= binary5thPrcdExpr -> ^( $o $binary6thPrcdExpr $b) )*
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:78:23: (a= binary5thPrcdExpr -> $a)
            // C:\\work5\\src\\dremel\\common\\Bql.g:78:24: a= binary5thPrcdExpr
            {
            pushFollow(FOLLOW_binary5thPrcdExpr_in_binary6thPrcdExpr1103);
            a=binary5thPrcdExpr();

            state._fsp--;

            stream_binary5thPrcdExpr.add(a.getTree());


            // AST REWRITE
            // elements: a
            // token labels: 
            // rule labels: retval, a
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 78:43: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }

            retval.tree = root_0;
            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:78:49: (o= binary6thPrcdOp b= binary5thPrcdExpr -> ^( $o $binary6thPrcdExpr $b) )*
            loop23:
            do {
                int alt23=2;
                int LA23_0 = input.LA(1);

                if ( ((LA23_0>=EQUAL && LA23_0<=NOT_EQUAL)) ) {
                    alt23=1;
                }


                switch (alt23) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:78:50: o= binary6thPrcdOp b= binary5thPrcdExpr
            	    {
            	    pushFollow(FOLLOW_binary6thPrcdOp_in_binary6thPrcdExpr1112);
            	    o=binary6thPrcdOp();

            	    state._fsp--;

            	    stream_binary6thPrcdOp.add(o.getTree());
            	    pushFollow(FOLLOW_binary5thPrcdExpr_in_binary6thPrcdExpr1116);
            	    b=binary5thPrcdExpr();

            	    state._fsp--;

            	    stream_binary5thPrcdExpr.add(b.getTree());


            	    // AST REWRITE
            	    // elements: o, binary6thPrcdExpr, b
            	    // token labels: 
            	    // rule labels: retval, b, o
            	    // token list labels: 
            	    // rule list labels: 
            	    // wildcard labels: 
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_b=new RewriteRuleSubtreeStream(adaptor,"rule b",b!=null?b.tree:null);
            	    RewriteRuleSubtreeStream stream_o=new RewriteRuleSubtreeStream(adaptor,"rule o",o!=null?o.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 78:89: -> ^( $o $binary6thPrcdExpr $b)
            	    {
            	        // C:\\work5\\src\\dremel\\common\\Bql.g:78:93: ^( $o $binary6thPrcdExpr $b)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(stream_o.nextNode(), root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());
            	        adaptor.addChild(root_1, stream_b.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }

            	    retval.tree = root_0;
            	    }
            	    break;

            	default :
            	    break loop23;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary6thPrcdExpr"

    public static class binary5thPrcdExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary5thPrcdExpr"
    // C:\\work5\\src\\dremel\\common\\Bql.g:79:1: binary5thPrcdExpr : (a= binary4thPrcdExpr -> $a) (o= binary5thPrcdOp b= binary4thPrcdExpr -> ^( $o $binary5thPrcdExpr $b) )* ;
    public final BqlParser.binary5thPrcdExpr_return binary5thPrcdExpr() throws RecognitionException {
        BqlParser.binary5thPrcdExpr_return retval = new BqlParser.binary5thPrcdExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.binary4thPrcdExpr_return a = null;

        BqlParser.binary5thPrcdOp_return o = null;

        BqlParser.binary4thPrcdExpr_return b = null;


        RewriteRuleSubtreeStream stream_binary5thPrcdOp=new RewriteRuleSubtreeStream(adaptor,"rule binary5thPrcdOp");
        RewriteRuleSubtreeStream stream_binary4thPrcdExpr=new RewriteRuleSubtreeStream(adaptor,"rule binary4thPrcdExpr");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:79:21: ( (a= binary4thPrcdExpr -> $a) (o= binary5thPrcdOp b= binary4thPrcdExpr -> ^( $o $binary5thPrcdExpr $b) )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:79:23: (a= binary4thPrcdExpr -> $a) (o= binary5thPrcdOp b= binary4thPrcdExpr -> ^( $o $binary5thPrcdExpr $b) )*
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:79:23: (a= binary4thPrcdExpr -> $a)
            // C:\\work5\\src\\dremel\\common\\Bql.g:79:24: a= binary4thPrcdExpr
            {
            pushFollow(FOLLOW_binary4thPrcdExpr_in_binary5thPrcdExpr1145);
            a=binary4thPrcdExpr();

            state._fsp--;

            stream_binary4thPrcdExpr.add(a.getTree());


            // AST REWRITE
            // elements: a
            // token labels: 
            // rule labels: retval, a
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 79:43: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }

            retval.tree = root_0;
            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:79:49: (o= binary5thPrcdOp b= binary4thPrcdExpr -> ^( $o $binary5thPrcdExpr $b) )*
            loop24:
            do {
                int alt24=2;
                int LA24_0 = input.LA(1);

                if ( ((LA24_0>=LESS_THAN && LA24_0<=GREATER_THAN_OR_EQUAL)) ) {
                    alt24=1;
                }


                switch (alt24) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:79:50: o= binary5thPrcdOp b= binary4thPrcdExpr
            	    {
            	    pushFollow(FOLLOW_binary5thPrcdOp_in_binary5thPrcdExpr1154);
            	    o=binary5thPrcdOp();

            	    state._fsp--;

            	    stream_binary5thPrcdOp.add(o.getTree());
            	    pushFollow(FOLLOW_binary4thPrcdExpr_in_binary5thPrcdExpr1158);
            	    b=binary4thPrcdExpr();

            	    state._fsp--;

            	    stream_binary4thPrcdExpr.add(b.getTree());


            	    // AST REWRITE
            	    // elements: b, binary5thPrcdExpr, o
            	    // token labels: 
            	    // rule labels: retval, b, o
            	    // token list labels: 
            	    // rule list labels: 
            	    // wildcard labels: 
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_b=new RewriteRuleSubtreeStream(adaptor,"rule b",b!=null?b.tree:null);
            	    RewriteRuleSubtreeStream stream_o=new RewriteRuleSubtreeStream(adaptor,"rule o",o!=null?o.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 79:89: -> ^( $o $binary5thPrcdExpr $b)
            	    {
            	        // C:\\work5\\src\\dremel\\common\\Bql.g:79:93: ^( $o $binary5thPrcdExpr $b)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(stream_o.nextNode(), root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());
            	        adaptor.addChild(root_1, stream_b.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }

            	    retval.tree = root_0;
            	    }
            	    break;

            	default :
            	    break loop24;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary5thPrcdExpr"

    public static class binary4thPrcdExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary4thPrcdExpr"
    // C:\\work5\\src\\dremel\\common\\Bql.g:80:1: binary4thPrcdExpr : (a= binary3rdPrcdExpr -> $a) (o= binary4thPrcdOp b= binary3rdPrcdExpr -> ^( $o $binary4thPrcdExpr $b) )* ;
    public final BqlParser.binary4thPrcdExpr_return binary4thPrcdExpr() throws RecognitionException {
        BqlParser.binary4thPrcdExpr_return retval = new BqlParser.binary4thPrcdExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.binary3rdPrcdExpr_return a = null;

        BqlParser.binary4thPrcdOp_return o = null;

        BqlParser.binary3rdPrcdExpr_return b = null;


        RewriteRuleSubtreeStream stream_binary3rdPrcdExpr=new RewriteRuleSubtreeStream(adaptor,"rule binary3rdPrcdExpr");
        RewriteRuleSubtreeStream stream_binary4thPrcdOp=new RewriteRuleSubtreeStream(adaptor,"rule binary4thPrcdOp");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:80:21: ( (a= binary3rdPrcdExpr -> $a) (o= binary4thPrcdOp b= binary3rdPrcdExpr -> ^( $o $binary4thPrcdExpr $b) )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:80:23: (a= binary3rdPrcdExpr -> $a) (o= binary4thPrcdOp b= binary3rdPrcdExpr -> ^( $o $binary4thPrcdExpr $b) )*
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:80:23: (a= binary3rdPrcdExpr -> $a)
            // C:\\work5\\src\\dremel\\common\\Bql.g:80:24: a= binary3rdPrcdExpr
            {
            pushFollow(FOLLOW_binary3rdPrcdExpr_in_binary4thPrcdExpr1187);
            a=binary3rdPrcdExpr();

            state._fsp--;

            stream_binary3rdPrcdExpr.add(a.getTree());


            // AST REWRITE
            // elements: a
            // token labels: 
            // rule labels: retval, a
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 80:43: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }

            retval.tree = root_0;
            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:80:49: (o= binary4thPrcdOp b= binary3rdPrcdExpr -> ^( $o $binary4thPrcdExpr $b) )*
            loop25:
            do {
                int alt25=2;
                int LA25_0 = input.LA(1);

                if ( ((LA25_0>=BITWISE_LEFT_SHIFT && LA25_0<=BITWISE_RIGHT_SHIFT)) ) {
                    alt25=1;
                }


                switch (alt25) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:80:50: o= binary4thPrcdOp b= binary3rdPrcdExpr
            	    {
            	    pushFollow(FOLLOW_binary4thPrcdOp_in_binary4thPrcdExpr1196);
            	    o=binary4thPrcdOp();

            	    state._fsp--;

            	    stream_binary4thPrcdOp.add(o.getTree());
            	    pushFollow(FOLLOW_binary3rdPrcdExpr_in_binary4thPrcdExpr1200);
            	    b=binary3rdPrcdExpr();

            	    state._fsp--;

            	    stream_binary3rdPrcdExpr.add(b.getTree());


            	    // AST REWRITE
            	    // elements: b, binary4thPrcdExpr, o
            	    // token labels: 
            	    // rule labels: retval, b, o
            	    // token list labels: 
            	    // rule list labels: 
            	    // wildcard labels: 
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_b=new RewriteRuleSubtreeStream(adaptor,"rule b",b!=null?b.tree:null);
            	    RewriteRuleSubtreeStream stream_o=new RewriteRuleSubtreeStream(adaptor,"rule o",o!=null?o.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 80:89: -> ^( $o $binary4thPrcdExpr $b)
            	    {
            	        // C:\\work5\\src\\dremel\\common\\Bql.g:80:93: ^( $o $binary4thPrcdExpr $b)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(stream_o.nextNode(), root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());
            	        adaptor.addChild(root_1, stream_b.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }

            	    retval.tree = root_0;
            	    }
            	    break;

            	default :
            	    break loop25;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary4thPrcdExpr"

    public static class binary3rdPrcdExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary3rdPrcdExpr"
    // C:\\work5\\src\\dremel\\common\\Bql.g:81:1: binary3rdPrcdExpr : (a= binary2ndPrcdExpr -> $a) (o= binary3rdPrcdOp b= binary2ndPrcdExpr -> ^( $o $binary3rdPrcdExpr $b) )* ;
    public final BqlParser.binary3rdPrcdExpr_return binary3rdPrcdExpr() throws RecognitionException {
        BqlParser.binary3rdPrcdExpr_return retval = new BqlParser.binary3rdPrcdExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.binary2ndPrcdExpr_return a = null;

        BqlParser.binary3rdPrcdOp_return o = null;

        BqlParser.binary2ndPrcdExpr_return b = null;


        RewriteRuleSubtreeStream stream_binary3rdPrcdOp=new RewriteRuleSubtreeStream(adaptor,"rule binary3rdPrcdOp");
        RewriteRuleSubtreeStream stream_binary2ndPrcdExpr=new RewriteRuleSubtreeStream(adaptor,"rule binary2ndPrcdExpr");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:81:21: ( (a= binary2ndPrcdExpr -> $a) (o= binary3rdPrcdOp b= binary2ndPrcdExpr -> ^( $o $binary3rdPrcdExpr $b) )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:81:23: (a= binary2ndPrcdExpr -> $a) (o= binary3rdPrcdOp b= binary2ndPrcdExpr -> ^( $o $binary3rdPrcdExpr $b) )*
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:81:23: (a= binary2ndPrcdExpr -> $a)
            // C:\\work5\\src\\dremel\\common\\Bql.g:81:24: a= binary2ndPrcdExpr
            {
            pushFollow(FOLLOW_binary2ndPrcdExpr_in_binary3rdPrcdExpr1229);
            a=binary2ndPrcdExpr();

            state._fsp--;

            stream_binary2ndPrcdExpr.add(a.getTree());


            // AST REWRITE
            // elements: a
            // token labels: 
            // rule labels: retval, a
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 81:43: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }

            retval.tree = root_0;
            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:81:49: (o= binary3rdPrcdOp b= binary2ndPrcdExpr -> ^( $o $binary3rdPrcdExpr $b) )*
            loop26:
            do {
                int alt26=2;
                int LA26_0 = input.LA(1);

                if ( ((LA26_0>=ADD && LA26_0<=SUBSTRUCT)) ) {
                    alt26=1;
                }


                switch (alt26) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:81:50: o= binary3rdPrcdOp b= binary2ndPrcdExpr
            	    {
            	    pushFollow(FOLLOW_binary3rdPrcdOp_in_binary3rdPrcdExpr1238);
            	    o=binary3rdPrcdOp();

            	    state._fsp--;

            	    stream_binary3rdPrcdOp.add(o.getTree());
            	    pushFollow(FOLLOW_binary2ndPrcdExpr_in_binary3rdPrcdExpr1242);
            	    b=binary2ndPrcdExpr();

            	    state._fsp--;

            	    stream_binary2ndPrcdExpr.add(b.getTree());


            	    // AST REWRITE
            	    // elements: o, b, binary3rdPrcdExpr
            	    // token labels: 
            	    // rule labels: retval, b, o
            	    // token list labels: 
            	    // rule list labels: 
            	    // wildcard labels: 
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_b=new RewriteRuleSubtreeStream(adaptor,"rule b",b!=null?b.tree:null);
            	    RewriteRuleSubtreeStream stream_o=new RewriteRuleSubtreeStream(adaptor,"rule o",o!=null?o.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 81:89: -> ^( $o $binary3rdPrcdExpr $b)
            	    {
            	        // C:\\work5\\src\\dremel\\common\\Bql.g:81:93: ^( $o $binary3rdPrcdExpr $b)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(stream_o.nextNode(), root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());
            	        adaptor.addChild(root_1, stream_b.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }

            	    retval.tree = root_0;
            	    }
            	    break;

            	default :
            	    break loop26;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary3rdPrcdExpr"

    public static class binary2ndPrcdExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary2ndPrcdExpr"
    // C:\\work5\\src\\dremel\\common\\Bql.g:82:1: binary2ndPrcdExpr : (a= binary1stPrcdExpr -> $a) (o= binary2ndPrcdOp b= binary1stPrcdExpr -> ^( $o $binary2ndPrcdExpr $b) )* ;
    public final BqlParser.binary2ndPrcdExpr_return binary2ndPrcdExpr() throws RecognitionException {
        BqlParser.binary2ndPrcdExpr_return retval = new BqlParser.binary2ndPrcdExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.binary1stPrcdExpr_return a = null;

        BqlParser.binary2ndPrcdOp_return o = null;

        BqlParser.binary1stPrcdExpr_return b = null;


        RewriteRuleSubtreeStream stream_binary1stPrcdExpr=new RewriteRuleSubtreeStream(adaptor,"rule binary1stPrcdExpr");
        RewriteRuleSubtreeStream stream_binary2ndPrcdOp=new RewriteRuleSubtreeStream(adaptor,"rule binary2ndPrcdOp");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:82:21: ( (a= binary1stPrcdExpr -> $a) (o= binary2ndPrcdOp b= binary1stPrcdExpr -> ^( $o $binary2ndPrcdExpr $b) )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:82:23: (a= binary1stPrcdExpr -> $a) (o= binary2ndPrcdOp b= binary1stPrcdExpr -> ^( $o $binary2ndPrcdExpr $b) )*
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:82:23: (a= binary1stPrcdExpr -> $a)
            // C:\\work5\\src\\dremel\\common\\Bql.g:82:24: a= binary1stPrcdExpr
            {
            pushFollow(FOLLOW_binary1stPrcdExpr_in_binary2ndPrcdExpr1271);
            a=binary1stPrcdExpr();

            state._fsp--;

            stream_binary1stPrcdExpr.add(a.getTree());


            // AST REWRITE
            // elements: a
            // token labels: 
            // rule labels: retval, a
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 82:43: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }

            retval.tree = root_0;
            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:82:49: (o= binary2ndPrcdOp b= binary1stPrcdExpr -> ^( $o $binary2ndPrcdExpr $b) )*
            loop27:
            do {
                int alt27=2;
                int LA27_0 = input.LA(1);

                if ( (LA27_0==STAR||LA27_0==REMAINDER||(LA27_0>=SLASH && LA27_0<=DIV)) ) {
                    alt27=1;
                }


                switch (alt27) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:82:50: o= binary2ndPrcdOp b= binary1stPrcdExpr
            	    {
            	    pushFollow(FOLLOW_binary2ndPrcdOp_in_binary2ndPrcdExpr1280);
            	    o=binary2ndPrcdOp();

            	    state._fsp--;

            	    stream_binary2ndPrcdOp.add(o.getTree());
            	    pushFollow(FOLLOW_binary1stPrcdExpr_in_binary2ndPrcdExpr1284);
            	    b=binary1stPrcdExpr();

            	    state._fsp--;

            	    stream_binary1stPrcdExpr.add(b.getTree());


            	    // AST REWRITE
            	    // elements: b, binary2ndPrcdExpr, o
            	    // token labels: 
            	    // rule labels: retval, b, o
            	    // token list labels: 
            	    // rule list labels: 
            	    // wildcard labels: 
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_b=new RewriteRuleSubtreeStream(adaptor,"rule b",b!=null?b.tree:null);
            	    RewriteRuleSubtreeStream stream_o=new RewriteRuleSubtreeStream(adaptor,"rule o",o!=null?o.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 82:89: -> ^( $o $binary2ndPrcdExpr $b)
            	    {
            	        // C:\\work5\\src\\dremel\\common\\Bql.g:82:93: ^( $o $binary2ndPrcdExpr $b)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(stream_o.nextNode(), root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());
            	        adaptor.addChild(root_1, stream_b.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }

            	    retval.tree = root_0;
            	    }
            	    break;

            	default :
            	    break loop27;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary2ndPrcdExpr"

    public static class binary1stPrcdExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary1stPrcdExpr"
    // C:\\work5\\src\\dremel\\common\\Bql.g:83:1: binary1stPrcdExpr : (a= unaryPrefixExpr -> $a) (o= binary1stPrcdOp b= unaryPrefixExpr -> ^( $o $binary1stPrcdExpr $b) )* ;
    public final BqlParser.binary1stPrcdExpr_return binary1stPrcdExpr() throws RecognitionException {
        BqlParser.binary1stPrcdExpr_return retval = new BqlParser.binary1stPrcdExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.unaryPrefixExpr_return a = null;

        BqlParser.binary1stPrcdOp_return o = null;

        BqlParser.unaryPrefixExpr_return b = null;


        RewriteRuleSubtreeStream stream_unaryPrefixExpr=new RewriteRuleSubtreeStream(adaptor,"rule unaryPrefixExpr");
        RewriteRuleSubtreeStream stream_binary1stPrcdOp=new RewriteRuleSubtreeStream(adaptor,"rule binary1stPrcdOp");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:83:21: ( (a= unaryPrefixExpr -> $a) (o= binary1stPrcdOp b= unaryPrefixExpr -> ^( $o $binary1stPrcdExpr $b) )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:83:23: (a= unaryPrefixExpr -> $a) (o= binary1stPrcdOp b= unaryPrefixExpr -> ^( $o $binary1stPrcdExpr $b) )*
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:83:23: (a= unaryPrefixExpr -> $a)
            // C:\\work5\\src\\dremel\\common\\Bql.g:83:24: a= unaryPrefixExpr
            {
            pushFollow(FOLLOW_unaryPrefixExpr_in_binary1stPrcdExpr1313);
            a=unaryPrefixExpr();

            state._fsp--;

            stream_unaryPrefixExpr.add(a.getTree());


            // AST REWRITE
            // elements: a
            // token labels: 
            // rule labels: retval, a
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 83:41: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }

            retval.tree = root_0;
            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:83:49: (o= binary1stPrcdOp b= unaryPrefixExpr -> ^( $o $binary1stPrcdExpr $b) )*
            loop28:
            do {
                int alt28=2;
                int LA28_0 = input.LA(1);

                if ( (LA28_0==CONTAINS) ) {
                    alt28=1;
                }


                switch (alt28) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:83:50: o= binary1stPrcdOp b= unaryPrefixExpr
            	    {
            	    pushFollow(FOLLOW_binary1stPrcdOp_in_binary1stPrcdExpr1324);
            	    o=binary1stPrcdOp();

            	    state._fsp--;

            	    stream_binary1stPrcdOp.add(o.getTree());
            	    pushFollow(FOLLOW_unaryPrefixExpr_in_binary1stPrcdExpr1328);
            	    b=unaryPrefixExpr();

            	    state._fsp--;

            	    stream_unaryPrefixExpr.add(b.getTree());


            	    // AST REWRITE
            	    // elements: binary1stPrcdExpr, o, b
            	    // token labels: 
            	    // rule labels: retval, b, o
            	    // token list labels: 
            	    // rule list labels: 
            	    // wildcard labels: 
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_b=new RewriteRuleSubtreeStream(adaptor,"rule b",b!=null?b.tree:null);
            	    RewriteRuleSubtreeStream stream_o=new RewriteRuleSubtreeStream(adaptor,"rule o",o!=null?o.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 83:88: -> ^( $o $binary1stPrcdExpr $b)
            	    {
            	        // C:\\work5\\src\\dremel\\common\\Bql.g:83:92: ^( $o $binary1stPrcdExpr $b)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(stream_o.nextNode(), root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());
            	        adaptor.addChild(root_1, stream_b.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }

            	    retval.tree = root_0;
            	    }
            	    break;

            	default :
            	    break loop28;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary1stPrcdExpr"

    public static class unaryPrefixExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "unaryPrefixExpr"
    // C:\\work5\\src\\dremel\\common\\Bql.g:84:1: unaryPrefixExpr : (o= unaryPrefixOp -> ^( $o $unaryPrefixExpr) )* (a= unaryPostfixExpr -> $a) ;
    public final BqlParser.unaryPrefixExpr_return unaryPrefixExpr() throws RecognitionException {
        BqlParser.unaryPrefixExpr_return retval = new BqlParser.unaryPrefixExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.unaryPrefixOp_return o = null;

        BqlParser.unaryPostfixExpr_return a = null;


        RewriteRuleSubtreeStream stream_unaryPrefixOp=new RewriteRuleSubtreeStream(adaptor,"rule unaryPrefixOp");
        RewriteRuleSubtreeStream stream_unaryPostfixExpr=new RewriteRuleSubtreeStream(adaptor,"rule unaryPostfixExpr");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:84:20: ( (o= unaryPrefixOp -> ^( $o $unaryPrefixExpr) )* (a= unaryPostfixExpr -> $a) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:84:22: (o= unaryPrefixOp -> ^( $o $unaryPrefixExpr) )* (a= unaryPostfixExpr -> $a)
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:84:22: (o= unaryPrefixOp -> ^( $o $unaryPrefixExpr) )*
            loop29:
            do {
                int alt29=2;
                int LA29_0 = input.LA(1);

                if ( ((LA29_0>=BITWISE_NOT && LA29_0<=LOGICAL_NOT)) ) {
                    alt29=1;
                }


                switch (alt29) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:84:23: o= unaryPrefixOp
            	    {
            	    pushFollow(FOLLOW_unaryPrefixOp_in_unaryPrefixExpr1359);
            	    o=unaryPrefixOp();

            	    state._fsp--;

            	    stream_unaryPrefixOp.add(o.getTree());


            	    // AST REWRITE
            	    // elements: unaryPrefixExpr, o
            	    // token labels: 
            	    // rule labels: retval, o
            	    // token list labels: 
            	    // rule list labels: 
            	    // wildcard labels: 
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_o=new RewriteRuleSubtreeStream(adaptor,"rule o",o!=null?o.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 84:38: -> ^( $o $unaryPrefixExpr)
            	    {
            	        // C:\\work5\\src\\dremel\\common\\Bql.g:84:40: ^( $o $unaryPrefixExpr)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(stream_o.nextNode(), root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }

            	    retval.tree = root_0;
            	    }
            	    break;

            	default :
            	    break loop29;
                }
            } while (true);

            // C:\\work5\\src\\dremel\\common\\Bql.g:84:65: (a= unaryPostfixExpr -> $a)
            // C:\\work5\\src\\dremel\\common\\Bql.g:84:66: a= unaryPostfixExpr
            {
            pushFollow(FOLLOW_unaryPostfixExpr_in_unaryPrefixExpr1374);
            a=unaryPostfixExpr();

            state._fsp--;

            stream_unaryPostfixExpr.add(a.getTree());


            // AST REWRITE
            // elements: a
            // token labels: 
            // rule labels: retval, a
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 84:86: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }

            retval.tree = root_0;
            }


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "unaryPrefixExpr"

    public static class unaryPostfixExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "unaryPostfixExpr"
    // C:\\work5\\src\\dremel\\common\\Bql.g:85:1: unaryPostfixExpr : (a= atomExpr -> $a) (o= unaryPostfixOp -> ^( $o $unaryPostfixExpr) )* ;
    public final BqlParser.unaryPostfixExpr_return unaryPostfixExpr() throws RecognitionException {
        BqlParser.unaryPostfixExpr_return retval = new BqlParser.unaryPostfixExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.atomExpr_return a = null;

        BqlParser.unaryPostfixOp_return o = null;


        RewriteRuleSubtreeStream stream_unaryPostfixOp=new RewriteRuleSubtreeStream(adaptor,"rule unaryPostfixOp");
        RewriteRuleSubtreeStream stream_atomExpr=new RewriteRuleSubtreeStream(adaptor,"rule atomExpr");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:85:20: ( (a= atomExpr -> $a) (o= unaryPostfixOp -> ^( $o $unaryPostfixExpr) )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:85:22: (a= atomExpr -> $a) (o= unaryPostfixOp -> ^( $o $unaryPostfixExpr) )*
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:85:22: (a= atomExpr -> $a)
            // C:\\work5\\src\\dremel\\common\\Bql.g:85:23: a= atomExpr
            {
            pushFollow(FOLLOW_atomExpr_in_unaryPostfixExpr1394);
            a=atomExpr();

            state._fsp--;

            stream_atomExpr.add(a.getTree());


            // AST REWRITE
            // elements: a
            // token labels: 
            // rule labels: retval, a
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 85:33: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }

            retval.tree = root_0;
            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:85:39: (o= unaryPostfixOp -> ^( $o $unaryPostfixExpr) )*
            loop30:
            do {
                int alt30=2;
                int LA30_0 = input.LA(1);

                if ( (LA30_0==LPAREN||LA30_0==IN) ) {
                    alt30=1;
                }


                switch (alt30) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:85:40: o= unaryPostfixOp
            	    {
            	    pushFollow(FOLLOW_unaryPostfixOp_in_unaryPostfixExpr1403);
            	    o=unaryPostfixOp();

            	    state._fsp--;

            	    stream_unaryPostfixOp.add(o.getTree());


            	    // AST REWRITE
            	    // elements: unaryPostfixExpr, o
            	    // token labels: 
            	    // rule labels: retval, o
            	    // token list labels: 
            	    // rule list labels: 
            	    // wildcard labels: 
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_o=new RewriteRuleSubtreeStream(adaptor,"rule o",o!=null?o.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 85:61: -> ^( $o $unaryPostfixExpr)
            	    {
            	        // C:\\work5\\src\\dremel\\common\\Bql.g:85:65: ^( $o $unaryPostfixExpr)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(stream_o.nextNode(), root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }

            	    retval.tree = root_0;
            	    }
            	    break;

            	default :
            	    break loop30;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "unaryPostfixExpr"

    public static class atomExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "atomExpr"
    // C:\\work5\\src\\dremel\\common\\Bql.g:86:1: atomExpr : ( INT -> ^( N_INT INT ) | FLOAT -> ^( N_FLOAT FLOAT ) | STRING -> ^( N_STRING STRING ) | ( LPAREN expression RPAREN ) -> expression | columnPath );
    public final BqlParser.atomExpr_return atomExpr() throws RecognitionException {
        BqlParser.atomExpr_return retval = new BqlParser.atomExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token INT56=null;
        Token FLOAT57=null;
        Token STRING58=null;
        Token LPAREN59=null;
        Token RPAREN61=null;
        BqlParser.expression_return expression60 = null;

        BqlParser.columnPath_return columnPath62 = null;


        CommonTree INT56_tree=null;
        CommonTree FLOAT57_tree=null;
        CommonTree STRING58_tree=null;
        CommonTree LPAREN59_tree=null;
        CommonTree RPAREN61_tree=null;
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_FLOAT=new RewriteRuleTokenStream(adaptor,"token FLOAT");
        RewriteRuleTokenStream stream_INT=new RewriteRuleTokenStream(adaptor,"token INT");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:86:14: ( INT -> ^( N_INT INT ) | FLOAT -> ^( N_FLOAT FLOAT ) | STRING -> ^( N_STRING STRING ) | ( LPAREN expression RPAREN ) -> expression | columnPath )
            int alt31=5;
            switch ( input.LA(1) ) {
            case INT:
                {
                alt31=1;
                }
                break;
            case FLOAT:
                {
                alt31=2;
                }
                break;
            case STRING:
                {
                alt31=3;
                }
                break;
            case LPAREN:
                {
                alt31=4;
                }
                break;
            case ID:
            case STAR:
                {
                alt31=5;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 31, 0, input);

                throw nvae;
            }

            switch (alt31) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:86:16: INT
                    {
                    INT56=(Token)match(input,INT,FOLLOW_INT_in_atomExpr1431);  
                    stream_INT.add(INT56);



                    // AST REWRITE
                    // elements: INT
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 86:29: -> ^( N_INT INT )
                    {
                        // C:\\work5\\src\\dremel\\common\\Bql.g:86:33: ^( N_INT INT )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_INT, "N_INT"), root_1);

                        adaptor.addChild(root_1, stream_INT.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:87:9: FLOAT
                    {
                    FLOAT57=(Token)match(input,FLOAT,FOLLOW_FLOAT_in_atomExpr1459);  
                    stream_FLOAT.add(FLOAT57);



                    // AST REWRITE
                    // elements: FLOAT
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 87:23: -> ^( N_FLOAT FLOAT )
                    {
                        // C:\\work5\\src\\dremel\\common\\Bql.g:87:27: ^( N_FLOAT FLOAT )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_FLOAT, "N_FLOAT"), root_1);

                        adaptor.addChild(root_1, stream_FLOAT.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 3 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:88:9: STRING
                    {
                    STRING58=(Token)match(input,STRING,FOLLOW_STRING_in_atomExpr1487);  
                    stream_STRING.add(STRING58);



                    // AST REWRITE
                    // elements: STRING
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 88:24: -> ^( N_STRING STRING )
                    {
                        // C:\\work5\\src\\dremel\\common\\Bql.g:88:28: ^( N_STRING STRING )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_STRING, "N_STRING"), root_1);

                        adaptor.addChild(root_1, stream_STRING.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 4 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:89:9: ( LPAREN expression RPAREN )
                    {
                    // C:\\work5\\src\\dremel\\common\\Bql.g:89:9: ( LPAREN expression RPAREN )
                    // C:\\work5\\src\\dremel\\common\\Bql.g:89:10: LPAREN expression RPAREN
                    {
                    LPAREN59=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_atomExpr1517);  
                    stream_LPAREN.add(LPAREN59);

                    pushFollow(FOLLOW_expression_in_atomExpr1519);
                    expression60=expression();

                    state._fsp--;

                    stream_expression.add(expression60.getTree());
                    RPAREN61=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_atomExpr1521);  
                    stream_RPAREN.add(RPAREN61);


                    }



                    // AST REWRITE
                    // elements: expression
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 89:42: -> expression
                    {
                        adaptor.addChild(root_0, stream_expression.nextTree());

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 5 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:90:9: columnPath
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_columnPath_in_atomExpr1545);
                    columnPath62=columnPath();

                    state._fsp--;

                    adaptor.addChild(root_0, columnPath62.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "atomExpr"

    public static class binary11thPrcdOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary11thPrcdOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:95:1: binary11thPrcdOp : LOGICAL_OR -> N_LOGICAL_OR ;
    public final BqlParser.binary11thPrcdOp_return binary11thPrcdOp() throws RecognitionException {
        BqlParser.binary11thPrcdOp_return retval = new BqlParser.binary11thPrcdOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token LOGICAL_OR63=null;

        CommonTree LOGICAL_OR63_tree=null;
        RewriteRuleTokenStream stream_LOGICAL_OR=new RewriteRuleTokenStream(adaptor,"token LOGICAL_OR");

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:95:20: ( LOGICAL_OR -> N_LOGICAL_OR )
            // C:\\work5\\src\\dremel\\common\\Bql.g:95:22: LOGICAL_OR
            {
            LOGICAL_OR63=(Token)match(input,LOGICAL_OR,FOLLOW_LOGICAL_OR_in_binary11thPrcdOp1558);  
            stream_LOGICAL_OR.add(LOGICAL_OR63);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 95:40: -> N_LOGICAL_OR
            {
                adaptor.addChild(root_0, (CommonTree)adaptor.create(N_LOGICAL_OR, "N_LOGICAL_OR"));

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary11thPrcdOp"

    public static class binary10thPrcdOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary10thPrcdOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:96:1: binary10thPrcdOp : LOGICAL_AND -> N_LOGICAL_AND ;
    public final BqlParser.binary10thPrcdOp_return binary10thPrcdOp() throws RecognitionException {
        BqlParser.binary10thPrcdOp_return retval = new BqlParser.binary10thPrcdOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token LOGICAL_AND64=null;

        CommonTree LOGICAL_AND64_tree=null;
        RewriteRuleTokenStream stream_LOGICAL_AND=new RewriteRuleTokenStream(adaptor,"token LOGICAL_AND");

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:96:20: ( LOGICAL_AND -> N_LOGICAL_AND )
            // C:\\work5\\src\\dremel\\common\\Bql.g:96:22: LOGICAL_AND
            {
            LOGICAL_AND64=(Token)match(input,LOGICAL_AND,FOLLOW_LOGICAL_AND_in_binary10thPrcdOp1579);  
            stream_LOGICAL_AND.add(LOGICAL_AND64);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 96:41: -> N_LOGICAL_AND
            {
                adaptor.addChild(root_0, (CommonTree)adaptor.create(N_LOGICAL_AND, "N_LOGICAL_AND"));

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary10thPrcdOp"

    public static class binary9thPrcdOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary9thPrcdOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:97:1: binary9thPrcdOp : BITWISE_OR -> N_BITWISE_OR ;
    public final BqlParser.binary9thPrcdOp_return binary9thPrcdOp() throws RecognitionException {
        BqlParser.binary9thPrcdOp_return retval = new BqlParser.binary9thPrcdOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token BITWISE_OR65=null;

        CommonTree BITWISE_OR65_tree=null;
        RewriteRuleTokenStream stream_BITWISE_OR=new RewriteRuleTokenStream(adaptor,"token BITWISE_OR");

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:97:20: ( BITWISE_OR -> N_BITWISE_OR )
            // C:\\work5\\src\\dremel\\common\\Bql.g:97:22: BITWISE_OR
            {
            BITWISE_OR65=(Token)match(input,BITWISE_OR,FOLLOW_BITWISE_OR_in_binary9thPrcdOp1600);  
            stream_BITWISE_OR.add(BITWISE_OR65);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 97:40: -> N_BITWISE_OR
            {
                adaptor.addChild(root_0, (CommonTree)adaptor.create(N_BITWISE_OR, "N_BITWISE_OR"));

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary9thPrcdOp"

    public static class binary8thPrcdOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary8thPrcdOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:98:1: binary8thPrcdOp : BITWISE_XOR -> N_BITWISE_XOR ;
    public final BqlParser.binary8thPrcdOp_return binary8thPrcdOp() throws RecognitionException {
        BqlParser.binary8thPrcdOp_return retval = new BqlParser.binary8thPrcdOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token BITWISE_XOR66=null;

        CommonTree BITWISE_XOR66_tree=null;
        RewriteRuleTokenStream stream_BITWISE_XOR=new RewriteRuleTokenStream(adaptor,"token BITWISE_XOR");

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:98:20: ( BITWISE_XOR -> N_BITWISE_XOR )
            // C:\\work5\\src\\dremel\\common\\Bql.g:98:22: BITWISE_XOR
            {
            BITWISE_XOR66=(Token)match(input,BITWISE_XOR,FOLLOW_BITWISE_XOR_in_binary8thPrcdOp1621);  
            stream_BITWISE_XOR.add(BITWISE_XOR66);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 98:41: -> N_BITWISE_XOR
            {
                adaptor.addChild(root_0, (CommonTree)adaptor.create(N_BITWISE_XOR, "N_BITWISE_XOR"));

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary8thPrcdOp"

    public static class binary7thPrcdOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary7thPrcdOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:99:1: binary7thPrcdOp : BITWISE_AND -> N_BITWISE_AND ;
    public final BqlParser.binary7thPrcdOp_return binary7thPrcdOp() throws RecognitionException {
        BqlParser.binary7thPrcdOp_return retval = new BqlParser.binary7thPrcdOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token BITWISE_AND67=null;

        CommonTree BITWISE_AND67_tree=null;
        RewriteRuleTokenStream stream_BITWISE_AND=new RewriteRuleTokenStream(adaptor,"token BITWISE_AND");

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:99:20: ( BITWISE_AND -> N_BITWISE_AND )
            // C:\\work5\\src\\dremel\\common\\Bql.g:99:22: BITWISE_AND
            {
            BITWISE_AND67=(Token)match(input,BITWISE_AND,FOLLOW_BITWISE_AND_in_binary7thPrcdOp1642);  
            stream_BITWISE_AND.add(BITWISE_AND67);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 99:41: -> N_BITWISE_AND
            {
                adaptor.addChild(root_0, (CommonTree)adaptor.create(N_BITWISE_AND, "N_BITWISE_AND"));

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary7thPrcdOp"

    public static class binary6thPrcdOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary6thPrcdOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:100:1: binary6thPrcdOp : ( EQUAL -> N_EQUAL | NOT_EQUAL -> N_NOT_EQUAL );
    public final BqlParser.binary6thPrcdOp_return binary6thPrcdOp() throws RecognitionException {
        BqlParser.binary6thPrcdOp_return retval = new BqlParser.binary6thPrcdOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token EQUAL68=null;
        Token NOT_EQUAL69=null;

        CommonTree EQUAL68_tree=null;
        CommonTree NOT_EQUAL69_tree=null;
        RewriteRuleTokenStream stream_NOT_EQUAL=new RewriteRuleTokenStream(adaptor,"token NOT_EQUAL");
        RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:100:20: ( EQUAL -> N_EQUAL | NOT_EQUAL -> N_NOT_EQUAL )
            int alt32=2;
            int LA32_0 = input.LA(1);

            if ( (LA32_0==EQUAL) ) {
                alt32=1;
            }
            else if ( (LA32_0==NOT_EQUAL) ) {
                alt32=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 32, 0, input);

                throw nvae;
            }
            switch (alt32) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:100:22: EQUAL
                    {
                    EQUAL68=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_binary6thPrcdOp1663);  
                    stream_EQUAL.add(EQUAL68);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 100:36: -> N_EQUAL
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_EQUAL, "N_EQUAL"));

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:101:9: NOT_EQUAL
                    {
                    NOT_EQUAL69=(Token)match(input,NOT_EQUAL,FOLLOW_NOT_EQUAL_in_binary6thPrcdOp1686);  
                    stream_NOT_EQUAL.add(NOT_EQUAL69);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 101:26: -> N_NOT_EQUAL
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_NOT_EQUAL, "N_NOT_EQUAL"));

                    }

                    retval.tree = root_0;
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary6thPrcdOp"

    public static class binary5thPrcdOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary5thPrcdOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:102:1: binary5thPrcdOp : ( LESS_THAN -> N_LESS_THAN | LESS_THAN_OR_EQUAL -> N_LESS_THAN_OR_EQUAL | GREATER_THAN -> N_GREATER_THAN | GREATER_THAN_OR_EQUAL -> N_GREATER_THAN_OR_EQUAL );
    public final BqlParser.binary5thPrcdOp_return binary5thPrcdOp() throws RecognitionException {
        BqlParser.binary5thPrcdOp_return retval = new BqlParser.binary5thPrcdOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token LESS_THAN70=null;
        Token LESS_THAN_OR_EQUAL71=null;
        Token GREATER_THAN72=null;
        Token GREATER_THAN_OR_EQUAL73=null;

        CommonTree LESS_THAN70_tree=null;
        CommonTree LESS_THAN_OR_EQUAL71_tree=null;
        CommonTree GREATER_THAN72_tree=null;
        CommonTree GREATER_THAN_OR_EQUAL73_tree=null;
        RewriteRuleTokenStream stream_GREATER_THAN_OR_EQUAL=new RewriteRuleTokenStream(adaptor,"token GREATER_THAN_OR_EQUAL");
        RewriteRuleTokenStream stream_LESS_THAN_OR_EQUAL=new RewriteRuleTokenStream(adaptor,"token LESS_THAN_OR_EQUAL");
        RewriteRuleTokenStream stream_GREATER_THAN=new RewriteRuleTokenStream(adaptor,"token GREATER_THAN");
        RewriteRuleTokenStream stream_LESS_THAN=new RewriteRuleTokenStream(adaptor,"token LESS_THAN");

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:102:20: ( LESS_THAN -> N_LESS_THAN | LESS_THAN_OR_EQUAL -> N_LESS_THAN_OR_EQUAL | GREATER_THAN -> N_GREATER_THAN | GREATER_THAN_OR_EQUAL -> N_GREATER_THAN_OR_EQUAL )
            int alt33=4;
            switch ( input.LA(1) ) {
            case LESS_THAN:
                {
                alt33=1;
                }
                break;
            case LESS_THAN_OR_EQUAL:
                {
                alt33=2;
                }
                break;
            case GREATER_THAN:
                {
                alt33=3;
                }
                break;
            case GREATER_THAN_OR_EQUAL:
                {
                alt33=4;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 33, 0, input);

                throw nvae;
            }

            switch (alt33) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:102:22: LESS_THAN
                    {
                    LESS_THAN70=(Token)match(input,LESS_THAN,FOLLOW_LESS_THAN_in_binary5thPrcdOp1707);  
                    stream_LESS_THAN.add(LESS_THAN70);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 102:39: -> N_LESS_THAN
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_LESS_THAN, "N_LESS_THAN"));

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:103:9: LESS_THAN_OR_EQUAL
                    {
                    LESS_THAN_OR_EQUAL71=(Token)match(input,LESS_THAN_OR_EQUAL,FOLLOW_LESS_THAN_OR_EQUAL_in_binary5thPrcdOp1729);  
                    stream_LESS_THAN_OR_EQUAL.add(LESS_THAN_OR_EQUAL71);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 103:34: -> N_LESS_THAN_OR_EQUAL
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_LESS_THAN_OR_EQUAL, "N_LESS_THAN_OR_EQUAL"));

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 3 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:104:9: GREATER_THAN
                    {
                    GREATER_THAN72=(Token)match(input,GREATER_THAN,FOLLOW_GREATER_THAN_in_binary5thPrcdOp1750);  
                    stream_GREATER_THAN.add(GREATER_THAN72);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 104:29: -> N_GREATER_THAN
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_GREATER_THAN, "N_GREATER_THAN"));

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 4 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:105:9: GREATER_THAN_OR_EQUAL
                    {
                    GREATER_THAN_OR_EQUAL73=(Token)match(input,GREATER_THAN_OR_EQUAL,FOLLOW_GREATER_THAN_OR_EQUAL_in_binary5thPrcdOp1772);  
                    stream_GREATER_THAN_OR_EQUAL.add(GREATER_THAN_OR_EQUAL73);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 105:37: -> N_GREATER_THAN_OR_EQUAL
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_GREATER_THAN_OR_EQUAL, "N_GREATER_THAN_OR_EQUAL"));

                    }

                    retval.tree = root_0;
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary5thPrcdOp"

    public static class binary4thPrcdOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary4thPrcdOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:106:1: binary4thPrcdOp : ( BITWISE_LEFT_SHIFT -> N_BITWISE_LEFT_SHIFT | BITWISE_RIGHT_SHIFT -> N_BITWISE_RIGHT_SHIFT );
    public final BqlParser.binary4thPrcdOp_return binary4thPrcdOp() throws RecognitionException {
        BqlParser.binary4thPrcdOp_return retval = new BqlParser.binary4thPrcdOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token BITWISE_LEFT_SHIFT74=null;
        Token BITWISE_RIGHT_SHIFT75=null;

        CommonTree BITWISE_LEFT_SHIFT74_tree=null;
        CommonTree BITWISE_RIGHT_SHIFT75_tree=null;
        RewriteRuleTokenStream stream_BITWISE_LEFT_SHIFT=new RewriteRuleTokenStream(adaptor,"token BITWISE_LEFT_SHIFT");
        RewriteRuleTokenStream stream_BITWISE_RIGHT_SHIFT=new RewriteRuleTokenStream(adaptor,"token BITWISE_RIGHT_SHIFT");

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:106:20: ( BITWISE_LEFT_SHIFT -> N_BITWISE_LEFT_SHIFT | BITWISE_RIGHT_SHIFT -> N_BITWISE_RIGHT_SHIFT )
            int alt34=2;
            int LA34_0 = input.LA(1);

            if ( (LA34_0==BITWISE_LEFT_SHIFT) ) {
                alt34=1;
            }
            else if ( (LA34_0==BITWISE_RIGHT_SHIFT) ) {
                alt34=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 34, 0, input);

                throw nvae;
            }
            switch (alt34) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:106:22: BITWISE_LEFT_SHIFT
                    {
                    BITWISE_LEFT_SHIFT74=(Token)match(input,BITWISE_LEFT_SHIFT,FOLLOW_BITWISE_LEFT_SHIFT_in_binary4thPrcdOp1792);  
                    stream_BITWISE_LEFT_SHIFT.add(BITWISE_LEFT_SHIFT74);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 106:47: -> N_BITWISE_LEFT_SHIFT
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_BITWISE_LEFT_SHIFT, "N_BITWISE_LEFT_SHIFT"));

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:107:9: BITWISE_RIGHT_SHIFT
                    {
                    BITWISE_RIGHT_SHIFT75=(Token)match(input,BITWISE_RIGHT_SHIFT,FOLLOW_BITWISE_RIGHT_SHIFT_in_binary4thPrcdOp1813);  
                    stream_BITWISE_RIGHT_SHIFT.add(BITWISE_RIGHT_SHIFT75);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 107:35: -> N_BITWISE_RIGHT_SHIFT
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_BITWISE_RIGHT_SHIFT, "N_BITWISE_RIGHT_SHIFT"));

                    }

                    retval.tree = root_0;
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary4thPrcdOp"

    public static class binary3rdPrcdOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary3rdPrcdOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:108:1: binary3rdPrcdOp : ( ADD -> N_ADD | SUBSTRUCT -> N_SUBSTRUCT );
    public final BqlParser.binary3rdPrcdOp_return binary3rdPrcdOp() throws RecognitionException {
        BqlParser.binary3rdPrcdOp_return retval = new BqlParser.binary3rdPrcdOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ADD76=null;
        Token SUBSTRUCT77=null;

        CommonTree ADD76_tree=null;
        CommonTree SUBSTRUCT77_tree=null;
        RewriteRuleTokenStream stream_SUBSTRUCT=new RewriteRuleTokenStream(adaptor,"token SUBSTRUCT");
        RewriteRuleTokenStream stream_ADD=new RewriteRuleTokenStream(adaptor,"token ADD");

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:108:20: ( ADD -> N_ADD | SUBSTRUCT -> N_SUBSTRUCT )
            int alt35=2;
            int LA35_0 = input.LA(1);

            if ( (LA35_0==ADD) ) {
                alt35=1;
            }
            else if ( (LA35_0==SUBSTRUCT) ) {
                alt35=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 35, 0, input);

                throw nvae;
            }
            switch (alt35) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:108:22: ADD
                    {
                    ADD76=(Token)match(input,ADD,FOLLOW_ADD_in_binary3rdPrcdOp1833);  
                    stream_ADD.add(ADD76);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 108:34: -> N_ADD
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_ADD, "N_ADD"));

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:109:9: SUBSTRUCT
                    {
                    SUBSTRUCT77=(Token)match(input,SUBSTRUCT,FOLLOW_SUBSTRUCT_in_binary3rdPrcdOp1856);  
                    stream_SUBSTRUCT.add(SUBSTRUCT77);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 109:18: -> N_SUBSTRUCT
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_SUBSTRUCT, "N_SUBSTRUCT"));

                    }

                    retval.tree = root_0;
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary3rdPrcdOp"

    public static class binary2ndPrcdOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary2ndPrcdOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:110:1: binary2ndPrcdOp : ( multiplyOp -> N_MULTIPLY | divideOp -> N_DIVIDE | REMAINDER -> N_REMAINDER );
    public final BqlParser.binary2ndPrcdOp_return binary2ndPrcdOp() throws RecognitionException {
        BqlParser.binary2ndPrcdOp_return retval = new BqlParser.binary2ndPrcdOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token REMAINDER80=null;
        BqlParser.multiplyOp_return multiplyOp78 = null;

        BqlParser.divideOp_return divideOp79 = null;


        CommonTree REMAINDER80_tree=null;
        RewriteRuleTokenStream stream_REMAINDER=new RewriteRuleTokenStream(adaptor,"token REMAINDER");
        RewriteRuleSubtreeStream stream_divideOp=new RewriteRuleSubtreeStream(adaptor,"rule divideOp");
        RewriteRuleSubtreeStream stream_multiplyOp=new RewriteRuleSubtreeStream(adaptor,"rule multiplyOp");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:110:20: ( multiplyOp -> N_MULTIPLY | divideOp -> N_DIVIDE | REMAINDER -> N_REMAINDER )
            int alt36=3;
            switch ( input.LA(1) ) {
            case STAR:
                {
                alt36=1;
                }
                break;
            case SLASH:
            case DIV:
                {
                alt36=2;
                }
                break;
            case REMAINDER:
                {
                alt36=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 36, 0, input);

                throw nvae;
            }

            switch (alt36) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:110:22: multiplyOp
                    {
                    pushFollow(FOLLOW_multiplyOp_in_binary2ndPrcdOp1868);
                    multiplyOp78=multiplyOp();

                    state._fsp--;

                    stream_multiplyOp.add(multiplyOp78.getTree());


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 110:40: -> N_MULTIPLY
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_MULTIPLY, "N_MULTIPLY"));

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:111:9: divideOp
                    {
                    pushFollow(FOLLOW_divideOp_in_binary2ndPrcdOp1890);
                    divideOp79=divideOp();

                    state._fsp--;

                    stream_divideOp.add(divideOp79.getTree());


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 111:25: -> N_DIVIDE
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_DIVIDE, "N_DIVIDE"));

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 3 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:112:9: REMAINDER
                    {
                    REMAINDER80=(Token)match(input,REMAINDER,FOLLOW_REMAINDER_in_binary2ndPrcdOp1912);  
                    stream_REMAINDER.add(REMAINDER80);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 112:26: -> N_REMAINDER
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_REMAINDER, "N_REMAINDER"));

                    }

                    retval.tree = root_0;
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary2ndPrcdOp"

    public static class binary1stPrcdOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binary1stPrcdOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:113:1: binary1stPrcdOp : CONTAINS -> N_CONTAINS ;
    public final BqlParser.binary1stPrcdOp_return binary1stPrcdOp() throws RecognitionException {
        BqlParser.binary1stPrcdOp_return retval = new BqlParser.binary1stPrcdOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token CONTAINS81=null;

        CommonTree CONTAINS81_tree=null;
        RewriteRuleTokenStream stream_CONTAINS=new RewriteRuleTokenStream(adaptor,"token CONTAINS");

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:113:20: ( CONTAINS -> N_CONTAINS )
            // C:\\work5\\src\\dremel\\common\\Bql.g:113:22: CONTAINS
            {
            CONTAINS81=(Token)match(input,CONTAINS,FOLLOW_CONTAINS_in_binary1stPrcdOp1933);  
            stream_CONTAINS.add(CONTAINS81);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 113:38: -> N_CONTAINS
            {
                adaptor.addChild(root_0, (CommonTree)adaptor.create(N_CONTAINS, "N_CONTAINS"));

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binary1stPrcdOp"

    public static class unaryPrefixOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "unaryPrefixOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:114:1: unaryPrefixOp : ( BITWISE_NOT -> N_BITWISE_NOT | LOGICAL_NOT -> N_LOGICAL_NOT );
    public final BqlParser.unaryPrefixOp_return unaryPrefixOp() throws RecognitionException {
        BqlParser.unaryPrefixOp_return retval = new BqlParser.unaryPrefixOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token BITWISE_NOT82=null;
        Token LOGICAL_NOT83=null;

        CommonTree BITWISE_NOT82_tree=null;
        CommonTree LOGICAL_NOT83_tree=null;
        RewriteRuleTokenStream stream_LOGICAL_NOT=new RewriteRuleTokenStream(adaptor,"token LOGICAL_NOT");
        RewriteRuleTokenStream stream_BITWISE_NOT=new RewriteRuleTokenStream(adaptor,"token BITWISE_NOT");

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:114:18: ( BITWISE_NOT -> N_BITWISE_NOT | LOGICAL_NOT -> N_LOGICAL_NOT )
            int alt37=2;
            int LA37_0 = input.LA(1);

            if ( (LA37_0==BITWISE_NOT) ) {
                alt37=1;
            }
            else if ( (LA37_0==LOGICAL_NOT) ) {
                alt37=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 37, 0, input);

                throw nvae;
            }
            switch (alt37) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:114:20: BITWISE_NOT
                    {
                    BITWISE_NOT82=(Token)match(input,BITWISE_NOT,FOLLOW_BITWISE_NOT_in_unaryPrefixOp1955);  
                    stream_BITWISE_NOT.add(BITWISE_NOT82);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 114:39: -> N_BITWISE_NOT
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_BITWISE_NOT, "N_BITWISE_NOT"));

                    }

                    retval.tree = root_0;
                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:115:9: LOGICAL_NOT
                    {
                    LOGICAL_NOT83=(Token)match(input,LOGICAL_NOT,FOLLOW_LOGICAL_NOT_in_unaryPrefixOp1977);  
                    stream_LOGICAL_NOT.add(LOGICAL_NOT83);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 115:28: -> N_LOGICAL_NOT
                    {
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(N_LOGICAL_NOT, "N_LOGICAL_NOT"));

                    }

                    retval.tree = root_0;
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "unaryPrefixOp"

    public static class unaryPostfixOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "unaryPostfixOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:116:1: unaryPostfixOp : ( unaryPostfixOpIn | unaryPostfixOpCall );
    public final BqlParser.unaryPostfixOp_return unaryPostfixOp() throws RecognitionException {
        BqlParser.unaryPostfixOp_return retval = new BqlParser.unaryPostfixOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        BqlParser.unaryPostfixOpIn_return unaryPostfixOpIn84 = null;

        BqlParser.unaryPostfixOpCall_return unaryPostfixOpCall85 = null;



        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:116:19: ( unaryPostfixOpIn | unaryPostfixOpCall )
            int alt38=2;
            int LA38_0 = input.LA(1);

            if ( (LA38_0==IN) ) {
                alt38=1;
            }
            else if ( (LA38_0==LPAREN) ) {
                alt38=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 38, 0, input);

                throw nvae;
            }
            switch (alt38) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:116:21: unaryPostfixOpIn
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_unaryPostfixOpIn_in_unaryPostfixOp1998);
                    unaryPostfixOpIn84=unaryPostfixOpIn();

                    state._fsp--;

                    adaptor.addChild(root_0, unaryPostfixOpIn84.getTree());

                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:116:40: unaryPostfixOpCall
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_unaryPostfixOpCall_in_unaryPostfixOp2002);
                    unaryPostfixOpCall85=unaryPostfixOpCall();

                    state._fsp--;

                    adaptor.addChild(root_0, unaryPostfixOpCall85.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "unaryPostfixOp"

    public static class unaryPostfixOpIn_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "unaryPostfixOpIn"
    // C:\\work5\\src\\dremel\\common\\Bql.g:117:1: unaryPostfixOpIn : IN LPAREN ( expression ( COMMA expression )* )? RPAREN -> ^( N_IN_PARAMS ( expression )* ) ;
    public final BqlParser.unaryPostfixOpIn_return unaryPostfixOpIn() throws RecognitionException {
        BqlParser.unaryPostfixOpIn_return retval = new BqlParser.unaryPostfixOpIn_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token IN86=null;
        Token LPAREN87=null;
        Token COMMA89=null;
        Token RPAREN91=null;
        BqlParser.expression_return expression88 = null;

        BqlParser.expression_return expression90 = null;


        CommonTree IN86_tree=null;
        CommonTree LPAREN87_tree=null;
        CommonTree COMMA89_tree=null;
        CommonTree RPAREN91_tree=null;
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_IN=new RewriteRuleTokenStream(adaptor,"token IN");
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:117:20: ( IN LPAREN ( expression ( COMMA expression )* )? RPAREN -> ^( N_IN_PARAMS ( expression )* ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:117:22: IN LPAREN ( expression ( COMMA expression )* )? RPAREN
            {
            IN86=(Token)match(input,IN,FOLLOW_IN_in_unaryPostfixOpIn2011);  
            stream_IN.add(IN86);

            LPAREN87=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_unaryPostfixOpIn2013);  
            stream_LPAREN.add(LPAREN87);

            // C:\\work5\\src\\dremel\\common\\Bql.g:117:32: ( expression ( COMMA expression )* )?
            int alt40=2;
            int LA40_0 = input.LA(1);

            if ( (LA40_0==LPAREN||LA40_0==INT||(LA40_0>=ID && LA40_0<=STRING)||(LA40_0>=BITWISE_NOT && LA40_0<=LOGICAL_NOT)) ) {
                alt40=1;
            }
            switch (alt40) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:117:33: expression ( COMMA expression )*
                    {
                    pushFollow(FOLLOW_expression_in_unaryPostfixOpIn2016);
                    expression88=expression();

                    state._fsp--;

                    stream_expression.add(expression88.getTree());
                    // C:\\work5\\src\\dremel\\common\\Bql.g:117:44: ( COMMA expression )*
                    loop39:
                    do {
                        int alt39=2;
                        int LA39_0 = input.LA(1);

                        if ( (LA39_0==COMMA) ) {
                            alt39=1;
                        }


                        switch (alt39) {
                    	case 1 :
                    	    // C:\\work5\\src\\dremel\\common\\Bql.g:117:45: COMMA expression
                    	    {
                    	    COMMA89=(Token)match(input,COMMA,FOLLOW_COMMA_in_unaryPostfixOpIn2019);  
                    	    stream_COMMA.add(COMMA89);

                    	    pushFollow(FOLLOW_expression_in_unaryPostfixOpIn2021);
                    	    expression90=expression();

                    	    state._fsp--;

                    	    stream_expression.add(expression90.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop39;
                        }
                    } while (true);


                    }
                    break;

            }

            RPAREN91=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_unaryPostfixOpIn2027);  
            stream_RPAREN.add(RPAREN91);



            // AST REWRITE
            // elements: expression
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 117:75: -> ^( N_IN_PARAMS ( expression )* )
            {
                // C:\\work5\\src\\dremel\\common\\Bql.g:117:78: ^( N_IN_PARAMS ( expression )* )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_IN_PARAMS, "N_IN_PARAMS"), root_1);

                // C:\\work5\\src\\dremel\\common\\Bql.g:117:92: ( expression )*
                while ( stream_expression.hasNext() ) {
                    adaptor.addChild(root_1, stream_expression.nextTree());

                }
                stream_expression.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "unaryPostfixOpIn"

    public static class unaryPostfixOpCall_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "unaryPostfixOpCall"
    // C:\\work5\\src\\dremel\\common\\Bql.g:118:1: unaryPostfixOpCall : LPAREN ( expression ( COMMA expression )* )? RPAREN -> ^( N_CALL_PARAMS ( expression )* ) ;
    public final BqlParser.unaryPostfixOpCall_return unaryPostfixOpCall() throws RecognitionException {
        BqlParser.unaryPostfixOpCall_return retval = new BqlParser.unaryPostfixOpCall_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token LPAREN92=null;
        Token COMMA94=null;
        Token RPAREN96=null;
        BqlParser.expression_return expression93 = null;

        BqlParser.expression_return expression95 = null;


        CommonTree LPAREN92_tree=null;
        CommonTree COMMA94_tree=null;
        CommonTree RPAREN96_tree=null;
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:118:22: ( LPAREN ( expression ( COMMA expression )* )? RPAREN -> ^( N_CALL_PARAMS ( expression )* ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:118:24: LPAREN ( expression ( COMMA expression )* )? RPAREN
            {
            LPAREN92=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_unaryPostfixOpCall2047);  
            stream_LPAREN.add(LPAREN92);

            // C:\\work5\\src\\dremel\\common\\Bql.g:118:31: ( expression ( COMMA expression )* )?
            int alt42=2;
            int LA42_0 = input.LA(1);

            if ( (LA42_0==LPAREN||LA42_0==INT||(LA42_0>=ID && LA42_0<=STRING)||(LA42_0>=BITWISE_NOT && LA42_0<=LOGICAL_NOT)) ) {
                alt42=1;
            }
            switch (alt42) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:118:32: expression ( COMMA expression )*
                    {
                    pushFollow(FOLLOW_expression_in_unaryPostfixOpCall2050);
                    expression93=expression();

                    state._fsp--;

                    stream_expression.add(expression93.getTree());
                    // C:\\work5\\src\\dremel\\common\\Bql.g:118:43: ( COMMA expression )*
                    loop41:
                    do {
                        int alt41=2;
                        int LA41_0 = input.LA(1);

                        if ( (LA41_0==COMMA) ) {
                            alt41=1;
                        }


                        switch (alt41) {
                    	case 1 :
                    	    // C:\\work5\\src\\dremel\\common\\Bql.g:118:44: COMMA expression
                    	    {
                    	    COMMA94=(Token)match(input,COMMA,FOLLOW_COMMA_in_unaryPostfixOpCall2053);  
                    	    stream_COMMA.add(COMMA94);

                    	    pushFollow(FOLLOW_expression_in_unaryPostfixOpCall2055);
                    	    expression95=expression();

                    	    state._fsp--;

                    	    stream_expression.add(expression95.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop41;
                        }
                    } while (true);


                    }
                    break;

            }

            RPAREN96=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_unaryPostfixOpCall2061);  
            stream_RPAREN.add(RPAREN96);



            // AST REWRITE
            // elements: expression
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 118:75: -> ^( N_CALL_PARAMS ( expression )* )
            {
                // C:\\work5\\src\\dremel\\common\\Bql.g:118:79: ^( N_CALL_PARAMS ( expression )* )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(N_CALL_PARAMS, "N_CALL_PARAMS"), root_1);

                // C:\\work5\\src\\dremel\\common\\Bql.g:118:95: ( expression )*
                while ( stream_expression.hasNext() ) {
                    adaptor.addChild(root_1, stream_expression.nextTree());

                }
                stream_expression.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "unaryPostfixOpCall"

    public static class divideOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "divideOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:121:1: divideOp : ( SLASH | DIV );
    public final BqlParser.divideOp_return divideOp() throws RecognitionException {
        BqlParser.divideOp_return retval = new BqlParser.divideOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set97=null;

        CommonTree set97_tree=null;

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:121:13: ( SLASH | DIV )
            // C:\\work5\\src\\dremel\\common\\Bql.g:
            {
            root_0 = (CommonTree)adaptor.nil();

            set97=(Token)input.LT(1);
            if ( (input.LA(1)>=SLASH && input.LA(1)<=DIV) ) {
                input.consume();
                adaptor.addChild(root_0, (CommonTree)adaptor.create(set97));
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "divideOp"

    public static class multiplyOp_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "multiplyOp"
    // C:\\work5\\src\\dremel\\common\\Bql.g:122:1: multiplyOp : STAR ;
    public final BqlParser.multiplyOp_return multiplyOp() throws RecognitionException {
        BqlParser.multiplyOp_return retval = new BqlParser.multiplyOp_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token STAR98=null;

        CommonTree STAR98_tree=null;

        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:122:15: ( STAR )
            // C:\\work5\\src\\dremel\\common\\Bql.g:122:17: STAR
            {
            root_0 = (CommonTree)adaptor.nil();

            STAR98=(Token)match(input,STAR,FOLLOW_STAR_in_multiplyOp2100); 
            STAR98_tree = (CommonTree)adaptor.create(STAR98);
            adaptor.addChild(root_0, STAR98_tree);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "multiplyOp"

    // Delegated rules


 

    public static final BitSet FOLLOW_selectStatement_in_request180 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_SEMICOLON_in_request183 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_request188 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_selectClause_in_selectStatement207 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_fromClause_in_selectStatement209 = new BitSet(new long[]{0x7000000000000002L,0x0000000000000002L});
    public static final BitSet FOLLOW_whereClause_in_selectStatement211 = new BitSet(new long[]{0x6000000000000002L,0x0000000000000002L});
    public static final BitSet FOLLOW_groupbyClause_in_selectStatement222 = new BitSet(new long[]{0x4000000000000002L,0x0000000000000002L});
    public static final BitSet FOLLOW_orderbyClause_in_selectStatement225 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000002L});
    public static final BitSet FOLLOW_limitClause_in_selectStatement228 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_SELECT_in_selectClause267 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_columnExpr_in_selectClause269 = new BitSet(new long[]{0x0020000000000002L});
    public static final BitSet FOLLOW_COMMA_in_selectClause272 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_columnExpr_in_selectClause274 = new BitSet(new long[]{0x0020000000000002L});
    public static final BitSet FOLLOW_expression_in_columnExpr302 = new BitSet(new long[]{0x00C0000000000002L});
    public static final BitSet FOLLOW_withinClause_in_columnExpr304 = new BitSet(new long[]{0x0040000000000002L});
    public static final BitSet FOLLOW_AS_in_columnExpr308 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_columnName_in_columnExpr310 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_WITHIN_in_withinClause347 = new BitSet(new long[]{0x0500000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_RECORD_in_withinClause350 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_columnPath_in_withinClause376 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FROM_in_fromClause412 = new BitSet(new long[]{0x0400000000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_subSelectStatement_in_fromClause414 = new BitSet(new long[]{0x0020000000000002L});
    public static final BitSet FOLLOW_COMMA_in_fromClause417 = new BitSet(new long[]{0x0400000000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_subSelectStatement_in_fromClause419 = new BitSet(new long[]{0x0020000000000002L});
    public static final BitSet FOLLOW_tableName_in_subSelectStatement444 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_subSelectStatement449 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_selectStatement_in_subSelectStatement452 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_RPAREN_in_subSelectStatement454 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_WHERE_in_whereClause471 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_expression_in_whereClause473 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_GROUPBY_in_groupbyClause502 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_columnName_in_groupbyClause504 = new BitSet(new long[]{0x0020000000000002L});
    public static final BitSet FOLLOW_COMMA_in_groupbyClause507 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_columnName_in_groupbyClause509 = new BitSet(new long[]{0x0020000000000002L});
    public static final BitSet FOLLOW_ORDERBY_in_orderbyClause541 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_orderbyColumnName_in_orderbyClause543 = new BitSet(new long[]{0x0020000000000002L});
    public static final BitSet FOLLOW_COMMA_in_orderbyClause546 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_orderbyColumnName_in_orderbyClause548 = new BitSet(new long[]{0x0020000000000002L});
    public static final BitSet FOLLOW_columnName_in_orderbyColumnName571 = new BitSet(new long[]{0x8000000000000002L,0x0000000000000001L});
    public static final BitSet FOLLOW_ASC_in_orderbyColumnName574 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DESC_in_orderbyColumnName601 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LIMIT_in_limitClause669 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_INT_in_limitClause671 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_columnPath2_in_columnPath700 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_columnName_in_columnPath2727 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000008L});
    public static final BitSet FOLLOW_DOT_in_columnPath2730 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_columnName_in_columnPath2733 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000008L});
    public static final BitSet FOLLOW_ID_in_columnName752 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STAR_in_columnName764 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_tablePath2_in_tablePath786 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_tableName_in_tablePath2812 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000008L});
    public static final BitSet FOLLOW_DOT_in_tablePath2815 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_tableName_in_tablePath2818 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000008L});
    public static final BitSet FOLLOW_ID_in_tableName837 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expression2_in_expression863 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_binary10thPrcdExpr_in_expression2893 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000100L});
    public static final BitSet FOLLOW_binary11thPrcdOp_in_expression2902 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_binary10thPrcdExpr_in_expression2906 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000100L});
    public static final BitSet FOLLOW_binary9thPrcdExpr_in_binary10thPrcdExpr935 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000200L});
    public static final BitSet FOLLOW_binary10thPrcdOp_in_binary10thPrcdExpr944 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_binary9thPrcdExpr_in_binary10thPrcdExpr948 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000200L});
    public static final BitSet FOLLOW_binary8thPrcdExpr_in_binary9thPrcdExpr977 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000400L});
    public static final BitSet FOLLOW_binary9thPrcdOp_in_binary9thPrcdExpr986 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_binary8thPrcdExpr_in_binary9thPrcdExpr990 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000400L});
    public static final BitSet FOLLOW_binary7thPrcdExpr_in_binary8thPrcdExpr1019 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000800L});
    public static final BitSet FOLLOW_binary8thPrcdOp_in_binary8thPrcdExpr1028 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_binary7thPrcdExpr_in_binary8thPrcdExpr1032 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000800L});
    public static final BitSet FOLLOW_binary6thPrcdExpr_in_binary7thPrcdExpr1061 = new BitSet(new long[]{0x0000000000000002L,0x0000000000001000L});
    public static final BitSet FOLLOW_binary7thPrcdOp_in_binary7thPrcdExpr1070 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_binary6thPrcdExpr_in_binary7thPrcdExpr1074 = new BitSet(new long[]{0x0000000000000002L,0x0000000000001000L});
    public static final BitSet FOLLOW_binary5thPrcdExpr_in_binary6thPrcdExpr1103 = new BitSet(new long[]{0x0000000000000002L,0x0000000000006000L});
    public static final BitSet FOLLOW_binary6thPrcdOp_in_binary6thPrcdExpr1112 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_binary5thPrcdExpr_in_binary6thPrcdExpr1116 = new BitSet(new long[]{0x0000000000000002L,0x0000000000006000L});
    public static final BitSet FOLLOW_binary4thPrcdExpr_in_binary5thPrcdExpr1145 = new BitSet(new long[]{0x0000000000000002L,0x0000000000078000L});
    public static final BitSet FOLLOW_binary5thPrcdOp_in_binary5thPrcdExpr1154 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_binary4thPrcdExpr_in_binary5thPrcdExpr1158 = new BitSet(new long[]{0x0000000000000002L,0x0000000000078000L});
    public static final BitSet FOLLOW_binary3rdPrcdExpr_in_binary4thPrcdExpr1187 = new BitSet(new long[]{0x0000000000000002L,0x0000000000180000L});
    public static final BitSet FOLLOW_binary4thPrcdOp_in_binary4thPrcdExpr1196 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_binary3rdPrcdExpr_in_binary4thPrcdExpr1200 = new BitSet(new long[]{0x0000000000000002L,0x0000000000180000L});
    public static final BitSet FOLLOW_binary2ndPrcdExpr_in_binary3rdPrcdExpr1229 = new BitSet(new long[]{0x0000000000000002L,0x0000000000600000L});
    public static final BitSet FOLLOW_binary3rdPrcdOp_in_binary3rdPrcdExpr1238 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_binary2ndPrcdExpr_in_binary3rdPrcdExpr1242 = new BitSet(new long[]{0x0000000000000002L,0x0000000000600000L});
    public static final BitSet FOLLOW_binary1stPrcdExpr_in_binary2ndPrcdExpr1271 = new BitSet(new long[]{0x0000000000000002L,0x0000000030800020L});
    public static final BitSet FOLLOW_binary2ndPrcdOp_in_binary2ndPrcdExpr1280 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_binary1stPrcdExpr_in_binary2ndPrcdExpr1284 = new BitSet(new long[]{0x0000000000000002L,0x0000000030800020L});
    public static final BitSet FOLLOW_unaryPrefixExpr_in_binary1stPrcdExpr1313 = new BitSet(new long[]{0x0000000000000002L,0x0000000001000000L});
    public static final BitSet FOLLOW_binary1stPrcdOp_in_binary1stPrcdExpr1324 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_unaryPrefixExpr_in_binary1stPrcdExpr1328 = new BitSet(new long[]{0x0000000000000002L,0x0000000001000000L});
    public static final BitSet FOLLOW_unaryPrefixOp_in_unaryPrefixExpr1359 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_unaryPostfixExpr_in_unaryPrefixExpr1374 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_atomExpr_in_unaryPostfixExpr1394 = new BitSet(new long[]{0x0400000000000002L,0x0000000008000000L});
    public static final BitSet FOLLOW_unaryPostfixOp_in_unaryPostfixExpr1403 = new BitSet(new long[]{0x0400000000000002L,0x0000000008000000L});
    public static final BitSet FOLLOW_INT_in_atomExpr1431 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FLOAT_in_atomExpr1459 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_atomExpr1487 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_atomExpr1517 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_expression_in_atomExpr1519 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_RPAREN_in_atomExpr1521 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_columnPath_in_atomExpr1545 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LOGICAL_OR_in_binary11thPrcdOp1558 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LOGICAL_AND_in_binary10thPrcdOp1579 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_BITWISE_OR_in_binary9thPrcdOp1600 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_BITWISE_XOR_in_binary8thPrcdOp1621 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_BITWISE_AND_in_binary7thPrcdOp1642 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_EQUAL_in_binary6thPrcdOp1663 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NOT_EQUAL_in_binary6thPrcdOp1686 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LESS_THAN_in_binary5thPrcdOp1707 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LESS_THAN_OR_EQUAL_in_binary5thPrcdOp1729 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_GREATER_THAN_in_binary5thPrcdOp1750 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_GREATER_THAN_OR_EQUAL_in_binary5thPrcdOp1772 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_BITWISE_LEFT_SHIFT_in_binary4thPrcdOp1792 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_BITWISE_RIGHT_SHIFT_in_binary4thPrcdOp1813 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ADD_in_binary3rdPrcdOp1833 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_SUBSTRUCT_in_binary3rdPrcdOp1856 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_multiplyOp_in_binary2ndPrcdOp1868 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_divideOp_in_binary2ndPrcdOp1890 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_REMAINDER_in_binary2ndPrcdOp1912 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_CONTAINS_in_binary1stPrcdOp1933 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_BITWISE_NOT_in_unaryPrefixOp1955 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LOGICAL_NOT_in_unaryPrefixOp1977 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unaryPostfixOpIn_in_unaryPostfixOp1998 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unaryPostfixOpCall_in_unaryPostfixOp2002 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_IN_in_unaryPostfixOpIn2011 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_LPAREN_in_unaryPostfixOpIn2013 = new BitSet(new long[]{0x0C00000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_expression_in_unaryPostfixOpIn2016 = new BitSet(new long[]{0x0820000000000000L});
    public static final BitSet FOLLOW_COMMA_in_unaryPostfixOpIn2019 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_expression_in_unaryPostfixOpIn2021 = new BitSet(new long[]{0x0820000000000000L});
    public static final BitSet FOLLOW_RPAREN_in_unaryPostfixOpIn2027 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_unaryPostfixOpCall2047 = new BitSet(new long[]{0x0C00000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_expression_in_unaryPostfixOpCall2050 = new BitSet(new long[]{0x0820000000000000L});
    public static final BitSet FOLLOW_COMMA_in_unaryPostfixOpCall2053 = new BitSet(new long[]{0x0400000000000000L,0x00000000060000F4L});
    public static final BitSet FOLLOW_expression_in_unaryPostfixOpCall2055 = new BitSet(new long[]{0x0820000000000000L});
    public static final BitSet FOLLOW_RPAREN_in_unaryPostfixOpCall2061 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_divideOp0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STAR_in_multiplyOp2100 = new BitSet(new long[]{0x0000000000000002L});

}