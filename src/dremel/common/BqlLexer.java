// $ANTLR 3.2 Sep 23, 2009 12:02:23 C:\\work5\\src\\dremel\\common\\Bql.g 2010-09-20 09:15:41
package dremel.common;

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class BqlLexer extends Lexer {
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
    public static final int RECORD=56;
    public static final int N_WHERE=7;
    public static final int G=109;
    public static final int N_LESS_THAN=31;
    public static final int A=106;
    public static final int B=113;
    public static final int ASC=63;
    public static final int C=97;
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
    public static final int AS=54;
    public static final int N_SELECT=6;
    public static final int N_BITWISE_OR=26;
    public static final int N_ORDERBY=9;
    public static final int SLASH=92;
    public static final int IN=91;
    public static final int N_TABLE_NAME=19;
    public static final int COMMA=53;
    public static final int EQUAL=77;
    public static final int LOGICAL_OR=72;
    public static final int DOT=67;
    public static final int N_FROM=5;
    public static final int ADD=85;
    public static final int N_SELECT_STATEMENT=4;
    public static final int LESS_THAN_OR_EQUAL=80;
    public static final int N_REMAINDER=37;
    public static final int N_EQUAL=29;
    public static final int SUBSTRUCT=86;
    public static final int GREATER_THAN=81;
    public static final int N_WITHIN_RECORD=16;
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
    public static final int DESC=64;
    public static final int DIV=93;
    public static final int N_GREATER_THAN=33;
    public static final int STRING=71;

    // delegates
    // delegators

    public BqlLexer() {;} 
    public BqlLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public BqlLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);

    }
    public String getGrammarFileName() { return "C:\\work5\\src\\dremel\\common\\Bql.g"; }

    // $ANTLR start "SELECT"
    public final void mSELECT() throws RecognitionException {
        try {
            int _type = SELECT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:126:13: ( S E L E C T )
            // C:\\work5\\src\\dremel\\common\\Bql.g:126:15: S E L E C T
            {
            mS(); 
            mE(); 
            mL(); 
            mE(); 
            mC(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "SELECT"

    // $ANTLR start "WITHIN"
    public final void mWITHIN() throws RecognitionException {
        try {
            int _type = WITHIN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:127:12: ( W I T H I N )
            // C:\\work5\\src\\dremel\\common\\Bql.g:127:14: W I T H I N
            {
            mW(); 
            mI(); 
            mT(); 
            mH(); 
            mI(); 
            mN(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "WITHIN"

    // $ANTLR start "RECORD"
    public final void mRECORD() throws RecognitionException {
        try {
            int _type = RECORD;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:128:12: ( R E C O R D )
            // C:\\work5\\src\\dremel\\common\\Bql.g:128:14: R E C O R D
            {
            mR(); 
            mE(); 
            mC(); 
            mO(); 
            mR(); 
            mD(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "RECORD"

    // $ANTLR start "AS"
    public final void mAS() throws RecognitionException {
        try {
            int _type = AS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:129:8: ( A S )
            // C:\\work5\\src\\dremel\\common\\Bql.g:129:10: A S
            {
            mA(); 
            mS(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "AS"

    // $ANTLR start "FROM"
    public final void mFROM() throws RecognitionException {
        try {
            int _type = FROM;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:130:10: ( F R O M )
            // C:\\work5\\src\\dremel\\common\\Bql.g:130:12: F R O M
            {
            mF(); 
            mR(); 
            mO(); 
            mM(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "FROM"

    // $ANTLR start "WHERE"
    public final void mWHERE() throws RecognitionException {
        try {
            int _type = WHERE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:131:11: ( W H E R E )
            // C:\\work5\\src\\dremel\\common\\Bql.g:131:13: W H E R E
            {
            mW(); 
            mH(); 
            mE(); 
            mR(); 
            mE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "WHERE"

    // $ANTLR start "GROUPBY"
    public final void mGROUPBY() throws RecognitionException {
        try {
            int _type = GROUPBY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:132:13: ( G R O U P WS B Y )
            // C:\\work5\\src\\dremel\\common\\Bql.g:132:15: G R O U P WS B Y
            {
            mG(); 
            mR(); 
            mO(); 
            mU(); 
            mP(); 
            mWS(); 
            mB(); 
            mY(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "GROUPBY"

    // $ANTLR start "ORDERBY"
    public final void mORDERBY() throws RecognitionException {
        try {
            int _type = ORDERBY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:133:13: ( O R D E R WS B Y )
            // C:\\work5\\src\\dremel\\common\\Bql.g:133:15: O R D E R WS B Y
            {
            mO(); 
            mR(); 
            mD(); 
            mE(); 
            mR(); 
            mWS(); 
            mB(); 
            mY(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "ORDERBY"

    // $ANTLR start "DESC"
    public final void mDESC() throws RecognitionException {
        try {
            int _type = DESC;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:134:10: ( D E S C )
            // C:\\work5\\src\\dremel\\common\\Bql.g:134:12: D E S C
            {
            mD(); 
            mE(); 
            mS(); 
            mC(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "DESC"

    // $ANTLR start "ASC"
    public final void mASC() throws RecognitionException {
        try {
            int _type = ASC;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:135:9: ( A S C )
            // C:\\work5\\src\\dremel\\common\\Bql.g:135:11: A S C
            {
            mA(); 
            mS(); 
            mC(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "ASC"

    // $ANTLR start "LIMIT"
    public final void mLIMIT() throws RecognitionException {
        try {
            int _type = LIMIT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:136:11: ( L I M I T )
            // C:\\work5\\src\\dremel\\common\\Bql.g:136:13: L I M I T
            {
            mL(); 
            mI(); 
            mM(); 
            mI(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "LIMIT"

    // $ANTLR start "LOGICAL_OR"
    public final void mLOGICAL_OR() throws RecognitionException {
        try {
            int _type = LOGICAL_OR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:138:16: ( O R )
            // C:\\work5\\src\\dremel\\common\\Bql.g:138:18: O R
            {
            mO(); 
            mR(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "LOGICAL_OR"

    // $ANTLR start "LOGICAL_AND"
    public final void mLOGICAL_AND() throws RecognitionException {
        try {
            int _type = LOGICAL_AND;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:139:16: ( A N D )
            // C:\\work5\\src\\dremel\\common\\Bql.g:139:18: A N D
            {
            mA(); 
            mN(); 
            mD(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "LOGICAL_AND"

    // $ANTLR start "LOGICAL_NOT"
    public final void mLOGICAL_NOT() throws RecognitionException {
        try {
            int _type = LOGICAL_NOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:140:16: ( N O T )
            // C:\\work5\\src\\dremel\\common\\Bql.g:140:18: N O T
            {
            mN(); 
            mO(); 
            mT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "LOGICAL_NOT"

    // $ANTLR start "CONTAINS"
    public final void mCONTAINS() throws RecognitionException {
        try {
            int _type = CONTAINS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:141:14: ( C O N T A I N S )
            // C:\\work5\\src\\dremel\\common\\Bql.g:141:16: C O N T A I N S
            {
            mC(); 
            mO(); 
            mN(); 
            mT(); 
            mA(); 
            mI(); 
            mN(); 
            mS(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "CONTAINS"

    // $ANTLR start "IN"
    public final void mIN() throws RecognitionException {
        try {
            int _type = IN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:142:8: ( I N )
            // C:\\work5\\src\\dremel\\common\\Bql.g:142:10: I N
            {
            mI(); 
            mN(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "IN"

    // $ANTLR start "BITWISE_AND"
    public final void mBITWISE_AND() throws RecognitionException {
        try {
            int _type = BITWISE_AND;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:145:17: ( '&' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:145:20: '&'
            {
            match('&'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "BITWISE_AND"

    // $ANTLR start "BITWISE_NOT"
    public final void mBITWISE_NOT() throws RecognitionException {
        try {
            int _type = BITWISE_NOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:146:16: ( '~' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:146:19: '~'
            {
            match('~'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "BITWISE_NOT"

    // $ANTLR start "BITWISE_OR"
    public final void mBITWISE_OR() throws RecognitionException {
        try {
            int _type = BITWISE_OR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:147:15: ( '|' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:147:18: '|'
            {
            match('|'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "BITWISE_OR"

    // $ANTLR start "BITWISE_XOR"
    public final void mBITWISE_XOR() throws RecognitionException {
        try {
            int _type = BITWISE_XOR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:148:17: ( '^' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:148:20: '^'
            {
            match('^'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "BITWISE_XOR"

    // $ANTLR start "EQUAL"
    public final void mEQUAL() throws RecognitionException {
        try {
            int _type = EQUAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:149:12: ( '=' | '==' )
            int alt1=2;
            int LA1_0 = input.LA(1);

            if ( (LA1_0=='=') ) {
                int LA1_1 = input.LA(2);

                if ( (LA1_1=='=') ) {
                    alt1=2;
                }
                else {
                    alt1=1;}
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 1, 0, input);

                throw nvae;
            }
            switch (alt1) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:149:15: '='
                    {
                    match('='); 

                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:149:21: '=='
                    {
                    match("=="); 


                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "EQUAL"

    // $ANTLR start "NOT_EQUAL"
    public final void mNOT_EQUAL() throws RecognitionException {
        try {
            int _type = NOT_EQUAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:150:15: ( '<>' | '!=' )
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0=='<') ) {
                alt2=1;
            }
            else if ( (LA2_0=='!') ) {
                alt2=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }
            switch (alt2) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:150:18: '<>'
                    {
                    match("<>"); 


                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:150:25: '!='
                    {
                    match("!="); 


                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "NOT_EQUAL"

    // $ANTLR start "LESS_THAN_OR_EQUAL"
    public final void mLESS_THAN_OR_EQUAL() throws RecognitionException {
        try {
            int _type = LESS_THAN_OR_EQUAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:151:23: ( '<=' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:151:25: '<='
            {
            match("<="); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "LESS_THAN_OR_EQUAL"

    // $ANTLR start "LESS_THAN"
    public final void mLESS_THAN() throws RecognitionException {
        try {
            int _type = LESS_THAN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:152:15: ( '<' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:152:18: '<'
            {
            match('<'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "LESS_THAN"

    // $ANTLR start "GREATER_THAN_OR_EQUAL"
    public final void mGREATER_THAN_OR_EQUAL() throws RecognitionException {
        try {
            int _type = GREATER_THAN_OR_EQUAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:153:26: ( '>=' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:153:29: '>='
            {
            match(">="); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "GREATER_THAN_OR_EQUAL"

    // $ANTLR start "GREATER_THAN"
    public final void mGREATER_THAN() throws RecognitionException {
        try {
            int _type = GREATER_THAN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:154:18: ( '>' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:154:21: '>'
            {
            match('>'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "GREATER_THAN"

    // $ANTLR start "SLASH"
    public final void mSLASH() throws RecognitionException {
        try {
            int _type = SLASH;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:155:12: ( '/' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:155:15: '/'
            {
            match('/'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "SLASH"

    // $ANTLR start "DIV"
    public final void mDIV() throws RecognitionException {
        try {
            int _type = DIV;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:156:9: ( ' D I V' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:156:11: ' D I V'
            {
            match(" D I V"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "DIV"

    // $ANTLR start "STAR"
    public final void mSTAR() throws RecognitionException {
        try {
            int _type = STAR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:157:10: ( '*' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:157:13: '*'
            {
            match('*'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "STAR"

    // $ANTLR start "ADD"
    public final void mADD() throws RecognitionException {
        try {
            int _type = ADD;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:158:10: ( '+' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:158:13: '+'
            {
            match('+'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "ADD"

    // $ANTLR start "SUBSTRUCT"
    public final void mSUBSTRUCT() throws RecognitionException {
        try {
            int _type = SUBSTRUCT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:159:15: ( '-' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:159:17: '-'
            {
            match('-'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "SUBSTRUCT"

    // $ANTLR start "REMAINDER"
    public final void mREMAINDER() throws RecognitionException {
        try {
            int _type = REMAINDER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:160:14: ( '%' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:160:17: '%'
            {
            match('%'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "REMAINDER"

    // $ANTLR start "BITWISE_LEFT_SHIFT"
    public final void mBITWISE_LEFT_SHIFT() throws RecognitionException {
        try {
            int _type = BITWISE_LEFT_SHIFT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:161:23: ( '<<' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:161:26: '<<'
            {
            match("<<"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "BITWISE_LEFT_SHIFT"

    // $ANTLR start "BITWISE_RIGHT_SHIFT"
    public final void mBITWISE_RIGHT_SHIFT() throws RecognitionException {
        try {
            int _type = BITWISE_RIGHT_SHIFT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:162:24: ( '>>' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:162:27: '>>'
            {
            match(">>"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "BITWISE_RIGHT_SHIFT"

    // $ANTLR start "DOT"
    public final void mDOT() throws RecognitionException {
        try {
            int _type = DOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:163:10: ( '.' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:163:13: '.'
            {
            match('.'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "DOT"

    // $ANTLR start "COLON"
    public final void mCOLON() throws RecognitionException {
        try {
            int _type = COLON;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:164:12: ( ':' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:164:15: ':'
            {
            match(':'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "COLON"

    // $ANTLR start "COMMA"
    public final void mCOMMA() throws RecognitionException {
        try {
            int _type = COMMA;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:165:12: ( ',' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:165:15: ','
            {
            match(','); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "COMMA"

    // $ANTLR start "SEMICOLON"
    public final void mSEMICOLON() throws RecognitionException {
        try {
            int _type = SEMICOLON;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:166:14: ( ';' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:166:17: ';'
            {
            match(';'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "SEMICOLON"

    // $ANTLR start "LPAREN"
    public final void mLPAREN() throws RecognitionException {
        try {
            int _type = LPAREN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:167:13: ( '(' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:167:16: '('
            {
            match('('); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "LPAREN"

    // $ANTLR start "RPAREN"
    public final void mRPAREN() throws RecognitionException {
        try {
            int _type = RPAREN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:168:13: ( ')' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:168:16: ')'
            {
            match(')'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "RPAREN"

    // $ANTLR start "LSQUARE"
    public final void mLSQUARE() throws RecognitionException {
        try {
            int _type = LSQUARE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:169:13: ( '[' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:169:16: '['
            {
            match('['); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "LSQUARE"

    // $ANTLR start "RSQUARE"
    public final void mRSQUARE() throws RecognitionException {
        try {
            int _type = RSQUARE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:170:13: ( ']' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:170:16: ']'
            {
            match(']'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "RSQUARE"

    // $ANTLR start "LCURLY"
    public final void mLCURLY() throws RecognitionException {
        try {
            int _type = LCURLY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:171:13: ( '{' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:171:16: '{'
            {
            match('{'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "LCURLY"

    // $ANTLR start "RCURLY"
    public final void mRCURLY() throws RecognitionException {
        try {
            int _type = RCURLY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:172:13: ( '}' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:172:16: '}'
            {
            match('}'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "RCURLY"

    // $ANTLR start "ID"
    public final void mID() throws RecognitionException {
        try {
            int _type = ID;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:175:8: ( F_ID1 | F_ID2 )
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( ((LA3_0>='A' && LA3_0<='Z')||LA3_0=='_'||(LA3_0>='a' && LA3_0<='z')) ) {
                alt3=1;
            }
            else if ( (LA3_0=='[') ) {
                alt3=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 3, 0, input);

                throw nvae;
            }
            switch (alt3) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:175:10: F_ID1
                    {
                    mF_ID1(); 

                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:175:18: F_ID2
                    {
                    mF_ID2(); 

                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "ID"

    // $ANTLR start "F_ID1"
    public final void mF_ID1() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:176:20: ( ( 'a' .. 'z' | 'A' .. 'Z' | '_' ) ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' )* )
            // C:\\work5\\src\\dremel\\common\\Bql.g:176:22: ( 'a' .. 'z' | 'A' .. 'Z' | '_' ) ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' )*
            {
            if ( (input.LA(1)>='A' && input.LA(1)<='Z')||input.LA(1)=='_'||(input.LA(1)>='a' && input.LA(1)<='z') ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}

            // C:\\work5\\src\\dremel\\common\\Bql.g:176:46: ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' )*
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( ((LA4_0>='0' && LA4_0<='9')||(LA4_0>='A' && LA4_0<='Z')||LA4_0=='_'||(LA4_0>='a' && LA4_0<='z')) ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:
            	    {
            	    if ( (input.LA(1)>='0' && input.LA(1)<='9')||(input.LA(1)>='A' && input.LA(1)<='Z')||input.LA(1)=='_'||(input.LA(1)>='a' && input.LA(1)<='z') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    break loop4;
                }
            } while (true);


            }

        }
        finally {
        }
    }
    // $ANTLR end "F_ID1"

    // $ANTLR start "F_ID2"
    public final void mF_ID2() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:177:19: ( '[' ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' | ' ' | '\\\\' | '/' | '-' | '+' | '*' | '.' | ':' | '$' )* ']' )
            // C:\\work5\\src\\dremel\\common\\Bql.g:177:22: '[' ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' | ' ' | '\\\\' | '/' | '-' | '+' | '*' | '.' | ':' | '$' )* ']'
            {
            match('['); 
            // C:\\work5\\src\\dremel\\common\\Bql.g:177:26: ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' | ' ' | '\\\\' | '/' | '-' | '+' | '*' | '.' | ':' | '$' )*
            loop5:
            do {
                int alt5=2;
                int LA5_0 = input.LA(1);

                if ( (LA5_0==' '||LA5_0=='$'||(LA5_0>='*' && LA5_0<='+')||(LA5_0>='-' && LA5_0<=':')||(LA5_0>='A' && LA5_0<='Z')||LA5_0=='\\'||LA5_0=='_'||(LA5_0>='a' && LA5_0<='z')) ) {
                    alt5=1;
                }


                switch (alt5) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:
            	    {
            	    if ( input.LA(1)==' '||input.LA(1)=='$'||(input.LA(1)>='*' && input.LA(1)<='+')||(input.LA(1)>='-' && input.LA(1)<=':')||(input.LA(1)>='A' && input.LA(1)<='Z')||input.LA(1)=='\\'||input.LA(1)=='_'||(input.LA(1)>='a' && input.LA(1)<='z') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    break loop5;
                }
            } while (true);

            match(']'); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "F_ID2"

    // $ANTLR start "INT"
    public final void mINT() throws RecognitionException {
        try {
            int _type = INT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:178:10: ( ( '0' .. '9' )+ )
            // C:\\work5\\src\\dremel\\common\\Bql.g:178:12: ( '0' .. '9' )+
            {
            // C:\\work5\\src\\dremel\\common\\Bql.g:178:12: ( '0' .. '9' )+
            int cnt6=0;
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( ((LA6_0>='0' && LA6_0<='9')) ) {
                    alt6=1;
                }


                switch (alt6) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:178:12: '0' .. '9'
            	    {
            	    matchRange('0','9'); 

            	    }
            	    break;

            	default :
            	    if ( cnt6 >= 1 ) break loop6;
                        EarlyExitException eee =
                            new EarlyExitException(6, input);
                        throw eee;
                }
                cnt6++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "INT"

    // $ANTLR start "FLOAT"
    public final void mFLOAT() throws RecognitionException {
        try {
            int _type = FLOAT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:179:14: ( ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( F_EXPONENT )? | '.' ( '0' .. '9' )+ ( F_EXPONENT )? | ( '0' .. '9' )+ F_EXPONENT )
            int alt13=3;
            alt13 = dfa13.predict(input);
            switch (alt13) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:179:19: ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( F_EXPONENT )?
                    {
                    // C:\\work5\\src\\dremel\\common\\Bql.g:179:19: ( '0' .. '9' )+
                    int cnt7=0;
                    loop7:
                    do {
                        int alt7=2;
                        int LA7_0 = input.LA(1);

                        if ( ((LA7_0>='0' && LA7_0<='9')) ) {
                            alt7=1;
                        }


                        switch (alt7) {
                    	case 1 :
                    	    // C:\\work5\\src\\dremel\\common\\Bql.g:179:20: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt7 >= 1 ) break loop7;
                                EarlyExitException eee =
                                    new EarlyExitException(7, input);
                                throw eee;
                        }
                        cnt7++;
                    } while (true);

                    match('.'); 
                    // C:\\work5\\src\\dremel\\common\\Bql.g:179:35: ( '0' .. '9' )*
                    loop8:
                    do {
                        int alt8=2;
                        int LA8_0 = input.LA(1);

                        if ( ((LA8_0>='0' && LA8_0<='9')) ) {
                            alt8=1;
                        }


                        switch (alt8) {
                    	case 1 :
                    	    // C:\\work5\\src\\dremel\\common\\Bql.g:179:36: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    break loop8;
                        }
                    } while (true);

                    // C:\\work5\\src\\dremel\\common\\Bql.g:179:47: ( F_EXPONENT )?
                    int alt9=2;
                    int LA9_0 = input.LA(1);

                    if ( (LA9_0=='E'||LA9_0=='e') ) {
                        alt9=1;
                    }
                    switch (alt9) {
                        case 1 :
                            // C:\\work5\\src\\dremel\\common\\Bql.g:179:47: F_EXPONENT
                            {
                            mF_EXPONENT(); 

                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:179:66: '.' ( '0' .. '9' )+ ( F_EXPONENT )?
                    {
                    match('.'); 
                    // C:\\work5\\src\\dremel\\common\\Bql.g:179:70: ( '0' .. '9' )+
                    int cnt10=0;
                    loop10:
                    do {
                        int alt10=2;
                        int LA10_0 = input.LA(1);

                        if ( ((LA10_0>='0' && LA10_0<='9')) ) {
                            alt10=1;
                        }


                        switch (alt10) {
                    	case 1 :
                    	    // C:\\work5\\src\\dremel\\common\\Bql.g:179:71: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt10 >= 1 ) break loop10;
                                EarlyExitException eee =
                                    new EarlyExitException(10, input);
                                throw eee;
                        }
                        cnt10++;
                    } while (true);

                    // C:\\work5\\src\\dremel\\common\\Bql.g:179:82: ( F_EXPONENT )?
                    int alt11=2;
                    int LA11_0 = input.LA(1);

                    if ( (LA11_0=='E'||LA11_0=='e') ) {
                        alt11=1;
                    }
                    switch (alt11) {
                        case 1 :
                            // C:\\work5\\src\\dremel\\common\\Bql.g:179:82: F_EXPONENT
                            {
                            mF_EXPONENT(); 

                            }
                            break;

                    }


                    }
                    break;
                case 3 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:179:101: ( '0' .. '9' )+ F_EXPONENT
                    {
                    // C:\\work5\\src\\dremel\\common\\Bql.g:179:101: ( '0' .. '9' )+
                    int cnt12=0;
                    loop12:
                    do {
                        int alt12=2;
                        int LA12_0 = input.LA(1);

                        if ( ((LA12_0>='0' && LA12_0<='9')) ) {
                            alt12=1;
                        }


                        switch (alt12) {
                    	case 1 :
                    	    // C:\\work5\\src\\dremel\\common\\Bql.g:179:102: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt12 >= 1 ) break loop12;
                                EarlyExitException eee =
                                    new EarlyExitException(12, input);
                                throw eee;
                        }
                        cnt12++;
                    } while (true);

                    mF_EXPONENT(); 

                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "FLOAT"

    // $ANTLR start "STRING"
    public final void mSTRING() throws RecognitionException {
        try {
            int _type = STRING;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:180:16: ( ( '\"' (~ ( '\"' ) )* '\"' ) | ( '\\'' (~ ( '\\'' ) )* '\\'' ) )
            int alt16=2;
            int LA16_0 = input.LA(1);

            if ( (LA16_0=='\"') ) {
                alt16=1;
            }
            else if ( (LA16_0=='\'') ) {
                alt16=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 16, 0, input);

                throw nvae;
            }
            switch (alt16) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:180:18: ( '\"' (~ ( '\"' ) )* '\"' )
                    {
                    // C:\\work5\\src\\dremel\\common\\Bql.g:180:18: ( '\"' (~ ( '\"' ) )* '\"' )
                    // C:\\work5\\src\\dremel\\common\\Bql.g:180:19: '\"' (~ ( '\"' ) )* '\"'
                    {
                    match('\"'); 
                    // C:\\work5\\src\\dremel\\common\\Bql.g:180:23: (~ ( '\"' ) )*
                    loop14:
                    do {
                        int alt14=2;
                        int LA14_0 = input.LA(1);

                        if ( ((LA14_0>='\u0000' && LA14_0<='!')||(LA14_0>='#' && LA14_0<='\uFFFF')) ) {
                            alt14=1;
                        }


                        switch (alt14) {
                    	case 1 :
                    	    // C:\\work5\\src\\dremel\\common\\Bql.g:180:25: ~ ( '\"' )
                    	    {
                    	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='!')||(input.LA(1)>='#' && input.LA(1)<='\uFFFF') ) {
                    	        input.consume();

                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;}


                    	    }
                    	    break;

                    	default :
                    	    break loop14;
                        }
                    } while (true);

                    match('\"'); 

                    }


                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:180:42: ( '\\'' (~ ( '\\'' ) )* '\\'' )
                    {
                    // C:\\work5\\src\\dremel\\common\\Bql.g:180:42: ( '\\'' (~ ( '\\'' ) )* '\\'' )
                    // C:\\work5\\src\\dremel\\common\\Bql.g:180:43: '\\'' (~ ( '\\'' ) )* '\\''
                    {
                    match('\''); 
                    // C:\\work5\\src\\dremel\\common\\Bql.g:180:48: (~ ( '\\'' ) )*
                    loop15:
                    do {
                        int alt15=2;
                        int LA15_0 = input.LA(1);

                        if ( ((LA15_0>='\u0000' && LA15_0<='&')||(LA15_0>='(' && LA15_0<='\uFFFF')) ) {
                            alt15=1;
                        }


                        switch (alt15) {
                    	case 1 :
                    	    // C:\\work5\\src\\dremel\\common\\Bql.g:180:50: ~ ( '\\'' )
                    	    {
                    	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='&')||(input.LA(1)>='(' && input.LA(1)<='\uFFFF') ) {
                    	        input.consume();

                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;}


                    	    }
                    	    break;

                    	default :
                    	    break loop15;
                        }
                    } while (true);

                    match('\''); 

                    }


                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "STRING"

    // $ANTLR start "F_EXPONENT"
    public final void mF_EXPONENT() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:181:24: ( ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+ )
            // C:\\work5\\src\\dremel\\common\\Bql.g:181:27: ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+
            {
            if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}

            // C:\\work5\\src\\dremel\\common\\Bql.g:181:37: ( '+' | '-' )?
            int alt17=2;
            int LA17_0 = input.LA(1);

            if ( (LA17_0=='+'||LA17_0=='-') ) {
                alt17=1;
            }
            switch (alt17) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:
                    {
                    if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                        input.consume();

                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;}


                    }
                    break;

            }

            // C:\\work5\\src\\dremel\\common\\Bql.g:181:48: ( '0' .. '9' )+
            int cnt18=0;
            loop18:
            do {
                int alt18=2;
                int LA18_0 = input.LA(1);

                if ( ((LA18_0>='0' && LA18_0<='9')) ) {
                    alt18=1;
                }


                switch (alt18) {
            	case 1 :
            	    // C:\\work5\\src\\dremel\\common\\Bql.g:181:49: '0' .. '9'
            	    {
            	    matchRange('0','9'); 

            	    }
            	    break;

            	default :
            	    if ( cnt18 >= 1 ) break loop18;
                        EarlyExitException eee =
                            new EarlyExitException(18, input);
                        throw eee;
                }
                cnt18++;
            } while (true);


            }

        }
        finally {
        }
    }
    // $ANTLR end "F_EXPONENT"

    // $ANTLR start "F_HEX_DIGIT"
    public final void mF_HEX_DIGIT() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:182:25: ( ( '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:182:28: ( '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' )
            {
            if ( (input.LA(1)>='0' && input.LA(1)<='9')||(input.LA(1)>='A' && input.LA(1)<='F')||(input.LA(1)>='a' && input.LA(1)<='f') ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "F_HEX_DIGIT"

    // $ANTLR start "WS"
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:185:10: ( ( ' ' | '\\t' | '\\r' | '\\n' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:185:15: ( ' ' | '\\t' | '\\r' | '\\n' )
            {
            if ( (input.LA(1)>='\t' && input.LA(1)<='\n')||input.LA(1)=='\r'||input.LA(1)==' ' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}

            _channel=HIDDEN;

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "WS"

    // $ANTLR start "COMMENT"
    public final void mCOMMENT() throws RecognitionException {
        try {
            int _type = COMMENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // C:\\work5\\src\\dremel\\common\\Bql.g:188:13: ( '//' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n' | '/*' ( options {greedy=false; } : . )* '*/' )
            int alt22=2;
            int LA22_0 = input.LA(1);

            if ( (LA22_0=='/') ) {
                int LA22_1 = input.LA(2);

                if ( (LA22_1=='/') ) {
                    alt22=1;
                }
                else if ( (LA22_1=='*') ) {
                    alt22=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 22, 1, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 22, 0, input);

                throw nvae;
            }
            switch (alt22) {
                case 1 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:188:18: '//' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n'
                    {
                    match("//"); 

                    // C:\\work5\\src\\dremel\\common\\Bql.g:188:23: (~ ( '\\n' | '\\r' ) )*
                    loop19:
                    do {
                        int alt19=2;
                        int LA19_0 = input.LA(1);

                        if ( ((LA19_0>='\u0000' && LA19_0<='\t')||(LA19_0>='\u000B' && LA19_0<='\f')||(LA19_0>='\u000E' && LA19_0<='\uFFFF')) ) {
                            alt19=1;
                        }


                        switch (alt19) {
                    	case 1 :
                    	    // C:\\work5\\src\\dremel\\common\\Bql.g:188:23: ~ ( '\\n' | '\\r' )
                    	    {
                    	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='\t')||(input.LA(1)>='\u000B' && input.LA(1)<='\f')||(input.LA(1)>='\u000E' && input.LA(1)<='\uFFFF') ) {
                    	        input.consume();

                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;}


                    	    }
                    	    break;

                    	default :
                    	    break loop19;
                        }
                    } while (true);

                    // C:\\work5\\src\\dremel\\common\\Bql.g:188:37: ( '\\r' )?
                    int alt20=2;
                    int LA20_0 = input.LA(1);

                    if ( (LA20_0=='\r') ) {
                        alt20=1;
                    }
                    switch (alt20) {
                        case 1 :
                            // C:\\work5\\src\\dremel\\common\\Bql.g:188:37: '\\r'
                            {
                            match('\r'); 

                            }
                            break;

                    }

                    match('\n'); 
                    _channel=HIDDEN;

                    }
                    break;
                case 2 :
                    // C:\\work5\\src\\dremel\\common\\Bql.g:188:69: '/*' ( options {greedy=false; } : . )* '*/'
                    {
                    match("/*"); 

                    // C:\\work5\\src\\dremel\\common\\Bql.g:188:74: ( options {greedy=false; } : . )*
                    loop21:
                    do {
                        int alt21=2;
                        int LA21_0 = input.LA(1);

                        if ( (LA21_0=='*') ) {
                            int LA21_1 = input.LA(2);

                            if ( (LA21_1=='/') ) {
                                alt21=2;
                            }
                            else if ( ((LA21_1>='\u0000' && LA21_1<='.')||(LA21_1>='0' && LA21_1<='\uFFFF')) ) {
                                alt21=1;
                            }


                        }
                        else if ( ((LA21_0>='\u0000' && LA21_0<=')')||(LA21_0>='+' && LA21_0<='\uFFFF')) ) {
                            alt21=1;
                        }


                        switch (alt21) {
                    	case 1 :
                    	    // C:\\work5\\src\\dremel\\common\\Bql.g:188:102: .
                    	    {
                    	    matchAny(); 

                    	    }
                    	    break;

                    	default :
                    	    break loop21;
                        }
                    } while (true);

                    match("*/"); 

                    _channel=HIDDEN;

                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "COMMENT"

    // $ANTLR start "A"
    public final void mA() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:191:11: ( ( 'a' | 'A' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:191:12: ( 'a' | 'A' )
            {
            if ( input.LA(1)=='A'||input.LA(1)=='a' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "A"

    // $ANTLR start "B"
    public final void mB() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:192:11: ( ( 'b' | 'B' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:192:12: ( 'b' | 'B' )
            {
            if ( input.LA(1)=='B'||input.LA(1)=='b' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "B"

    // $ANTLR start "C"
    public final void mC() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:193:11: ( ( 'c' | 'C' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:193:12: ( 'c' | 'C' )
            {
            if ( input.LA(1)=='C'||input.LA(1)=='c' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "C"

    // $ANTLR start "D"
    public final void mD() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:194:11: ( ( 'd' | 'D' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:194:12: ( 'd' | 'D' )
            {
            if ( input.LA(1)=='D'||input.LA(1)=='d' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "D"

    // $ANTLR start "E"
    public final void mE() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:195:11: ( ( 'e' | 'E' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:195:12: ( 'e' | 'E' )
            {
            if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "E"

    // $ANTLR start "F"
    public final void mF() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:196:11: ( ( 'f' | 'F' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:196:12: ( 'f' | 'F' )
            {
            if ( input.LA(1)=='F'||input.LA(1)=='f' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "F"

    // $ANTLR start "G"
    public final void mG() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:197:11: ( ( 'g' | 'G' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:197:12: ( 'g' | 'G' )
            {
            if ( input.LA(1)=='G'||input.LA(1)=='g' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "G"

    // $ANTLR start "H"
    public final void mH() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:198:11: ( ( 'h' | 'H' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:198:12: ( 'h' | 'H' )
            {
            if ( input.LA(1)=='H'||input.LA(1)=='h' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "H"

    // $ANTLR start "I"
    public final void mI() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:199:11: ( ( 'i' | 'I' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:199:12: ( 'i' | 'I' )
            {
            if ( input.LA(1)=='I'||input.LA(1)=='i' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "I"

    // $ANTLR start "J"
    public final void mJ() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:200:11: ( ( 'j' | 'J' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:200:12: ( 'j' | 'J' )
            {
            if ( input.LA(1)=='J'||input.LA(1)=='j' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "J"

    // $ANTLR start "K"
    public final void mK() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:201:11: ( ( 'k' | 'K' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:201:12: ( 'k' | 'K' )
            {
            if ( input.LA(1)=='K'||input.LA(1)=='k' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "K"

    // $ANTLR start "L"
    public final void mL() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:202:11: ( ( 'l' | 'L' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:202:12: ( 'l' | 'L' )
            {
            if ( input.LA(1)=='L'||input.LA(1)=='l' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "L"

    // $ANTLR start "M"
    public final void mM() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:203:11: ( ( 'm' | 'M' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:203:12: ( 'm' | 'M' )
            {
            if ( input.LA(1)=='M'||input.LA(1)=='m' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "M"

    // $ANTLR start "N"
    public final void mN() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:204:11: ( ( 'n' | 'N' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:204:12: ( 'n' | 'N' )
            {
            if ( input.LA(1)=='N'||input.LA(1)=='n' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "N"

    // $ANTLR start "O"
    public final void mO() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:205:11: ( ( 'o' | 'O' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:205:12: ( 'o' | 'O' )
            {
            if ( input.LA(1)=='O'||input.LA(1)=='o' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "O"

    // $ANTLR start "P"
    public final void mP() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:206:11: ( ( 'p' | 'P' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:206:12: ( 'p' | 'P' )
            {
            if ( input.LA(1)=='P'||input.LA(1)=='p' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "P"

    // $ANTLR start "Q"
    public final void mQ() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:207:11: ( ( 'q' | 'Q' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:207:12: ( 'q' | 'Q' )
            {
            if ( input.LA(1)=='Q'||input.LA(1)=='q' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "Q"

    // $ANTLR start "R"
    public final void mR() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:208:11: ( ( 'r' | 'R' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:208:12: ( 'r' | 'R' )
            {
            if ( input.LA(1)=='R'||input.LA(1)=='r' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "R"

    // $ANTLR start "S"
    public final void mS() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:209:11: ( ( 's' | 'S' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:209:12: ( 's' | 'S' )
            {
            if ( input.LA(1)=='S'||input.LA(1)=='s' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "S"

    // $ANTLR start "T"
    public final void mT() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:210:11: ( ( 't' | 'T' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:210:12: ( 't' | 'T' )
            {
            if ( input.LA(1)=='T'||input.LA(1)=='t' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "T"

    // $ANTLR start "U"
    public final void mU() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:211:11: ( ( 'u' | 'U' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:211:12: ( 'u' | 'U' )
            {
            if ( input.LA(1)=='U'||input.LA(1)=='u' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "U"

    // $ANTLR start "V"
    public final void mV() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:212:11: ( ( 'v' | 'V' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:212:12: ( 'v' | 'V' )
            {
            if ( input.LA(1)=='V'||input.LA(1)=='v' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "V"

    // $ANTLR start "W"
    public final void mW() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:213:11: ( ( 'w' | 'W' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:213:12: ( 'w' | 'W' )
            {
            if ( input.LA(1)=='W'||input.LA(1)=='w' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "W"

    // $ANTLR start "X"
    public final void mX() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:214:11: ( ( 'x' | 'X' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:214:12: ( 'x' | 'X' )
            {
            if ( input.LA(1)=='X'||input.LA(1)=='x' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "X"

    // $ANTLR start "Y"
    public final void mY() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:215:11: ( ( 'y' | 'Y' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:215:12: ( 'y' | 'Y' )
            {
            if ( input.LA(1)=='Y'||input.LA(1)=='y' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "Y"

    // $ANTLR start "Z"
    public final void mZ() throws RecognitionException {
        try {
            // C:\\work5\\src\\dremel\\common\\Bql.g:216:11: ( ( 'z' | 'Z' ) )
            // C:\\work5\\src\\dremel\\common\\Bql.g:216:12: ( 'z' | 'Z' )
            {
            if ( input.LA(1)=='Z'||input.LA(1)=='z' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "Z"

    public void mTokens() throws RecognitionException {
        // C:\\work5\\src\\dremel\\common\\Bql.g:1:8: ( SELECT | WITHIN | RECORD | AS | FROM | WHERE | GROUPBY | ORDERBY | DESC | ASC | LIMIT | LOGICAL_OR | LOGICAL_AND | LOGICAL_NOT | CONTAINS | IN | BITWISE_AND | BITWISE_NOT | BITWISE_OR | BITWISE_XOR | EQUAL | NOT_EQUAL | LESS_THAN_OR_EQUAL | LESS_THAN | GREATER_THAN_OR_EQUAL | GREATER_THAN | SLASH | DIV | STAR | ADD | SUBSTRUCT | REMAINDER | BITWISE_LEFT_SHIFT | BITWISE_RIGHT_SHIFT | DOT | COLON | COMMA | SEMICOLON | LPAREN | RPAREN | LSQUARE | RSQUARE | LCURLY | RCURLY | ID | INT | FLOAT | STRING | WS | COMMENT )
        int alt23=50;
        alt23 = dfa23.predict(input);
        switch (alt23) {
            case 1 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:10: SELECT
                {
                mSELECT(); 

                }
                break;
            case 2 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:17: WITHIN
                {
                mWITHIN(); 

                }
                break;
            case 3 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:24: RECORD
                {
                mRECORD(); 

                }
                break;
            case 4 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:31: AS
                {
                mAS(); 

                }
                break;
            case 5 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:34: FROM
                {
                mFROM(); 

                }
                break;
            case 6 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:39: WHERE
                {
                mWHERE(); 

                }
                break;
            case 7 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:45: GROUPBY
                {
                mGROUPBY(); 

                }
                break;
            case 8 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:53: ORDERBY
                {
                mORDERBY(); 

                }
                break;
            case 9 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:61: DESC
                {
                mDESC(); 

                }
                break;
            case 10 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:66: ASC
                {
                mASC(); 

                }
                break;
            case 11 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:70: LIMIT
                {
                mLIMIT(); 

                }
                break;
            case 12 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:76: LOGICAL_OR
                {
                mLOGICAL_OR(); 

                }
                break;
            case 13 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:87: LOGICAL_AND
                {
                mLOGICAL_AND(); 

                }
                break;
            case 14 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:99: LOGICAL_NOT
                {
                mLOGICAL_NOT(); 

                }
                break;
            case 15 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:111: CONTAINS
                {
                mCONTAINS(); 

                }
                break;
            case 16 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:120: IN
                {
                mIN(); 

                }
                break;
            case 17 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:123: BITWISE_AND
                {
                mBITWISE_AND(); 

                }
                break;
            case 18 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:135: BITWISE_NOT
                {
                mBITWISE_NOT(); 

                }
                break;
            case 19 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:147: BITWISE_OR
                {
                mBITWISE_OR(); 

                }
                break;
            case 20 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:158: BITWISE_XOR
                {
                mBITWISE_XOR(); 

                }
                break;
            case 21 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:170: EQUAL
                {
                mEQUAL(); 

                }
                break;
            case 22 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:176: NOT_EQUAL
                {
                mNOT_EQUAL(); 

                }
                break;
            case 23 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:186: LESS_THAN_OR_EQUAL
                {
                mLESS_THAN_OR_EQUAL(); 

                }
                break;
            case 24 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:205: LESS_THAN
                {
                mLESS_THAN(); 

                }
                break;
            case 25 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:215: GREATER_THAN_OR_EQUAL
                {
                mGREATER_THAN_OR_EQUAL(); 

                }
                break;
            case 26 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:237: GREATER_THAN
                {
                mGREATER_THAN(); 

                }
                break;
            case 27 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:250: SLASH
                {
                mSLASH(); 

                }
                break;
            case 28 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:256: DIV
                {
                mDIV(); 

                }
                break;
            case 29 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:260: STAR
                {
                mSTAR(); 

                }
                break;
            case 30 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:265: ADD
                {
                mADD(); 

                }
                break;
            case 31 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:269: SUBSTRUCT
                {
                mSUBSTRUCT(); 

                }
                break;
            case 32 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:279: REMAINDER
                {
                mREMAINDER(); 

                }
                break;
            case 33 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:289: BITWISE_LEFT_SHIFT
                {
                mBITWISE_LEFT_SHIFT(); 

                }
                break;
            case 34 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:308: BITWISE_RIGHT_SHIFT
                {
                mBITWISE_RIGHT_SHIFT(); 

                }
                break;
            case 35 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:328: DOT
                {
                mDOT(); 

                }
                break;
            case 36 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:332: COLON
                {
                mCOLON(); 

                }
                break;
            case 37 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:338: COMMA
                {
                mCOMMA(); 

                }
                break;
            case 38 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:344: SEMICOLON
                {
                mSEMICOLON(); 

                }
                break;
            case 39 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:354: LPAREN
                {
                mLPAREN(); 

                }
                break;
            case 40 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:361: RPAREN
                {
                mRPAREN(); 

                }
                break;
            case 41 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:368: LSQUARE
                {
                mLSQUARE(); 

                }
                break;
            case 42 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:376: RSQUARE
                {
                mRSQUARE(); 

                }
                break;
            case 43 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:384: LCURLY
                {
                mLCURLY(); 

                }
                break;
            case 44 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:391: RCURLY
                {
                mRCURLY(); 

                }
                break;
            case 45 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:398: ID
                {
                mID(); 

                }
                break;
            case 46 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:401: INT
                {
                mINT(); 

                }
                break;
            case 47 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:405: FLOAT
                {
                mFLOAT(); 

                }
                break;
            case 48 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:411: STRING
                {
                mSTRING(); 

                }
                break;
            case 49 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:418: WS
                {
                mWS(); 

                }
                break;
            case 50 :
                // C:\\work5\\src\\dremel\\common\\Bql.g:1:421: COMMENT
                {
                mCOMMENT(); 

                }
                break;

        }

    }


    protected DFA13 dfa13 = new DFA13(this);
    protected DFA23 dfa23 = new DFA23(this);
    static final String DFA13_eotS =
        "\5\uffff";
    static final String DFA13_eofS =
        "\5\uffff";
    static final String DFA13_minS =
        "\2\56\3\uffff";
    static final String DFA13_maxS =
        "\1\71\1\145\3\uffff";
    static final String DFA13_acceptS =
        "\2\uffff\1\2\1\1\1\3";
    static final String DFA13_specialS =
        "\5\uffff}>";
    static final String[] DFA13_transitionS = {
            "\1\2\1\uffff\12\1",
            "\1\3\1\uffff\12\1\13\uffff\1\4\37\uffff\1\4",
            "",
            "",
            ""
    };

    static final short[] DFA13_eot = DFA.unpackEncodedString(DFA13_eotS);
    static final short[] DFA13_eof = DFA.unpackEncodedString(DFA13_eofS);
    static final char[] DFA13_min = DFA.unpackEncodedStringToUnsignedChars(DFA13_minS);
    static final char[] DFA13_max = DFA.unpackEncodedStringToUnsignedChars(DFA13_maxS);
    static final short[] DFA13_accept = DFA.unpackEncodedString(DFA13_acceptS);
    static final short[] DFA13_special = DFA.unpackEncodedString(DFA13_specialS);
    static final short[][] DFA13_transition;

    static {
        int numStates = DFA13_transitionS.length;
        DFA13_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA13_transition[i] = DFA.unpackEncodedString(DFA13_transitionS[i]);
        }
    }

    class DFA13 extends DFA {

        public DFA13(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 13;
            this.eot = DFA13_eot;
            this.eof = DFA13_eof;
            this.min = DFA13_min;
            this.max = DFA13_max;
            this.accept = DFA13_accept;
            this.special = DFA13_special;
            this.transition = DFA13_transition;
        }
        public String getDescription() {
            return "179:1: FLOAT : ( ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( F_EXPONENT )? | '.' ( '0' .. '9' )+ ( F_EXPONENT )? | ( '0' .. '9' )+ F_EXPONENT );";
        }
    }
    static final String DFA23_eotS =
        "\1\uffff\14\45\5\uffff\1\71\1\uffff\1\74\1\76\1\50\4\uffff\1\101"+
        "\5\uffff\1\102\4\uffff\1\103\2\uffff\4\45\1\110\3\45\1\115\4\45"+
        "\1\123\15\uffff\4\45\1\uffff\1\130\1\131\2\45\1\uffff\3\45\1\137"+
        "\1\45\1\uffff\4\45\2\uffff\1\145\2\45\1\150\1\45\1\uffff\3\45\1"+
        "\155\1\45\1\uffff\2\45\1\uffff\1\161\1\45\1\163\1\164\1\uffff\1"+
        "\165\3\uffff\1\45\3\uffff\1\45\1\170\1\uffff";
    static final String DFA23_eofS =
        "\171\uffff";
    static final String DFA23_minS =
        "\1\11\1\105\1\110\1\105\1\116\3\122\1\105\1\111\2\117\1\116\5\uffff"+
        "\1\74\1\uffff\1\75\1\52\1\104\4\uffff\1\60\5\uffff\1\40\4\uffff"+
        "\1\56\2\uffff\1\114\1\124\1\105\1\103\1\60\1\104\2\117\1\60\1\123"+
        "\1\115\1\124\1\116\1\60\15\uffff\1\105\1\110\1\122\1\117\1\uffff"+
        "\2\60\1\115\1\125\1\uffff\1\105\1\103\1\111\1\60\1\124\1\uffff\1"+
        "\103\1\111\1\105\1\122\2\uffff\1\60\1\120\1\122\1\60\1\124\1\uffff"+
        "\1\101\1\124\1\116\1\60\1\104\1\uffff\2\11\1\uffff\1\60\1\111\2"+
        "\60\1\uffff\1\60\3\uffff\1\116\3\uffff\1\123\1\60\1\uffff";
    static final String DFA23_maxS =
        "\1\176\1\145\1\151\1\145\1\163\3\162\1\145\1\151\2\157\1\156\5"+
        "\uffff\1\76\1\uffff\1\76\1\57\1\104\4\uffff\1\71\5\uffff\1\172\4"+
        "\uffff\1\145\2\uffff\1\154\1\164\1\145\1\143\1\172\1\144\2\157\1"+
        "\172\1\163\1\155\1\164\1\156\1\172\15\uffff\1\145\1\150\1\162\1"+
        "\157\1\uffff\2\172\1\155\1\165\1\uffff\1\145\1\143\1\151\1\172\1"+
        "\164\1\uffff\1\143\1\151\1\145\1\162\2\uffff\1\172\1\160\1\162\1"+
        "\172\1\164\1\uffff\1\141\1\164\1\156\1\172\1\144\1\uffff\2\40\1"+
        "\uffff\1\172\1\151\2\172\1\uffff\1\172\3\uffff\1\156\3\uffff\1\163"+
        "\1\172\1\uffff";
    static final String DFA23_acceptS =
        "\15\uffff\1\21\1\22\1\23\1\24\1\25\1\uffff\1\26\3\uffff\1\35\1"+
        "\36\1\37\1\40\1\uffff\1\44\1\45\1\46\1\47\1\50\1\uffff\1\52\1\53"+
        "\1\54\1\55\1\uffff\1\60\1\61\16\uffff\1\27\1\41\1\30\1\31\1\42\1"+
        "\32\1\62\1\33\1\34\1\57\1\43\1\51\1\56\4\uffff\1\4\4\uffff\1\14"+
        "\5\uffff\1\20\4\uffff\1\12\1\15\5\uffff\1\16\5\uffff\1\5\2\uffff"+
        "\1\11\4\uffff\1\6\1\uffff\1\7\1\10\1\13\1\uffff\1\1\1\2\1\3\2\uffff"+
        "\1\17";
    static final String DFA23_specialS =
        "\171\uffff}>";
    static final String[] DFA23_transitionS = {
            "\2\50\2\uffff\1\50\22\uffff\1\26\1\23\1\47\2\uffff\1\32\1\15"+
            "\1\47\1\37\1\40\1\27\1\30\1\35\1\31\1\33\1\25\12\46\1\34\1\36"+
            "\1\22\1\21\1\24\2\uffff\1\4\1\45\1\13\1\10\1\45\1\5\1\6\1\45"+
            "\1\14\2\45\1\11\1\45\1\12\1\7\2\45\1\3\1\1\3\45\1\2\3\45\1\41"+
            "\1\uffff\1\42\1\20\1\45\1\uffff\1\4\1\45\1\13\1\10\1\45\1\5"+
            "\1\6\1\45\1\14\2\45\1\11\1\45\1\12\1\7\2\45\1\3\1\1\3\45\1\2"+
            "\3\45\1\43\1\17\1\44\1\16",
            "\1\51\37\uffff\1\51",
            "\1\53\1\52\36\uffff\1\53\1\52",
            "\1\54\37\uffff\1\54",
            "\1\56\4\uffff\1\55\32\uffff\1\56\4\uffff\1\55",
            "\1\57\37\uffff\1\57",
            "\1\60\37\uffff\1\60",
            "\1\61\37\uffff\1\61",
            "\1\62\37\uffff\1\62",
            "\1\63\37\uffff\1\63",
            "\1\64\37\uffff\1\64",
            "\1\65\37\uffff\1\65",
            "\1\66\37\uffff\1\66",
            "",
            "",
            "",
            "",
            "",
            "\1\70\1\67\1\23",
            "",
            "\1\72\1\73",
            "\1\75\4\uffff\1\75",
            "\1\77",
            "",
            "",
            "",
            "",
            "\12\100",
            "",
            "",
            "",
            "",
            "",
            "\1\45\3\uffff\1\45\5\uffff\2\45\1\uffff\16\45\6\uffff\32\45"+
            "\1\uffff\2\45\1\uffff\1\45\1\uffff\32\45",
            "",
            "",
            "",
            "",
            "\1\100\1\uffff\12\46\13\uffff\1\100\37\uffff\1\100",
            "",
            "",
            "\1\104\37\uffff\1\104",
            "\1\105\37\uffff\1\105",
            "\1\106\37\uffff\1\106",
            "\1\107\37\uffff\1\107",
            "\12\45\7\uffff\2\45\1\111\27\45\4\uffff\1\45\1\uffff\2\45"+
            "\1\111\27\45",
            "\1\112\37\uffff\1\112",
            "\1\113\37\uffff\1\113",
            "\1\114\37\uffff\1\114",
            "\12\45\7\uffff\3\45\1\116\26\45\4\uffff\1\45\1\uffff\3\45"+
            "\1\116\26\45",
            "\1\117\37\uffff\1\117",
            "\1\120\37\uffff\1\120",
            "\1\121\37\uffff\1\121",
            "\1\122\37\uffff\1\122",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\1\124\37\uffff\1\124",
            "\1\125\37\uffff\1\125",
            "\1\126\37\uffff\1\126",
            "\1\127\37\uffff\1\127",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\132\37\uffff\1\132",
            "\1\133\37\uffff\1\133",
            "",
            "\1\134\37\uffff\1\134",
            "\1\135\37\uffff\1\135",
            "\1\136\37\uffff\1\136",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\140\37\uffff\1\140",
            "",
            "\1\141\37\uffff\1\141",
            "\1\142\37\uffff\1\142",
            "\1\143\37\uffff\1\143",
            "\1\144\37\uffff\1\144",
            "",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\146\37\uffff\1\146",
            "\1\147\37\uffff\1\147",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\151\37\uffff\1\151",
            "",
            "\1\152\37\uffff\1\152",
            "\1\153\37\uffff\1\153",
            "\1\154\37\uffff\1\154",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\156\37\uffff\1\156",
            "",
            "\2\157\2\uffff\1\157\22\uffff\1\157",
            "\2\160\2\uffff\1\160\22\uffff\1\160",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\1\162\37\uffff\1\162",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            "",
            "",
            "",
            "\1\166\37\uffff\1\166",
            "",
            "",
            "",
            "\1\167\37\uffff\1\167",
            "\12\45\7\uffff\32\45\4\uffff\1\45\1\uffff\32\45",
            ""
    };

    static final short[] DFA23_eot = DFA.unpackEncodedString(DFA23_eotS);
    static final short[] DFA23_eof = DFA.unpackEncodedString(DFA23_eofS);
    static final char[] DFA23_min = DFA.unpackEncodedStringToUnsignedChars(DFA23_minS);
    static final char[] DFA23_max = DFA.unpackEncodedStringToUnsignedChars(DFA23_maxS);
    static final short[] DFA23_accept = DFA.unpackEncodedString(DFA23_acceptS);
    static final short[] DFA23_special = DFA.unpackEncodedString(DFA23_specialS);
    static final short[][] DFA23_transition;

    static {
        int numStates = DFA23_transitionS.length;
        DFA23_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA23_transition[i] = DFA.unpackEncodedString(DFA23_transitionS[i]);
        }
    }

    class DFA23 extends DFA {

        public DFA23(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 23;
            this.eot = DFA23_eot;
            this.eof = DFA23_eof;
            this.min = DFA23_min;
            this.max = DFA23_max;
            this.accept = DFA23_accept;
            this.special = DFA23_special;
            this.transition = DFA23_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( SELECT | WITHIN | RECORD | AS | FROM | WHERE | GROUPBY | ORDERBY | DESC | ASC | LIMIT | LOGICAL_OR | LOGICAL_AND | LOGICAL_NOT | CONTAINS | IN | BITWISE_AND | BITWISE_NOT | BITWISE_OR | BITWISE_XOR | EQUAL | NOT_EQUAL | LESS_THAN_OR_EQUAL | LESS_THAN | GREATER_THAN_OR_EQUAL | GREATER_THAN | SLASH | DIV | STAR | ADD | SUBSTRUCT | REMAINDER | BITWISE_LEFT_SHIFT | BITWISE_RIGHT_SHIFT | DOT | COLON | COMMA | SEMICOLON | LPAREN | RPAREN | LSQUARE | RSQUARE | LCURLY | RCURLY | ID | INT | FLOAT | STRING | WS | COMMENT );";
        }
    }
 

}