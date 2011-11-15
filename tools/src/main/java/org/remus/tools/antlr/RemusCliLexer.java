// $ANTLR 3.2 Sep 23, 2009 14:05:07 RemusCli.g 2011-11-15 09:11:11

package org.remus.tools.antlr;

import org.remus.tools.CLICommand;
import org.remus.tools.Selection;
import org.remus.tools.Conditional;
import java.util.LinkedList;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class RemusCliLexer extends Lexer {
    public static final int QUOTESTR=5;
    public static final int T__29=29;
    public static final int T__28=28;
    public static final int T__27=27;
    public static final int T__26=26;
    public static final int T__25=25;
    public static final int T__24=24;
    public static final int T__23=23;
    public static final int T__22=22;
    public static final int T__21=21;
    public static final int T__20=20;
    public static final int WHITESPACE=8;
    public static final int SEMICOLON=7;
    public static final int EOF=-1;
    public static final int T__30=30;
    public static final int T__19=19;
    public static final int T__31=31;
    public static final int QUOTE=6;
    public static final int T__32=32;
    public static final int T__16=16;
    public static final int T__33=33;
    public static final int T__15=15;
    public static final int T__34=34;
    public static final int T__18=18;
    public static final int T__35=35;
    public static final int T__17=17;
    public static final int T__12=12;
    public static final int T__11=11;
    public static final int T__14=14;
    public static final int T__13=13;
    public static final int T__10=10;
    public static final int DIGIT=9;
    public static final int STRING=4;

    // delegates
    // delegators

    public RemusCliLexer() {;} 
    public RemusCliLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public RemusCliLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);

    }
    public String getGrammarFileName() { return "RemusCli.g"; }

    // $ANTLR start "T__10"
    public final void mT__10() throws RecognitionException {
        try {
            int _type = T__10;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:16:7: ( 'quit' )
            // RemusCli.g:16:9: 'quit'
            {
            match("quit"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__10"

    // $ANTLR start "T__11"
    public final void mT__11() throws RecognitionException {
        try {
            int _type = T__11;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:17:7: ( 'show' )
            // RemusCli.g:17:9: 'show'
            {
            match("show"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__11"

    // $ANTLR start "T__12"
    public final void mT__12() throws RecognitionException {
        try {
            int _type = T__12;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:18:7: ( 'servers' )
            // RemusCli.g:18:9: 'servers'
            {
            match("servers"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__12"

    // $ANTLR start "T__13"
    public final void mT__13() throws RecognitionException {
        try {
            int _type = T__13;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:19:7: ( 'pipelines' )
            // RemusCli.g:19:9: 'pipelines'
            {
            match("pipelines"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__13"

    // $ANTLR start "T__14"
    public final void mT__14() throws RecognitionException {
        try {
            int _type = T__14;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:20:7: ( 'tables' )
            // RemusCli.g:20:9: 'tables'
            {
            match("tables"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__14"

    // $ANTLR start "T__15"
    public final void mT__15() throws RecognitionException {
        try {
            int _type = T__15;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:21:7: ( 'applets' )
            // RemusCli.g:21:9: 'applets'
            {
            match("applets"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__15"

    // $ANTLR start "T__16"
    public final void mT__16() throws RecognitionException {
        try {
            int _type = T__16;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:22:7: ( 'use' )
            // RemusCli.g:22:9: 'use'
            {
            match("use"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__16"

    // $ANTLR start "T__17"
    public final void mT__17() throws RecognitionException {
        try {
            int _type = T__17;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:23:7: ( 'select' )
            // RemusCli.g:23:9: 'select'
            {
            match("select"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__17"

    // $ANTLR start "T__18"
    public final void mT__18() throws RecognitionException {
        try {
            int _type = T__18;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:24:7: ( 'from' )
            // RemusCli.g:24:9: 'from'
            {
            match("from"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__18"

    // $ANTLR start "T__19"
    public final void mT__19() throws RecognitionException {
        try {
            int _type = T__19;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:25:7: ( 'where' )
            // RemusCli.g:25:9: 'where'
            {
            match("where"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__19"

    // $ANTLR start "T__20"
    public final void mT__20() throws RecognitionException {
        try {
            int _type = T__20;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:26:7: ( 'limit' )
            // RemusCli.g:26:9: 'limit'
            {
            match("limit"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__20"

    // $ANTLR start "T__21"
    public final void mT__21() throws RecognitionException {
        try {
            int _type = T__21;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:27:7: ( 'delete' )
            // RemusCli.g:27:9: 'delete'
            {
            match("delete"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__21"

    // $ANTLR start "T__22"
    public final void mT__22() throws RecognitionException {
        try {
            int _type = T__22;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:28:7: ( 'drop' )
            // RemusCli.g:28:9: 'drop'
            {
            match("drop"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__22"

    // $ANTLR start "T__23"
    public final void mT__23() throws RecognitionException {
        try {
            int _type = T__23;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:29:7: ( 'pipeline' )
            // RemusCli.g:29:9: 'pipeline'
            {
            match("pipeline"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__23"

    // $ANTLR start "T__24"
    public final void mT__24() throws RecognitionException {
        try {
            int _type = T__24;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:30:7: ( 'load' )
            // RemusCli.g:30:9: 'load'
            {
            match("load"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__24"

    // $ANTLR start "T__25"
    public final void mT__25() throws RecognitionException {
        try {
            int _type = T__25;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:31:7: ( 'infile' )
            // RemusCli.g:31:9: 'infile'
            {
            match("infile"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__25"

    // $ANTLR start "T__26"
    public final void mT__26() throws RecognitionException {
        try {
            int _type = T__26;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:32:7: ( 'into' )
            // RemusCli.g:32:9: 'into'
            {
            match("into"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__26"

    // $ANTLR start "T__27"
    public final void mT__27() throws RecognitionException {
        try {
            int _type = T__27;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:33:7: ( ':' )
            // RemusCli.g:33:9: ':'
            {
            match(':'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__27"

    // $ANTLR start "T__28"
    public final void mT__28() throws RecognitionException {
        try {
            int _type = T__28;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:34:7: ( '@' )
            // RemusCli.g:34:9: '@'
            {
            match('@'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__28"

    // $ANTLR start "T__29"
    public final void mT__29() throws RecognitionException {
        try {
            int _type = T__29;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:35:7: ( ',' )
            // RemusCli.g:35:9: ','
            {
            match(','); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__29"

    // $ANTLR start "T__30"
    public final void mT__30() throws RecognitionException {
        try {
            int _type = T__30;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:36:7: ( '*' )
            // RemusCli.g:36:9: '*'
            {
            match('*'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__30"

    // $ANTLR start "T__31"
    public final void mT__31() throws RecognitionException {
        try {
            int _type = T__31;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:37:7: ( 'KEY' )
            // RemusCli.g:37:9: 'KEY'
            {
            match("KEY"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__31"

    // $ANTLR start "T__32"
    public final void mT__32() throws RecognitionException {
        try {
            int _type = T__32;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:38:7: ( '=' )
            // RemusCli.g:38:9: '='
            {
            match('='); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__32"

    // $ANTLR start "T__33"
    public final void mT__33() throws RecognitionException {
        try {
            int _type = T__33;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:39:7: ( '!=' )
            // RemusCli.g:39:9: '!='
            {
            match("!="); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__33"

    // $ANTLR start "T__34"
    public final void mT__34() throws RecognitionException {
        try {
            int _type = T__34;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:40:7: ( 'like' )
            // RemusCli.g:40:9: 'like'
            {
            match("like"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__34"

    // $ANTLR start "T__35"
    public final void mT__35() throws RecognitionException {
        try {
            int _type = T__35;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:41:7: ( 'not' )
            // RemusCli.g:41:9: 'not'
            {
            match("not"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__35"

    // $ANTLR start "STRING"
    public final void mSTRING() throws RecognitionException {
        try {
            int _type = STRING;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:125:8: ( ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' | '\\.' | '@' )+ )
            // RemusCli.g:125:10: ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' | '\\.' | '@' )+
            {
            // RemusCli.g:125:10: ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' | '\\.' | '@' )+
            int cnt1=0;
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0=='.'||(LA1_0>='0' && LA1_0<='9')||(LA1_0>='@' && LA1_0<='Z')||LA1_0=='_'||(LA1_0>='a' && LA1_0<='z')) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // RemusCli.g:
            	    {
            	    if ( input.LA(1)=='.'||(input.LA(1)>='0' && input.LA(1)<='9')||(input.LA(1)>='@' && input.LA(1)<='Z')||input.LA(1)=='_'||(input.LA(1)>='a' && input.LA(1)<='z') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    if ( cnt1 >= 1 ) break loop1;
                        EarlyExitException eee =
                            new EarlyExitException(1, input);
                        throw eee;
                }
                cnt1++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "STRING"

    // $ANTLR start "QUOTE"
    public final void mQUOTE() throws RecognitionException {
        try {
            int _type = QUOTE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:128:7: ( '\"' )
            // RemusCli.g:128:9: '\"'
            {
            match('\"'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "QUOTE"

    // $ANTLR start "QUOTESTR"
    public final void mQUOTESTR() throws RecognitionException {
        try {
            int _type = QUOTESTR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:130:10: ( QUOTE ( options {greedy=false; } : . )* QUOTE )
            // RemusCli.g:130:12: QUOTE ( options {greedy=false; } : . )* QUOTE
            {
            mQUOTE(); 
            // RemusCli.g:130:18: ( options {greedy=false; } : . )*
            loop2:
            do {
                int alt2=2;
                int LA2_0 = input.LA(1);

                if ( (LA2_0=='\"') ) {
                    alt2=2;
                }
                else if ( ((LA2_0>='\u0000' && LA2_0<='!')||(LA2_0>='#' && LA2_0<='\uFFFF')) ) {
                    alt2=1;
                }


                switch (alt2) {
            	case 1 :
            	    // RemusCli.g:130:45: .
            	    {
            	    matchAny(); 

            	    }
            	    break;

            	default :
            	    break loop2;
                }
            } while (true);

            mQUOTE(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "QUOTESTR"

    // $ANTLR start "SEMICOLON"
    public final void mSEMICOLON() throws RecognitionException {
        try {
            int _type = SEMICOLON;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:132:11: ( ';' )
            // RemusCli.g:132:13: ';'
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

    // $ANTLR start "WHITESPACE"
    public final void mWHITESPACE() throws RecognitionException {
        try {
            int _type = WHITESPACE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:134:12: ( ( '\\t' | ' ' | '\\r' | '\\n' | '\\u000C' )+ )
            // RemusCli.g:134:14: ( '\\t' | ' ' | '\\r' | '\\n' | '\\u000C' )+
            {
            // RemusCli.g:134:14: ( '\\t' | ' ' | '\\r' | '\\n' | '\\u000C' )+
            int cnt3=0;
            loop3:
            do {
                int alt3=2;
                int LA3_0 = input.LA(1);

                if ( ((LA3_0>='\t' && LA3_0<='\n')||(LA3_0>='\f' && LA3_0<='\r')||LA3_0==' ') ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // RemusCli.g:
            	    {
            	    if ( (input.LA(1)>='\t' && input.LA(1)<='\n')||(input.LA(1)>='\f' && input.LA(1)<='\r')||input.LA(1)==' ' ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    if ( cnt3 >= 1 ) break loop3;
                        EarlyExitException eee =
                            new EarlyExitException(3, input);
                        throw eee;
                }
                cnt3++;
            } while (true);

             _channel = HIDDEN; 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "WHITESPACE"

    // $ANTLR start "DIGIT"
    public final void mDIGIT() throws RecognitionException {
        try {
            // RemusCli.g:136:16: ( '0' .. '9' )
            // RemusCli.g:136:18: '0' .. '9'
            {
            matchRange('0','9'); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "DIGIT"

    public void mTokens() throws RecognitionException {
        // RemusCli.g:1:8: ( T__10 | T__11 | T__12 | T__13 | T__14 | T__15 | T__16 | T__17 | T__18 | T__19 | T__20 | T__21 | T__22 | T__23 | T__24 | T__25 | T__26 | T__27 | T__28 | T__29 | T__30 | T__31 | T__32 | T__33 | T__34 | T__35 | STRING | QUOTE | QUOTESTR | SEMICOLON | WHITESPACE )
        int alt4=31;
        alt4 = dfa4.predict(input);
        switch (alt4) {
            case 1 :
                // RemusCli.g:1:10: T__10
                {
                mT__10(); 

                }
                break;
            case 2 :
                // RemusCli.g:1:16: T__11
                {
                mT__11(); 

                }
                break;
            case 3 :
                // RemusCli.g:1:22: T__12
                {
                mT__12(); 

                }
                break;
            case 4 :
                // RemusCli.g:1:28: T__13
                {
                mT__13(); 

                }
                break;
            case 5 :
                // RemusCli.g:1:34: T__14
                {
                mT__14(); 

                }
                break;
            case 6 :
                // RemusCli.g:1:40: T__15
                {
                mT__15(); 

                }
                break;
            case 7 :
                // RemusCli.g:1:46: T__16
                {
                mT__16(); 

                }
                break;
            case 8 :
                // RemusCli.g:1:52: T__17
                {
                mT__17(); 

                }
                break;
            case 9 :
                // RemusCli.g:1:58: T__18
                {
                mT__18(); 

                }
                break;
            case 10 :
                // RemusCli.g:1:64: T__19
                {
                mT__19(); 

                }
                break;
            case 11 :
                // RemusCli.g:1:70: T__20
                {
                mT__20(); 

                }
                break;
            case 12 :
                // RemusCli.g:1:76: T__21
                {
                mT__21(); 

                }
                break;
            case 13 :
                // RemusCli.g:1:82: T__22
                {
                mT__22(); 

                }
                break;
            case 14 :
                // RemusCli.g:1:88: T__23
                {
                mT__23(); 

                }
                break;
            case 15 :
                // RemusCli.g:1:94: T__24
                {
                mT__24(); 

                }
                break;
            case 16 :
                // RemusCli.g:1:100: T__25
                {
                mT__25(); 

                }
                break;
            case 17 :
                // RemusCli.g:1:106: T__26
                {
                mT__26(); 

                }
                break;
            case 18 :
                // RemusCli.g:1:112: T__27
                {
                mT__27(); 

                }
                break;
            case 19 :
                // RemusCli.g:1:118: T__28
                {
                mT__28(); 

                }
                break;
            case 20 :
                // RemusCli.g:1:124: T__29
                {
                mT__29(); 

                }
                break;
            case 21 :
                // RemusCli.g:1:130: T__30
                {
                mT__30(); 

                }
                break;
            case 22 :
                // RemusCli.g:1:136: T__31
                {
                mT__31(); 

                }
                break;
            case 23 :
                // RemusCli.g:1:142: T__32
                {
                mT__32(); 

                }
                break;
            case 24 :
                // RemusCli.g:1:148: T__33
                {
                mT__33(); 

                }
                break;
            case 25 :
                // RemusCli.g:1:154: T__34
                {
                mT__34(); 

                }
                break;
            case 26 :
                // RemusCli.g:1:160: T__35
                {
                mT__35(); 

                }
                break;
            case 27 :
                // RemusCli.g:1:166: STRING
                {
                mSTRING(); 

                }
                break;
            case 28 :
                // RemusCli.g:1:173: QUOTE
                {
                mQUOTE(); 

                }
                break;
            case 29 :
                // RemusCli.g:1:179: QUOTESTR
                {
                mQUOTESTR(); 

                }
                break;
            case 30 :
                // RemusCli.g:1:188: SEMICOLON
                {
                mSEMICOLON(); 

                }
                break;
            case 31 :
                // RemusCli.g:1:198: WHITESPACE
                {
                mWHITESPACE(); 

                }
                break;

        }

    }


    protected DFA4 dfa4 = new DFA4(this);
    static final String DFA4_eotS =
        "\1\uffff\13\24\1\uffff\1\46\2\uffff\1\24\2\uffff\1\24\1\uffff\1"+
        "\51\2\uffff\16\24\1\uffff\2\24\2\uffff\7\24\1\105\11\24\1\117\1"+
        "\120\1\121\1\122\5\24\1\uffff\1\130\2\24\1\133\1\134\1\24\1\136"+
        "\1\24\1\140\4\uffff\5\24\1\uffff\1\146\1\147\2\uffff\1\24\1\uffff"+
        "\1\24\1\uffff\1\24\1\153\1\24\1\155\1\24\2\uffff\1\157\1\160\1\161"+
        "\1\uffff\1\24\1\uffff\1\163\3\uffff\1\165\1\uffff\1\166\2\uffff";
    static final String DFA4_eofS =
        "\167\uffff";
    static final String DFA4_minS =
        "\1\11\1\165\1\145\1\151\1\141\1\160\1\163\1\162\1\150\1\151\1\145"+
        "\1\156\1\uffff\1\56\2\uffff\1\105\2\uffff\1\157\1\uffff\1\0\2\uffff"+
        "\1\151\1\157\1\154\1\160\1\142\1\160\1\145\1\157\1\145\1\153\1\141"+
        "\1\154\1\157\1\146\1\uffff\1\131\1\164\2\uffff\1\164\1\167\1\166"+
        "\2\145\2\154\1\56\1\155\1\162\1\151\1\145\1\144\1\145\1\160\1\151"+
        "\1\157\4\56\1\145\1\143\1\154\2\145\1\uffff\1\56\1\145\1\164\2\56"+
        "\1\164\1\56\1\154\1\56\4\uffff\1\162\1\164\1\151\1\163\1\164\1\uffff"+
        "\2\56\2\uffff\1\145\1\uffff\1\145\1\uffff\1\163\1\56\1\156\1\56"+
        "\1\163\2\uffff\3\56\1\uffff\1\145\1\uffff\1\56\3\uffff\1\56\1\uffff"+
        "\1\56\2\uffff";
    static final String DFA4_maxS =
        "\1\172\1\165\1\150\1\151\1\141\1\160\1\163\1\162\1\150\1\157\1\162"+
        "\1\156\1\uffff\1\172\2\uffff\1\105\2\uffff\1\157\1\uffff\1\uffff"+
        "\2\uffff\1\151\1\157\1\162\1\160\1\142\1\160\1\145\1\157\1\145\1"+
        "\155\1\141\1\154\1\157\1\164\1\uffff\1\131\1\164\2\uffff\1\164\1"+
        "\167\1\166\2\145\2\154\1\172\1\155\1\162\1\151\1\145\1\144\1\145"+
        "\1\160\1\151\1\157\4\172\1\145\1\143\1\154\2\145\1\uffff\1\172\1"+
        "\145\1\164\2\172\1\164\1\172\1\154\1\172\4\uffff\1\162\1\164\1\151"+
        "\1\163\1\164\1\uffff\2\172\2\uffff\1\145\1\uffff\1\145\1\uffff\1"+
        "\163\1\172\1\156\1\172\1\163\2\uffff\3\172\1\uffff\1\145\1\uffff"+
        "\1\172\3\uffff\1\172\1\uffff\1\172\2\uffff";
    static final String DFA4_acceptS =
        "\14\uffff\1\22\1\uffff\1\24\1\25\1\uffff\1\27\1\30\1\uffff\1\33"+
        "\1\uffff\1\36\1\37\16\uffff\1\23\2\uffff\1\34\1\35\32\uffff\1\7"+
        "\11\uffff\1\26\1\32\1\1\1\2\5\uffff\1\11\2\uffff\1\31\1\17\1\uffff"+
        "\1\15\1\uffff\1\21\5\uffff\1\12\1\13\3\uffff\1\10\1\uffff\1\5\1"+
        "\uffff\1\14\1\20\1\3\1\uffff\1\6\1\uffff\1\16\1\4";
    static final String DFA4_specialS =
        "\25\uffff\1\0\141\uffff}>";
    static final String[] DFA4_transitionS = {
            "\2\27\1\uffff\2\27\22\uffff\1\27\1\22\1\25\7\uffff\1\17\1\uffff"+
            "\1\16\1\uffff\1\24\1\uffff\12\24\1\14\1\26\1\uffff\1\21\2\uffff"+
            "\1\15\12\24\1\20\17\24\4\uffff\1\24\1\uffff\1\5\2\24\1\12\1"+
            "\24\1\7\2\24\1\13\2\24\1\11\1\24\1\23\1\24\1\3\1\1\1\24\1\2"+
            "\1\4\1\6\1\24\1\10\3\24",
            "\1\30",
            "\1\32\2\uffff\1\31",
            "\1\33",
            "\1\34",
            "\1\35",
            "\1\36",
            "\1\37",
            "\1\40",
            "\1\41\5\uffff\1\42",
            "\1\43\14\uffff\1\44",
            "\1\45",
            "",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "",
            "",
            "\1\47",
            "",
            "",
            "\1\50",
            "",
            "\0\52",
            "",
            "",
            "\1\53",
            "\1\54",
            "\1\56\5\uffff\1\55",
            "\1\57",
            "\1\60",
            "\1\61",
            "\1\62",
            "\1\63",
            "\1\64",
            "\1\66\1\uffff\1\65",
            "\1\67",
            "\1\70",
            "\1\71",
            "\1\72\15\uffff\1\73",
            "",
            "\1\74",
            "\1\75",
            "",
            "",
            "\1\76",
            "\1\77",
            "\1\100",
            "\1\101",
            "\1\102",
            "\1\103",
            "\1\104",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "\1\106",
            "\1\107",
            "\1\110",
            "\1\111",
            "\1\112",
            "\1\113",
            "\1\114",
            "\1\115",
            "\1\116",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "\1\123",
            "\1\124",
            "\1\125",
            "\1\126",
            "\1\127",
            "",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "\1\131",
            "\1\132",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "\1\135",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "\1\137",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "",
            "",
            "",
            "",
            "\1\141",
            "\1\142",
            "\1\143",
            "\1\144",
            "\1\145",
            "",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "",
            "",
            "\1\150",
            "",
            "\1\151",
            "",
            "\1\152",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "\1\154",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "\1\156",
            "",
            "",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "",
            "\1\162",
            "",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "",
            "",
            "",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\22\24"+
            "\1\164\7\24",
            "",
            "\1\24\1\uffff\12\24\6\uffff\33\24\4\uffff\1\24\1\uffff\32\24",
            "",
            ""
    };

    static final short[] DFA4_eot = DFA.unpackEncodedString(DFA4_eotS);
    static final short[] DFA4_eof = DFA.unpackEncodedString(DFA4_eofS);
    static final char[] DFA4_min = DFA.unpackEncodedStringToUnsignedChars(DFA4_minS);
    static final char[] DFA4_max = DFA.unpackEncodedStringToUnsignedChars(DFA4_maxS);
    static final short[] DFA4_accept = DFA.unpackEncodedString(DFA4_acceptS);
    static final short[] DFA4_special = DFA.unpackEncodedString(DFA4_specialS);
    static final short[][] DFA4_transition;

    static {
        int numStates = DFA4_transitionS.length;
        DFA4_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA4_transition[i] = DFA.unpackEncodedString(DFA4_transitionS[i]);
        }
    }

    class DFA4 extends DFA {

        public DFA4(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 4;
            this.eot = DFA4_eot;
            this.eof = DFA4_eof;
            this.min = DFA4_min;
            this.max = DFA4_max;
            this.accept = DFA4_accept;
            this.special = DFA4_special;
            this.transition = DFA4_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( T__10 | T__11 | T__12 | T__13 | T__14 | T__15 | T__16 | T__17 | T__18 | T__19 | T__20 | T__21 | T__22 | T__23 | T__24 | T__25 | T__26 | T__27 | T__28 | T__29 | T__30 | T__31 | T__32 | T__33 | T__34 | T__35 | STRING | QUOTE | QUOTESTR | SEMICOLON | WHITESPACE );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            IntStream input = _input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA4_21 = input.LA(1);

                        s = -1;
                        if ( ((LA4_21>='\u0000' && LA4_21<='\uFFFF')) ) {s = 42;}

                        else s = 41;

                        if ( s>=0 ) return s;
                        break;
            }
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 4, _s, input);
            error(nvae);
            throw nvae;
        }
    }
 

}