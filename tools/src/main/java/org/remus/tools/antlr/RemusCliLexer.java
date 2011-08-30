// $ANTLR 3.2 Sep 23, 2009 14:05:07 RemusCli.g 2011-08-29 21:33:10

package org.remus.tools.antlr;

import org.remus.tools.CLICommand;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class RemusCliLexer extends Lexer {
    public static final int T__12=12;
    public static final int T__11=11;
    public static final int T__14=14;
    public static final int T__13=13;
    public static final int T__10=10;
    public static final int WHITESPACE=6;
    public static final int SEMICOLON=5;
    public static final int DIGIT=7;
    public static final int EOF=-1;
    public static final int T__9=9;
    public static final int T__8=8;
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

    // $ANTLR start "T__8"
    public final void mT__8() throws RecognitionException {
        try {
            int _type = T__8;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:13:6: ( 'quit' )
            // RemusCli.g:13:8: 'quit'
            {
            match("quit"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__8"

    // $ANTLR start "T__9"
    public final void mT__9() throws RecognitionException {
        try {
            int _type = T__9;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:14:6: ( 'list' )
            // RemusCli.g:14:8: 'list'
            {
            match("list"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__9"

    // $ANTLR start "T__10"
    public final void mT__10() throws RecognitionException {
        try {
            int _type = T__10;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:15:7: ( 'servers' )
            // RemusCli.g:15:9: 'servers'
            {
            match("servers"); 


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
            // RemusCli.g:16:7: ( 'pipelines' )
            // RemusCli.g:16:9: 'pipelines'
            {
            match("pipelines"); 


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
            // RemusCli.g:17:7: ( 'instances' )
            // RemusCli.g:17:9: 'instances'
            {
            match("instances"); 


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
            // RemusCli.g:18:7: ( 'applets' )
            // RemusCli.g:18:9: 'applets'
            {
            match("applets"); 


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
            // RemusCli.g:19:7: ( 'use' )
            // RemusCli.g:19:9: 'use'
            {
            match("use"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__14"

    // $ANTLR start "STRING"
    public final void mSTRING() throws RecognitionException {
        try {
            int _type = STRING;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:46:8: ( ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' )+ )
            // RemusCli.g:46:10: ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' )+
            {
            // RemusCli.g:46:10: ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' )+
            int cnt1=0;
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>='0' && LA1_0<='9')||(LA1_0>='A' && LA1_0<='Z')||LA1_0=='_'||(LA1_0>='a' && LA1_0<='z')) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // RemusCli.g:
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

    // $ANTLR start "SEMICOLON"
    public final void mSEMICOLON() throws RecognitionException {
        try {
            int _type = SEMICOLON;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:48:11: ( ';' )
            // RemusCli.g:48:13: ';'
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
            // RemusCli.g:50:12: ( ( '\\t' | ' ' | '\\r' | '\\n' | '\\u000C' )+ )
            // RemusCli.g:50:14: ( '\\t' | ' ' | '\\r' | '\\n' | '\\u000C' )+
            {
            // RemusCli.g:50:14: ( '\\t' | ' ' | '\\r' | '\\n' | '\\u000C' )+
            int cnt2=0;
            loop2:
            do {
                int alt2=2;
                int LA2_0 = input.LA(1);

                if ( ((LA2_0>='\t' && LA2_0<='\n')||(LA2_0>='\f' && LA2_0<='\r')||LA2_0==' ') ) {
                    alt2=1;
                }


                switch (alt2) {
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
            	    if ( cnt2 >= 1 ) break loop2;
                        EarlyExitException eee =
                            new EarlyExitException(2, input);
                        throw eee;
                }
                cnt2++;
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
            // RemusCli.g:52:16: ( '0' .. '9' )
            // RemusCli.g:52:18: '0' .. '9'
            {
            matchRange('0','9'); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "DIGIT"

    public void mTokens() throws RecognitionException {
        // RemusCli.g:1:8: ( T__8 | T__9 | T__10 | T__11 | T__12 | T__13 | T__14 | STRING | SEMICOLON | WHITESPACE )
        int alt3=10;
        alt3 = dfa3.predict(input);
        switch (alt3) {
            case 1 :
                // RemusCli.g:1:10: T__8
                {
                mT__8(); 

                }
                break;
            case 2 :
                // RemusCli.g:1:15: T__9
                {
                mT__9(); 

                }
                break;
            case 3 :
                // RemusCli.g:1:20: T__10
                {
                mT__10(); 

                }
                break;
            case 4 :
                // RemusCli.g:1:26: T__11
                {
                mT__11(); 

                }
                break;
            case 5 :
                // RemusCli.g:1:32: T__12
                {
                mT__12(); 

                }
                break;
            case 6 :
                // RemusCli.g:1:38: T__13
                {
                mT__13(); 

                }
                break;
            case 7 :
                // RemusCli.g:1:44: T__14
                {
                mT__14(); 

                }
                break;
            case 8 :
                // RemusCli.g:1:50: STRING
                {
                mSTRING(); 

                }
                break;
            case 9 :
                // RemusCli.g:1:57: SEMICOLON
                {
                mSEMICOLON(); 

                }
                break;
            case 10 :
                // RemusCli.g:1:67: WHITESPACE
                {
                mWHITESPACE(); 

                }
                break;

        }

    }


    protected DFA3 dfa3 = new DFA3(this);
    static final String DFA3_eotS =
        "\1\uffff\7\10\3\uffff\15\10\1\37\1\40\1\41\4\10\3\uffff\10\10\1"+
        "\56\2\10\1\61\1\uffff\2\10\1\uffff\1\64\1\65\2\uffff";
    static final String DFA3_eofS =
        "\66\uffff";
    static final String DFA3_minS =
        "\1\11\1\165\1\151\1\145\1\151\1\156\1\160\1\163\3\uffff\1\151\1"+
        "\163\1\162\1\160\1\163\1\160\1\145\2\164\1\166\1\145\1\164\1\154"+
        "\3\60\1\145\1\154\1\141\1\145\3\uffff\1\162\1\151\1\156\1\164\1"+
        "\163\1\156\1\143\1\163\1\60\2\145\1\60\1\uffff\2\163\1\uffff\2\60"+
        "\2\uffff";
    static final String DFA3_maxS =
        "\1\172\1\165\1\151\1\145\1\151\1\156\1\160\1\163\3\uffff\1\151\1"+
        "\163\1\162\1\160\1\163\1\160\1\145\2\164\1\166\1\145\1\164\1\154"+
        "\3\172\1\145\1\154\1\141\1\145\3\uffff\1\162\1\151\1\156\1\164\1"+
        "\163\1\156\1\143\1\163\1\172\2\145\1\172\1\uffff\2\163\1\uffff\2"+
        "\172\2\uffff";
    static final String DFA3_acceptS =
        "\10\uffff\1\10\1\11\1\12\24\uffff\1\7\1\1\1\2\14\uffff\1\3\2\uffff"+
        "\1\6\2\uffff\1\4\1\5";
    static final String DFA3_specialS =
        "\66\uffff}>";
    static final String[] DFA3_transitionS = {
            "\2\12\1\uffff\2\12\22\uffff\1\12\17\uffff\12\10\1\uffff\1\11"+
            "\5\uffff\32\10\4\uffff\1\10\1\uffff\1\6\7\10\1\5\2\10\1\2\3"+
            "\10\1\4\1\1\1\10\1\3\1\10\1\7\5\10",
            "\1\13",
            "\1\14",
            "\1\15",
            "\1\16",
            "\1\17",
            "\1\20",
            "\1\21",
            "",
            "",
            "",
            "\1\22",
            "\1\23",
            "\1\24",
            "\1\25",
            "\1\26",
            "\1\27",
            "\1\30",
            "\1\31",
            "\1\32",
            "\1\33",
            "\1\34",
            "\1\35",
            "\1\36",
            "\12\10\7\uffff\32\10\4\uffff\1\10\1\uffff\32\10",
            "\12\10\7\uffff\32\10\4\uffff\1\10\1\uffff\32\10",
            "\12\10\7\uffff\32\10\4\uffff\1\10\1\uffff\32\10",
            "\1\42",
            "\1\43",
            "\1\44",
            "\1\45",
            "",
            "",
            "",
            "\1\46",
            "\1\47",
            "\1\50",
            "\1\51",
            "\1\52",
            "\1\53",
            "\1\54",
            "\1\55",
            "\12\10\7\uffff\32\10\4\uffff\1\10\1\uffff\32\10",
            "\1\57",
            "\1\60",
            "\12\10\7\uffff\32\10\4\uffff\1\10\1\uffff\32\10",
            "",
            "\1\62",
            "\1\63",
            "",
            "\12\10\7\uffff\32\10\4\uffff\1\10\1\uffff\32\10",
            "\12\10\7\uffff\32\10\4\uffff\1\10\1\uffff\32\10",
            "",
            ""
    };

    static final short[] DFA3_eot = DFA.unpackEncodedString(DFA3_eotS);
    static final short[] DFA3_eof = DFA.unpackEncodedString(DFA3_eofS);
    static final char[] DFA3_min = DFA.unpackEncodedStringToUnsignedChars(DFA3_minS);
    static final char[] DFA3_max = DFA.unpackEncodedStringToUnsignedChars(DFA3_maxS);
    static final short[] DFA3_accept = DFA.unpackEncodedString(DFA3_acceptS);
    static final short[] DFA3_special = DFA.unpackEncodedString(DFA3_specialS);
    static final short[][] DFA3_transition;

    static {
        int numStates = DFA3_transitionS.length;
        DFA3_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA3_transition[i] = DFA.unpackEncodedString(DFA3_transitionS[i]);
        }
    }

    class DFA3 extends DFA {

        public DFA3(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 3;
            this.eot = DFA3_eot;
            this.eof = DFA3_eof;
            this.min = DFA3_min;
            this.max = DFA3_max;
            this.accept = DFA3_accept;
            this.special = DFA3_special;
            this.transition = DFA3_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( T__8 | T__9 | T__10 | T__11 | T__12 | T__13 | T__14 | STRING | SEMICOLON | WHITESPACE );";
        }
    }
 

}