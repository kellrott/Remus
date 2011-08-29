// $ANTLR 3.2 Sep 23, 2009 14:05:07 RemusCli.g 2011-08-29 13:55:20

package org.remus.tools.antlr;

import org.remus.tools.CLICommand;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class RemusCliLexer extends Lexer {
    public static final int NUMBER=5;
    public static final int WHITESPACE=6;
    public static final int DIGIT=4;
    public static final int EOF=-1;
    public static final int T__8=8;
    public static final int T__7=7;

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

    // $ANTLR start "T__7"
    public final void mT__7() throws RecognitionException {
        try {
            int _type = T__7;
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
    // $ANTLR end "T__7"

    // $ANTLR start "T__8"
    public final void mT__8() throws RecognitionException {
        try {
            int _type = T__8;
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
    // $ANTLR end "T__8"

    // $ANTLR start "NUMBER"
    public final void mNUMBER() throws RecognitionException {
        try {
            int _type = NUMBER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:34:8: ( ( DIGIT )+ )
            // RemusCli.g:34:10: ( DIGIT )+
            {
            // RemusCli.g:34:10: ( DIGIT )+
            int cnt1=0;
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>='0' && LA1_0<='9')) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // RemusCli.g:34:11: DIGIT
            	    {
            	    mDIGIT(); 

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
    // $ANTLR end "NUMBER"

    // $ANTLR start "WHITESPACE"
    public final void mWHITESPACE() throws RecognitionException {
        try {
            int _type = WHITESPACE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // RemusCli.g:36:12: ( ( '\\t' | ' ' | '\\r' | '\\n' | '\\u000C' )+ )
            // RemusCli.g:36:14: ( '\\t' | ' ' | '\\r' | '\\n' | '\\u000C' )+
            {
            // RemusCli.g:36:14: ( '\\t' | ' ' | '\\r' | '\\n' | '\\u000C' )+
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
            // RemusCli.g:38:16: ( '0' .. '9' )
            // RemusCli.g:38:18: '0' .. '9'
            {
            matchRange('0','9'); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "DIGIT"

    public void mTokens() throws RecognitionException {
        // RemusCli.g:1:8: ( T__7 | T__8 | NUMBER | WHITESPACE )
        int alt3=4;
        switch ( input.LA(1) ) {
        case 'q':
            {
            alt3=1;
            }
            break;
        case 'l':
            {
            alt3=2;
            }
            break;
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            {
            alt3=3;
            }
            break;
        case '\t':
        case '\n':
        case '\f':
        case '\r':
        case ' ':
            {
            alt3=4;
            }
            break;
        default:
            NoViableAltException nvae =
                new NoViableAltException("", 3, 0, input);

            throw nvae;
        }

        switch (alt3) {
            case 1 :
                // RemusCli.g:1:10: T__7
                {
                mT__7(); 

                }
                break;
            case 2 :
                // RemusCli.g:1:15: T__8
                {
                mT__8(); 

                }
                break;
            case 3 :
                // RemusCli.g:1:20: NUMBER
                {
                mNUMBER(); 

                }
                break;
            case 4 :
                // RemusCli.g:1:27: WHITESPACE
                {
                mWHITESPACE(); 

                }
                break;

        }

    }


 

}