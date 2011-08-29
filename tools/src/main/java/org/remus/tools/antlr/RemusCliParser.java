// $ANTLR 3.2 Sep 23, 2009 14:05:07 RemusCli.g 2011-08-29 13:55:20

package org.remus.tools.antlr;

import org.remus.tools.CLICommand;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class RemusCliParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "DIGIT", "NUMBER", "WHITESPACE", "'quit'", "'list'"
    };
    public static final int NUMBER=5;
    public static final int WHITESPACE=6;
    public static final int DIGIT=4;
    public static final int EOF=-1;
    public static final int T__8=8;
    public static final int T__7=7;

    // delegates
    // delegators


        public RemusCliParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public RemusCliParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        

    public String[] getTokenNames() { return RemusCliParser.tokenNames; }
    public String getGrammarFileName() { return "RemusCli.g"; }



    // $ANTLR start "cmd"
    // RemusCli.g:19:1: cmd returns [CLICommand cmd] : (qc= quitCmd | lc= listCmd );
    public final CLICommand cmd() throws RecognitionException {
        CLICommand cmd = null;

        CLICommand qc = null;

        CLICommand lc = null;


        try {
            // RemusCli.g:20:2: (qc= quitCmd | lc= listCmd )
            int alt1=2;
            int LA1_0 = input.LA(1);

            if ( (LA1_0==7) ) {
                alt1=1;
            }
            else if ( (LA1_0==8) ) {
                alt1=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 1, 0, input);

                throw nvae;
            }
            switch (alt1) {
                case 1 :
                    // RemusCli.g:20:4: qc= quitCmd
                    {
                    pushFollow(FOLLOW_quitCmd_in_cmd49);
                    qc=quitCmd();

                    state._fsp--;

                    cmd =qc;

                    }
                    break;
                case 2 :
                    // RemusCli.g:21:4: lc= listCmd
                    {
                    pushFollow(FOLLOW_listCmd_in_cmd58);
                    lc=listCmd();

                    state._fsp--;

                    cmd =lc;

                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return cmd;
    }
    // $ANTLR end "cmd"


    // $ANTLR start "quitCmd"
    // RemusCli.g:24:1: quitCmd returns [CLICommand cmd] : 'quit' ;
    public final CLICommand quitCmd() throws RecognitionException {
        CLICommand cmd = null;

        try {
            // RemusCli.g:25:2: ( 'quit' )
            // RemusCli.g:25:4: 'quit'
            {
            match(input,7,FOLLOW_7_in_quitCmd74); 
            cmd = new CLICommand(CLICommand.QUIT);

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return cmd;
    }
    // $ANTLR end "quitCmd"


    // $ANTLR start "listCmd"
    // RemusCli.g:29:1: listCmd returns [CLICommand cmd] : 'list' ;
    public final CLICommand listCmd() throws RecognitionException {
        CLICommand cmd = null;

        try {
            // RemusCli.g:30:2: ( 'list' )
            // RemusCli.g:30:4: 'list'
            {
            match(input,8,FOLLOW_8_in_listCmd91); 
            cmd = new CLICommand(CLICommand.LIST);

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return cmd;
    }
    // $ANTLR end "listCmd"

    // Delegated rules


 

    public static final BitSet FOLLOW_quitCmd_in_cmd49 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_listCmd_in_cmd58 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_7_in_quitCmd74 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_8_in_listCmd91 = new BitSet(new long[]{0x0000000000000002L});

}