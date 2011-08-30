// $ANTLR 3.2 Sep 23, 2009 14:05:07 RemusCli.g 2011-08-29 21:33:10

package org.remus.tools.antlr;

import org.remus.tools.CLICommand;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class RemusCliParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "STRING", "SEMICOLON", "WHITESPACE", "DIGIT", "'quit'", "'list'", "'servers'", "'pipelines'", "'instances'", "'applets'", "'use'"
    };
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


        public RemusCliParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public RemusCliParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        

    public String[] getTokenNames() { return RemusCliParser.tokenNames; }
    public String getGrammarFileName() { return "RemusCli.g"; }



    // $ANTLR start "cmd"
    // RemusCli.g:19:1: cmd returns [CLICommand cmd] : (qc= quitCmd | lc= listCmd | uc= useCmd );
    public final CLICommand cmd() throws RecognitionException {
        CLICommand cmd = null;

        CLICommand qc = null;

        CLICommand lc = null;

        CLICommand uc = null;


        try {
            // RemusCli.g:20:2: (qc= quitCmd | lc= listCmd | uc= useCmd )
            int alt1=3;
            switch ( input.LA(1) ) {
            case 8:
                {
                alt1=1;
                }
                break;
            case 9:
                {
                alt1=2;
                }
                break;
            case 14:
                {
                alt1=3;
                }
                break;
            default:
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
                case 3 :
                    // RemusCli.g:22:4: uc= useCmd
                    {
                    pushFollow(FOLLOW_useCmd_in_cmd67);
                    uc=useCmd();

                    state._fsp--;

                    cmd =uc;

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
    // RemusCli.g:25:1: quitCmd returns [CLICommand cmd] : 'quit' ;
    public final CLICommand quitCmd() throws RecognitionException {
        CLICommand cmd = null;

        try {
            // RemusCli.g:26:2: ( 'quit' )
            // RemusCli.g:26:4: 'quit'
            {
            match(input,8,FOLLOW_8_in_quitCmd84); 
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
    // RemusCli.g:30:1: listCmd returns [CLICommand cmd] : ( 'list' 'servers' | 'list' 'pipelines' | 'list' 'instances' | 'list' 'applets' );
    public final CLICommand listCmd() throws RecognitionException {
        CLICommand cmd = null;

        try {
            // RemusCli.g:31:2: ( 'list' 'servers' | 'list' 'pipelines' | 'list' 'instances' | 'list' 'applets' )
            int alt2=4;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==9) ) {
                switch ( input.LA(2) ) {
                case 10:
                    {
                    alt2=1;
                    }
                    break;
                case 11:
                    {
                    alt2=2;
                    }
                    break;
                case 12:
                    {
                    alt2=3;
                    }
                    break;
                case 13:
                    {
                    alt2=4;
                    }
                    break;
                default:
                    NoViableAltException nvae =
                        new NoViableAltException("", 2, 1, input);

                    throw nvae;
                }

            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }
            switch (alt2) {
                case 1 :
                    // RemusCli.g:31:4: 'list' 'servers'
                    {
                    match(input,9,FOLLOW_9_in_listCmd101); 
                    match(input,10,FOLLOW_10_in_listCmd103); 
                    cmd = new CLICommand(CLICommand.LIST); cmd.setSystem(CLICommand.SERVERS);

                    }
                    break;
                case 2 :
                    // RemusCli.g:32:4: 'list' 'pipelines'
                    {
                    match(input,9,FOLLOW_9_in_listCmd110); 
                    match(input,11,FOLLOW_11_in_listCmd112); 
                    cmd = new CLICommand(CLICommand.LIST); cmd.setSystem(CLICommand.PIPELINES);

                    }
                    break;
                case 3 :
                    // RemusCli.g:33:4: 'list' 'instances'
                    {
                    match(input,9,FOLLOW_9_in_listCmd119); 
                    match(input,12,FOLLOW_12_in_listCmd121); 
                    cmd = new CLICommand(CLICommand.LIST); cmd.setSystem(CLICommand.INSTANCES);

                    }
                    break;
                case 4 :
                    // RemusCli.g:34:4: 'list' 'applets'
                    {
                    match(input,9,FOLLOW_9_in_listCmd128); 
                    match(input,13,FOLLOW_13_in_listCmd130); 
                    cmd = new CLICommand(CLICommand.LIST); cmd.setSystem(CLICommand.APPLETS);

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
    // $ANTLR end "listCmd"


    // $ANTLR start "useCmd"
    // RemusCli.g:37:1: useCmd returns [CLICommand cmd] : 'use' pn= pipelineName ;
    public final CLICommand useCmd() throws RecognitionException {
        CLICommand cmd = null;

        String pn = null;


        try {
            // RemusCli.g:38:2: ( 'use' pn= pipelineName )
            // RemusCli.g:38:4: 'use' pn= pipelineName
            {
            match(input,14,FOLLOW_14_in_useCmd146); 
            pushFollow(FOLLOW_pipelineName_in_useCmd150);
            pn=pipelineName();

            state._fsp--;

            cmd = new CLICommand(CLICommand.USE); cmd.setPipeline(pn);

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
    // $ANTLR end "useCmd"


    // $ANTLR start "pipelineName"
    // RemusCli.g:41:1: pipelineName returns [String name] : n= STRING ;
    public final String pipelineName() throws RecognitionException {
        String name = null;

        Token n=null;

        try {
            // RemusCli.g:42:2: (n= STRING )
            // RemusCli.g:42:4: n= STRING
            {
            n=(Token)match(input,STRING,FOLLOW_STRING_in_pipelineName168); 
            name =n.getText();

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return name;
    }
    // $ANTLR end "pipelineName"

    // Delegated rules


 

    public static final BitSet FOLLOW_quitCmd_in_cmd49 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_listCmd_in_cmd58 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_useCmd_in_cmd67 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_8_in_quitCmd84 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_9_in_listCmd101 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_10_in_listCmd103 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_9_in_listCmd110 = new BitSet(new long[]{0x0000000000000800L});
    public static final BitSet FOLLOW_11_in_listCmd112 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_9_in_listCmd119 = new BitSet(new long[]{0x0000000000001000L});
    public static final BitSet FOLLOW_12_in_listCmd121 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_9_in_listCmd128 = new BitSet(new long[]{0x0000000000002000L});
    public static final BitSet FOLLOW_13_in_listCmd130 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_14_in_useCmd146 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_pipelineName_in_useCmd150 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_pipelineName168 = new BitSet(new long[]{0x0000000000000002L});

}