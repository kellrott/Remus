// $ANTLR 3.2 Sep 23, 2009 14:05:07 RemusCli.g 2011-09-01 15:26:34

package org.remus.tools.antlr;

import org.remus.tools.CLICommand;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class RemusCliParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "STRING", "QUOTESTR", "QUOTE", "SEMICOLON", "WHITESPACE", "DIGIT", "'quit'", "'show'", "'servers'", "'pipelines'", "'stacks'", "'applets'", "'use'", "'select'", "'from'", "'drop'", "'pipeline'", "'load'", "'infile'", "'into'", "':'", "'*'"
    };
    public static final int QUOTESTR=5;
    public static final int T__25=25;
    public static final int T__24=24;
    public static final int T__23=23;
    public static final int T__22=22;
    public static final int T__21=21;
    public static final int T__20=20;
    public static final int WHITESPACE=8;
    public static final int SEMICOLON=7;
    public static final int EOF=-1;
    public static final int T__19=19;
    public static final int QUOTE=6;
    public static final int T__16=16;
    public static final int T__15=15;
    public static final int T__18=18;
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


        public RemusCliParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public RemusCliParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        

    public String[] getTokenNames() { return RemusCliParser.tokenNames; }
    public String getGrammarFileName() { return "RemusCli.g"; }



    // $ANTLR start "cmd"
    // RemusCli.g:19:1: cmd returns [CLICommand cmd] : (qc= quitCmd | lc= showCmd | uc= useCmd | sc= selectCmd | dc= dropCmd | lc= loadCmd );
    public final CLICommand cmd() throws RecognitionException {
        CLICommand cmd = null;

        CLICommand qc = null;

        CLICommand lc = null;

        CLICommand uc = null;

        CLICommand sc = null;

        CLICommand dc = null;


        try {
            // RemusCli.g:20:2: (qc= quitCmd | lc= showCmd | uc= useCmd | sc= selectCmd | dc= dropCmd | lc= loadCmd )
            int alt1=6;
            switch ( input.LA(1) ) {
            case 10:
                {
                alt1=1;
                }
                break;
            case 11:
                {
                alt1=2;
                }
                break;
            case 16:
                {
                alt1=3;
                }
                break;
            case 17:
                {
                alt1=4;
                }
                break;
            case 19:
                {
                alt1=5;
                }
                break;
            case 21:
                {
                alt1=6;
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
                    // RemusCli.g:21:4: lc= showCmd
                    {
                    pushFollow(FOLLOW_showCmd_in_cmd58);
                    lc=showCmd();

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
                case 4 :
                    // RemusCli.g:23:4: sc= selectCmd
                    {
                    pushFollow(FOLLOW_selectCmd_in_cmd77);
                    sc=selectCmd();

                    state._fsp--;

                    cmd =sc;

                    }
                    break;
                case 5 :
                    // RemusCli.g:24:4: dc= dropCmd
                    {
                    pushFollow(FOLLOW_dropCmd_in_cmd86);
                    dc=dropCmd();

                    state._fsp--;

                    cmd =dc;

                    }
                    break;
                case 6 :
                    // RemusCli.g:25:4: lc= loadCmd
                    {
                    pushFollow(FOLLOW_loadCmd_in_cmd95);
                    lc=loadCmd();

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
    // RemusCli.g:28:1: quitCmd returns [CLICommand cmd] : 'quit' ;
    public final CLICommand quitCmd() throws RecognitionException {
        CLICommand cmd = null;

        try {
            // RemusCli.g:29:2: ( 'quit' )
            // RemusCli.g:29:4: 'quit'
            {
            match(input,10,FOLLOW_10_in_quitCmd111); 
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


    // $ANTLR start "showCmd"
    // RemusCli.g:33:1: showCmd returns [CLICommand cmd] : ( 'show' 'servers' | 'show' 'pipelines' | 'show' 'stacks' | 'show' 'applets' );
    public final CLICommand showCmd() throws RecognitionException {
        CLICommand cmd = null;

        try {
            // RemusCli.g:34:2: ( 'show' 'servers' | 'show' 'pipelines' | 'show' 'stacks' | 'show' 'applets' )
            int alt2=4;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==11) ) {
                switch ( input.LA(2) ) {
                case 12:
                    {
                    alt2=1;
                    }
                    break;
                case 13:
                    {
                    alt2=2;
                    }
                    break;
                case 14:
                    {
                    alt2=3;
                    }
                    break;
                case 15:
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
                    // RemusCli.g:34:4: 'show' 'servers'
                    {
                    match(input,11,FOLLOW_11_in_showCmd128); 
                    match(input,12,FOLLOW_12_in_showCmd130); 
                    cmd = new CLICommand(CLICommand.LIST); cmd.setSystem(CLICommand.SERVERS);

                    }
                    break;
                case 2 :
                    // RemusCli.g:35:4: 'show' 'pipelines'
                    {
                    match(input,11,FOLLOW_11_in_showCmd137); 
                    match(input,13,FOLLOW_13_in_showCmd139); 
                    cmd = new CLICommand(CLICommand.LIST); cmd.setSystem(CLICommand.PIPELINES);

                    }
                    break;
                case 3 :
                    // RemusCli.g:36:4: 'show' 'stacks'
                    {
                    match(input,11,FOLLOW_11_in_showCmd146); 
                    match(input,14,FOLLOW_14_in_showCmd148); 
                    cmd = new CLICommand(CLICommand.LIST); cmd.setSystem(CLICommand.STACKS);

                    }
                    break;
                case 4 :
                    // RemusCli.g:37:4: 'show' 'applets'
                    {
                    match(input,11,FOLLOW_11_in_showCmd155); 
                    match(input,15,FOLLOW_15_in_showCmd157); 
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
    // $ANTLR end "showCmd"


    // $ANTLR start "useCmd"
    // RemusCli.g:40:1: useCmd returns [CLICommand cmd] : 'use' pn= pipelineName ;
    public final CLICommand useCmd() throws RecognitionException {
        CLICommand cmd = null;

        String pn = null;


        try {
            // RemusCli.g:41:2: ( 'use' pn= pipelineName )
            // RemusCli.g:41:4: 'use' pn= pipelineName
            {
            match(input,16,FOLLOW_16_in_useCmd173); 
            pushFollow(FOLLOW_pipelineName_in_useCmd177);
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


    // $ANTLR start "selectCmd"
    // RemusCli.g:45:1: selectCmd returns [CLICommand cmd] : 'select' f= fieldSelect 'from' s= stackName ;
    public final CLICommand selectCmd() throws RecognitionException {
        CLICommand cmd = null;

        String f = null;

        String s = null;


        try {
            // RemusCli.g:46:2: ( 'select' f= fieldSelect 'from' s= stackName )
            // RemusCli.g:46:4: 'select' f= fieldSelect 'from' s= stackName
            {
            match(input,17,FOLLOW_17_in_selectCmd194); 
            pushFollow(FOLLOW_fieldSelect_in_selectCmd198);
            f=fieldSelect();

            state._fsp--;

            match(input,18,FOLLOW_18_in_selectCmd200); 
            pushFollow(FOLLOW_stackName_in_selectCmd204);
            s=stackName();

            state._fsp--;

            cmd = new CLICommand(CLICommand.SELECT); cmd.setField(f); cmd.setStack(s);

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
    // $ANTLR end "selectCmd"


    // $ANTLR start "dropCmd"
    // RemusCli.g:50:1: dropCmd returns [CLICommand cmd] : 'drop' 'pipeline' n= pipelineName ;
    public final CLICommand dropCmd() throws RecognitionException {
        CLICommand cmd = null;

        String n = null;


        try {
            // RemusCli.g:51:2: ( 'drop' 'pipeline' n= pipelineName )
            // RemusCli.g:51:4: 'drop' 'pipeline' n= pipelineName
            {
            match(input,19,FOLLOW_19_in_dropCmd221); 
            match(input,20,FOLLOW_20_in_dropCmd223); 
            pushFollow(FOLLOW_pipelineName_in_dropCmd227);
            n=pipelineName();

            state._fsp--;

            cmd = new CLICommand(CLICommand.DROP); cmd.setPipeline(n);

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
    // $ANTLR end "dropCmd"


    // $ANTLR start "loadCmd"
    // RemusCli.g:54:1: loadCmd returns [CLICommand cmd] : 'load' 'pipeline' 'infile' pa= quoteStr 'into' p= pipelineName ;
    public final CLICommand loadCmd() throws RecognitionException {
        CLICommand cmd = null;

        String pa = null;

        String p = null;


        try {
            // RemusCli.g:55:2: ( 'load' 'pipeline' 'infile' pa= quoteStr 'into' p= pipelineName )
            // RemusCli.g:55:4: 'load' 'pipeline' 'infile' pa= quoteStr 'into' p= pipelineName
            {
            match(input,21,FOLLOW_21_in_loadCmd243); 
            match(input,20,FOLLOW_20_in_loadCmd245); 
            match(input,22,FOLLOW_22_in_loadCmd247); 
            pushFollow(FOLLOW_quoteStr_in_loadCmd251);
            pa=quoteStr();

            state._fsp--;

            match(input,23,FOLLOW_23_in_loadCmd253); 
            pushFollow(FOLLOW_pipelineName_in_loadCmd257);
            p=pipelineName();

            state._fsp--;

            cmd = new CLICommand(CLICommand.LOAD); cmd.setPipeline(p); cmd.setPath(pa);

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
    // $ANTLR end "loadCmd"


    // $ANTLR start "stackName"
    // RemusCli.g:58:1: stackName returns [String name] : (n= STRING ':' m= STRING | n1= STRING ':' m1= STRING ':' o1= STRING );
    public final String stackName() throws RecognitionException {
        String name = null;

        Token n=null;
        Token m=null;
        Token n1=null;
        Token m1=null;
        Token o1=null;

        try {
            // RemusCli.g:59:2: (n= STRING ':' m= STRING | n1= STRING ':' m1= STRING ':' o1= STRING )
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==STRING) ) {
                int LA3_1 = input.LA(2);

                if ( (LA3_1==24) ) {
                    int LA3_2 = input.LA(3);

                    if ( (LA3_2==STRING) ) {
                        int LA3_3 = input.LA(4);

                        if ( (LA3_3==24) ) {
                            alt3=2;
                        }
                        else if ( (LA3_3==EOF) ) {
                            alt3=1;
                        }
                        else {
                            NoViableAltException nvae =
                                new NoViableAltException("", 3, 3, input);

                            throw nvae;
                        }
                    }
                    else {
                        NoViableAltException nvae =
                            new NoViableAltException("", 3, 2, input);

                        throw nvae;
                    }
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 3, 1, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 3, 0, input);

                throw nvae;
            }
            switch (alt3) {
                case 1 :
                    // RemusCli.g:59:4: n= STRING ':' m= STRING
                    {
                    n=(Token)match(input,STRING,FOLLOW_STRING_in_stackName275); 
                    match(input,24,FOLLOW_24_in_stackName277); 
                    m=(Token)match(input,STRING,FOLLOW_STRING_in_stackName281); 
                    name =n.getText() + ":" + m.getText();

                    }
                    break;
                case 2 :
                    // RemusCli.g:60:4: n1= STRING ':' m1= STRING ':' o1= STRING
                    {
                    n1=(Token)match(input,STRING,FOLLOW_STRING_in_stackName290); 
                    match(input,24,FOLLOW_24_in_stackName292); 
                    m1=(Token)match(input,STRING,FOLLOW_STRING_in_stackName296); 
                    match(input,24,FOLLOW_24_in_stackName298); 
                    o1=(Token)match(input,STRING,FOLLOW_STRING_in_stackName302); 
                    name =n1.getText() + ":" + m1.getText() + ":" + o1.getText();

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
        return name;
    }
    // $ANTLR end "stackName"


    // $ANTLR start "pipelineName"
    // RemusCli.g:63:1: pipelineName returns [String name] : n= STRING ;
    public final String pipelineName() throws RecognitionException {
        String name = null;

        Token n=null;

        try {
            // RemusCli.g:64:2: (n= STRING )
            // RemusCli.g:64:4: n= STRING
            {
            n=(Token)match(input,STRING,FOLLOW_STRING_in_pipelineName320); 
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


    // $ANTLR start "fieldSelect"
    // RemusCli.g:67:1: fieldSelect returns [String field] : (n= STRING | '*' );
    public final String fieldSelect() throws RecognitionException {
        String field = null;

        Token n=null;

        try {
            // RemusCli.g:68:2: (n= STRING | '*' )
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==STRING) ) {
                alt4=1;
            }
            else if ( (LA4_0==25) ) {
                alt4=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 4, 0, input);

                throw nvae;
            }
            switch (alt4) {
                case 1 :
                    // RemusCli.g:68:4: n= STRING
                    {
                    n=(Token)match(input,STRING,FOLLOW_STRING_in_fieldSelect338); 
                    field =n.getText();

                    }
                    break;
                case 2 :
                    // RemusCli.g:69:4: '*'
                    {
                    match(input,25,FOLLOW_25_in_fieldSelect345); 
                    field ="*";

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
        return field;
    }
    // $ANTLR end "fieldSelect"


    // $ANTLR start "quoteStr"
    // RemusCli.g:72:1: quoteStr returns [String str] : s= QUOTESTR ;
    public final String quoteStr() throws RecognitionException {
        String str = null;

        Token s=null;

        try {
            // RemusCli.g:73:2: (s= QUOTESTR )
            // RemusCli.g:73:4: s= QUOTESTR
            {
            s=(Token)match(input,QUOTESTR,FOLLOW_QUOTESTR_in_quoteStr363); 
            String t=s.getText(); str =t.substring(1,t.length()-1);

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return str;
    }
    // $ANTLR end "quoteStr"

    // Delegated rules


 

    public static final BitSet FOLLOW_quitCmd_in_cmd49 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_showCmd_in_cmd58 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_useCmd_in_cmd67 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_selectCmd_in_cmd77 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_dropCmd_in_cmd86 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_loadCmd_in_cmd95 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_10_in_quitCmd111 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_11_in_showCmd128 = new BitSet(new long[]{0x0000000000001000L});
    public static final BitSet FOLLOW_12_in_showCmd130 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_11_in_showCmd137 = new BitSet(new long[]{0x0000000000002000L});
    public static final BitSet FOLLOW_13_in_showCmd139 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_11_in_showCmd146 = new BitSet(new long[]{0x0000000000004000L});
    public static final BitSet FOLLOW_14_in_showCmd148 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_11_in_showCmd155 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_15_in_showCmd157 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_16_in_useCmd173 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_pipelineName_in_useCmd177 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_17_in_selectCmd194 = new BitSet(new long[]{0x0000000002000010L});
    public static final BitSet FOLLOW_fieldSelect_in_selectCmd198 = new BitSet(new long[]{0x0000000000040000L});
    public static final BitSet FOLLOW_18_in_selectCmd200 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_stackName_in_selectCmd204 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_19_in_dropCmd221 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_20_in_dropCmd223 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_pipelineName_in_dropCmd227 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_21_in_loadCmd243 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_20_in_loadCmd245 = new BitSet(new long[]{0x0000000000400000L});
    public static final BitSet FOLLOW_22_in_loadCmd247 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_quoteStr_in_loadCmd251 = new BitSet(new long[]{0x0000000000800000L});
    public static final BitSet FOLLOW_23_in_loadCmd253 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_pipelineName_in_loadCmd257 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_stackName275 = new BitSet(new long[]{0x0000000001000000L});
    public static final BitSet FOLLOW_24_in_stackName277 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_STRING_in_stackName281 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_stackName290 = new BitSet(new long[]{0x0000000001000000L});
    public static final BitSet FOLLOW_24_in_stackName292 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_STRING_in_stackName296 = new BitSet(new long[]{0x0000000001000000L});
    public static final BitSet FOLLOW_24_in_stackName298 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_STRING_in_stackName302 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_pipelineName320 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_fieldSelect338 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_25_in_fieldSelect345 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUOTESTR_in_quoteStr363 = new BitSet(new long[]{0x0000000000000002L});

}