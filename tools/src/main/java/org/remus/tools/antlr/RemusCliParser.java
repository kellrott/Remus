// $ANTLR 3.2 Sep 23, 2009 14:05:07 RemusCli.g 2011-09-12 16:39:19

package org.remus.tools.antlr;

import org.remus.tools.CLICommand;
import org.remus.tools.Selection;
import org.remus.tools.Conditional;
import java.util.LinkedList;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class RemusCliParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "STRING", "QUOTESTR", "QUOTE", "SEMICOLON", "WHITESPACE", "DIGIT", "'quit'", "'show'", "'servers'", "'pipelines'", "'stacks'", "'applets'", "'use'", "'select'", "'from'", "'where'", "'limit'", "'drop'", "'pipeline'", "'load'", "'infile'", "'into'", "':'", "'@'", "','", "'*'", "'KEY'", "'='"
    };
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
    public static final int QUOTE=6;
    public static final int T__31=31;
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
    // RemusCli.g:25:1: cmd returns [CLICommand cmd] : (qc= quitCmd | lc= showCmd | uc= useCmd | sc= selectCmd | dc= dropCmd | lc= loadCmd );
    public final CLICommand cmd() throws RecognitionException {
        CLICommand cmd = null;

        CLICommand qc = null;

        CLICommand lc = null;

        CLICommand uc = null;

        CLICommand sc = null;

        CLICommand dc = null;


        try {
            // RemusCli.g:26:2: (qc= quitCmd | lc= showCmd | uc= useCmd | sc= selectCmd | dc= dropCmd | lc= loadCmd )
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
            case 21:
                {
                alt1=5;
                }
                break;
            case 23:
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
                    // RemusCli.g:26:4: qc= quitCmd
                    {
                    pushFollow(FOLLOW_quitCmd_in_cmd49);
                    qc=quitCmd();

                    state._fsp--;

                    cmd =qc;

                    }
                    break;
                case 2 :
                    // RemusCli.g:27:4: lc= showCmd
                    {
                    pushFollow(FOLLOW_showCmd_in_cmd58);
                    lc=showCmd();

                    state._fsp--;

                    cmd =lc;

                    }
                    break;
                case 3 :
                    // RemusCli.g:28:4: uc= useCmd
                    {
                    pushFollow(FOLLOW_useCmd_in_cmd67);
                    uc=useCmd();

                    state._fsp--;

                    cmd =uc;

                    }
                    break;
                case 4 :
                    // RemusCli.g:29:4: sc= selectCmd
                    {
                    pushFollow(FOLLOW_selectCmd_in_cmd77);
                    sc=selectCmd();

                    state._fsp--;

                    cmd =sc;

                    }
                    break;
                case 5 :
                    // RemusCli.g:30:4: dc= dropCmd
                    {
                    pushFollow(FOLLOW_dropCmd_in_cmd86);
                    dc=dropCmd();

                    state._fsp--;

                    cmd =dc;

                    }
                    break;
                case 6 :
                    // RemusCli.g:31:4: lc= loadCmd
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
    // RemusCli.g:34:1: quitCmd returns [CLICommand cmd] : 'quit' ;
    public final CLICommand quitCmd() throws RecognitionException {
        CLICommand cmd = null;

        try {
            // RemusCli.g:35:2: ( 'quit' )
            // RemusCli.g:35:4: 'quit'
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
    // RemusCli.g:39:1: showCmd returns [CLICommand cmd] : ( 'show' 'servers' | 'show' 'pipelines' | 'show' 'stacks' | 'show' 'applets' );
    public final CLICommand showCmd() throws RecognitionException {
        CLICommand cmd = null;

        try {
            // RemusCli.g:40:2: ( 'show' 'servers' | 'show' 'pipelines' | 'show' 'stacks' | 'show' 'applets' )
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
                    // RemusCli.g:40:4: 'show' 'servers'
                    {
                    match(input,11,FOLLOW_11_in_showCmd128); 
                    match(input,12,FOLLOW_12_in_showCmd130); 
                    cmd = new CLICommand(CLICommand.LIST); cmd.setSystem(CLICommand.SERVERS);

                    }
                    break;
                case 2 :
                    // RemusCli.g:41:4: 'show' 'pipelines'
                    {
                    match(input,11,FOLLOW_11_in_showCmd137); 
                    match(input,13,FOLLOW_13_in_showCmd139); 
                    cmd = new CLICommand(CLICommand.LIST); cmd.setSystem(CLICommand.PIPELINES);

                    }
                    break;
                case 3 :
                    // RemusCli.g:42:4: 'show' 'stacks'
                    {
                    match(input,11,FOLLOW_11_in_showCmd146); 
                    match(input,14,FOLLOW_14_in_showCmd148); 
                    cmd = new CLICommand(CLICommand.LIST); cmd.setSystem(CLICommand.STACKS);

                    }
                    break;
                case 4 :
                    // RemusCli.g:43:4: 'show' 'applets'
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
    // RemusCli.g:46:1: useCmd returns [CLICommand cmd] : 'use' pn= pipelineName ;
    public final CLICommand useCmd() throws RecognitionException {
        CLICommand cmd = null;

        String pn = null;


        try {
            // RemusCli.g:47:2: ( 'use' pn= pipelineName )
            // RemusCli.g:47:4: 'use' pn= pipelineName
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
    // RemusCli.g:51:1: selectCmd returns [CLICommand cmd] : 'select' f= selectionList 'from' s= stackName ( | 'where' c= conditionalList ) ( | 'limit' st= STRING ) ;
    public final CLICommand selectCmd() throws RecognitionException {
        CLICommand cmd = null;

        Token st=null;
        List<Selection> f = null;

        String s = null;

        List<Conditional> c = null;


        try {
            // RemusCli.g:52:2: ( 'select' f= selectionList 'from' s= stackName ( | 'where' c= conditionalList ) ( | 'limit' st= STRING ) )
            // RemusCli.g:52:4: 'select' f= selectionList 'from' s= stackName ( | 'where' c= conditionalList ) ( | 'limit' st= STRING )
            {
            match(input,17,FOLLOW_17_in_selectCmd194); 
            pushFollow(FOLLOW_selectionList_in_selectCmd198);
            f=selectionList();

            state._fsp--;

            match(input,18,FOLLOW_18_in_selectCmd200); 
            pushFollow(FOLLOW_stackName_in_selectCmd204);
            s=stackName();

            state._fsp--;

            cmd = new CLICommand(CLICommand.SELECT); cmd.setSelection(f); cmd.setStack(s);
            // RemusCli.g:53:2: ( | 'where' c= conditionalList )
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==EOF||LA3_0==20) ) {
                alt3=1;
            }
            else if ( (LA3_0==19) ) {
                alt3=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 3, 0, input);

                throw nvae;
            }
            switch (alt3) {
                case 1 :
                    // RemusCli.g:53:4: 
                    {
                    }
                    break;
                case 2 :
                    // RemusCli.g:53:6: 'where' c= conditionalList
                    {
                    match(input,19,FOLLOW_19_in_selectCmd213); 
                    pushFollow(FOLLOW_conditionalList_in_selectCmd217);
                    c=conditionalList();

                    state._fsp--;

                     cmd.setConditional(c); 

                    }
                    break;

            }

            // RemusCli.g:54:2: ( | 'limit' st= STRING )
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==EOF) ) {
                alt4=1;
            }
            else if ( (LA4_0==20) ) {
                alt4=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 4, 0, input);

                throw nvae;
            }
            switch (alt4) {
                case 1 :
                    // RemusCli.g:54:4: 
                    {
                    }
                    break;
                case 2 :
                    // RemusCli.g:54:6: 'limit' st= STRING
                    {
                    match(input,20,FOLLOW_20_in_selectCmd228); 
                    st=(Token)match(input,STRING,FOLLOW_STRING_in_selectCmd232); 
                     cmd.setLimit(Integer.parseInt(st.getText()));

                    }
                    break;

            }


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
    // RemusCli.g:58:1: dropCmd returns [CLICommand cmd] : 'drop' 'pipeline' n= pipelineName ;
    public final CLICommand dropCmd() throws RecognitionException {
        CLICommand cmd = null;

        String n = null;


        try {
            // RemusCli.g:59:2: ( 'drop' 'pipeline' n= pipelineName )
            // RemusCli.g:59:4: 'drop' 'pipeline' n= pipelineName
            {
            match(input,21,FOLLOW_21_in_dropCmd251); 
            match(input,22,FOLLOW_22_in_dropCmd253); 
            pushFollow(FOLLOW_pipelineName_in_dropCmd257);
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
    // RemusCli.g:62:1: loadCmd returns [CLICommand cmd] : 'load' 'pipeline' 'infile' pa= quoteStr 'into' p= pipelineName ;
    public final CLICommand loadCmd() throws RecognitionException {
        CLICommand cmd = null;

        String pa = null;

        String p = null;


        try {
            // RemusCli.g:63:2: ( 'load' 'pipeline' 'infile' pa= quoteStr 'into' p= pipelineName )
            // RemusCli.g:63:4: 'load' 'pipeline' 'infile' pa= quoteStr 'into' p= pipelineName
            {
            match(input,23,FOLLOW_23_in_loadCmd273); 
            match(input,22,FOLLOW_22_in_loadCmd275); 
            match(input,24,FOLLOW_24_in_loadCmd277); 
            pushFollow(FOLLOW_quoteStr_in_loadCmd281);
            pa=quoteStr();

            state._fsp--;

            match(input,25,FOLLOW_25_in_loadCmd283); 
            pushFollow(FOLLOW_pipelineName_in_loadCmd287);
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
    // RemusCli.g:66:1: stackName returns [String name] : (n= STRING ':' m= STRING | n1= STRING ':' m1= STRING ':' o1= STRING | '@' n1= STRING );
    public final String stackName() throws RecognitionException {
        String name = null;

        Token n=null;
        Token m=null;
        Token n1=null;
        Token m1=null;
        Token o1=null;

        try {
            // RemusCli.g:67:2: (n= STRING ':' m= STRING | n1= STRING ':' m1= STRING ':' o1= STRING | '@' n1= STRING )
            int alt5=3;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==STRING) ) {
                int LA5_1 = input.LA(2);

                if ( (LA5_1==26) ) {
                    int LA5_3 = input.LA(3);

                    if ( (LA5_3==STRING) ) {
                        int LA5_4 = input.LA(4);

                        if ( (LA5_4==26) ) {
                            alt5=2;
                        }
                        else if ( (LA5_4==EOF||(LA5_4>=19 && LA5_4<=20)) ) {
                            alt5=1;
                        }
                        else {
                            NoViableAltException nvae =
                                new NoViableAltException("", 5, 4, input);

                            throw nvae;
                        }
                    }
                    else {
                        NoViableAltException nvae =
                            new NoViableAltException("", 5, 3, input);

                        throw nvae;
                    }
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 5, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA5_0==27) ) {
                alt5=3;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 5, 0, input);

                throw nvae;
            }
            switch (alt5) {
                case 1 :
                    // RemusCli.g:67:4: n= STRING ':' m= STRING
                    {
                    n=(Token)match(input,STRING,FOLLOW_STRING_in_stackName305); 
                    match(input,26,FOLLOW_26_in_stackName307); 
                    m=(Token)match(input,STRING,FOLLOW_STRING_in_stackName311); 
                    name =n.getText() + ":" + m.getText();

                    }
                    break;
                case 2 :
                    // RemusCli.g:68:4: n1= STRING ':' m1= STRING ':' o1= STRING
                    {
                    n1=(Token)match(input,STRING,FOLLOW_STRING_in_stackName320); 
                    match(input,26,FOLLOW_26_in_stackName322); 
                    m1=(Token)match(input,STRING,FOLLOW_STRING_in_stackName326); 
                    match(input,26,FOLLOW_26_in_stackName328); 
                    o1=(Token)match(input,STRING,FOLLOW_STRING_in_stackName332); 
                    name =n1.getText() + ":" + m1.getText() + ":" + o1.getText();

                    }
                    break;
                case 3 :
                    // RemusCli.g:69:4: '@' n1= STRING
                    {
                    match(input,27,FOLLOW_27_in_stackName339); 
                    n1=(Token)match(input,STRING,FOLLOW_STRING_in_stackName343); 
                     name = "@" + n1.getText(); 

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
    // RemusCli.g:72:1: pipelineName returns [String name] : n= STRING ;
    public final String pipelineName() throws RecognitionException {
        String name = null;

        Token n=null;

        try {
            // RemusCli.g:73:2: (n= STRING )
            // RemusCli.g:73:4: n= STRING
            {
            n=(Token)match(input,STRING,FOLLOW_STRING_in_pipelineName361); 
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


    // $ANTLR start "selectionList"
    // RemusCli.g:76:1: selectionList returns [List<Selection> out=new LinkedList<Selection>();] : w= selection ( ',' w= selection )* ;
    public final List<Selection> selectionList() throws RecognitionException {
        List<Selection> out = new LinkedList<Selection>();;

        Selection w = null;


        try {
            // RemusCli.g:77:2: (w= selection ( ',' w= selection )* )
            // RemusCli.g:77:4: w= selection ( ',' w= selection )*
            {
            pushFollow(FOLLOW_selection_in_selectionList379);
            w=selection();

            state._fsp--;

            out.add(w);
            // RemusCli.g:77:31: ( ',' w= selection )*
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( (LA6_0==28) ) {
                    alt6=1;
                }


                switch (alt6) {
            	case 1 :
            	    // RemusCli.g:77:32: ',' w= selection
            	    {
            	    match(input,28,FOLLOW_28_in_selectionList384); 
            	    pushFollow(FOLLOW_selection_in_selectionList388);
            	    w=selection();

            	    state._fsp--;

            	    out.add(w);

            	    }
            	    break;

            	default :
            	    break loop6;
                }
            } while (true);


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return out;
    }
    // $ANTLR end "selectionList"


    // $ANTLR start "selection"
    // RemusCli.g:80:1: selection returns [Selection select] : (s= quoteStr | '*' | 'KEY' );
    public final Selection selection() throws RecognitionException {
        Selection select = null;

        String s = null;


        try {
            // RemusCli.g:81:2: (s= quoteStr | '*' | 'KEY' )
            int alt7=3;
            switch ( input.LA(1) ) {
            case QUOTESTR:
                {
                alt7=1;
                }
                break;
            case 29:
                {
                alt7=2;
                }
                break;
            case 30:
                {
                alt7=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 7, 0, input);

                throw nvae;
            }

            switch (alt7) {
                case 1 :
                    // RemusCli.g:81:4: s= quoteStr
                    {
                    pushFollow(FOLLOW_quoteStr_in_selection408);
                    s=quoteStr();

                    state._fsp--;

                    select =new Selection(s);

                    }
                    break;
                case 2 :
                    // RemusCli.g:82:4: '*'
                    {
                    match(input,29,FOLLOW_29_in_selection415); 
                    select =new Selection(Selection.ALL);

                    }
                    break;
                case 3 :
                    // RemusCli.g:83:4: 'KEY'
                    {
                    match(input,30,FOLLOW_30_in_selection422); 
                    select =new Selection(Selection.KEY);

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
        return select;
    }
    // $ANTLR end "selection"


    // $ANTLR start "conditionalList"
    // RemusCli.g:86:1: conditionalList returns [List<Conditional> out=new LinkedList<Conditional>();] : w= conditional ;
    public final List<Conditional> conditionalList() throws RecognitionException {
        List<Conditional> out = new LinkedList<Conditional>();;

        Conditional w = null;


        try {
            // RemusCli.g:87:2: (w= conditional )
            // RemusCli.g:87:4: w= conditional
            {
            pushFollow(FOLLOW_conditional_in_conditionalList440);
            w=conditional();

            state._fsp--;

            out.add(w);

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return out;
    }
    // $ANTLR end "conditionalList"


    // $ANTLR start "conditional"
    // RemusCli.g:91:1: conditional returns [Conditional cond] : ( 'KEY' '=' e= quoteStr | e1= quoteStr '=' e2= quoteStr );
    public final Conditional conditional() throws RecognitionException {
        Conditional cond = null;

        String e = null;

        String e1 = null;

        String e2 = null;


        try {
            // RemusCli.g:92:2: ( 'KEY' '=' e= quoteStr | e1= quoteStr '=' e2= quoteStr )
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0==30) ) {
                alt8=1;
            }
            else if ( (LA8_0==QUOTESTR) ) {
                alt8=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 8, 0, input);

                throw nvae;
            }
            switch (alt8) {
                case 1 :
                    // RemusCli.g:92:4: 'KEY' '=' e= quoteStr
                    {
                    match(input,30,FOLLOW_30_in_conditional457); 
                    match(input,31,FOLLOW_31_in_conditional459); 
                    pushFollow(FOLLOW_quoteStr_in_conditional463);
                    e=quoteStr();

                    state._fsp--;

                     
                    		cond =new Conditional(Conditional.EQUALS); 
                    		cond.setLeftType(Conditional.KEY); 
                    		cond.setRightType(Conditional.STRING);
                    		cond.setRight(e);
                    	

                    }
                    break;
                case 2 :
                    // RemusCli.g:98:4: e1= quoteStr '=' e2= quoteStr
                    {
                    pushFollow(FOLLOW_quoteStr_in_conditional472);
                    e1=quoteStr();

                    state._fsp--;

                    match(input,31,FOLLOW_31_in_conditional474); 
                    pushFollow(FOLLOW_quoteStr_in_conditional478);
                    e2=quoteStr();

                    state._fsp--;


                    		cond =new Conditional(Conditional.EQUALS); 
                    		cond.setLeftType(Conditional.FIELD); 
                    		cond.setLeft(e1);
                    		cond.setRightType(Conditional.STRING);
                    		cond.setRight(e2);
                    	

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
        return cond;
    }
    // $ANTLR end "conditional"


    // $ANTLR start "quoteStr"
    // RemusCli.g:108:1: quoteStr returns [String str] : s= QUOTESTR ;
    public final String quoteStr() throws RecognitionException {
        String str = null;

        Token s=null;

        try {
            // RemusCli.g:109:2: (s= QUOTESTR )
            // RemusCli.g:109:4: s= QUOTESTR
            {
            s=(Token)match(input,QUOTESTR,FOLLOW_QUOTESTR_in_quoteStr499); 
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
    public static final BitSet FOLLOW_17_in_selectCmd194 = new BitSet(new long[]{0x0000000060000020L});
    public static final BitSet FOLLOW_selectionList_in_selectCmd198 = new BitSet(new long[]{0x0000000000040000L});
    public static final BitSet FOLLOW_18_in_selectCmd200 = new BitSet(new long[]{0x0000000008000010L});
    public static final BitSet FOLLOW_stackName_in_selectCmd204 = new BitSet(new long[]{0x0000000000180002L});
    public static final BitSet FOLLOW_19_in_selectCmd213 = new BitSet(new long[]{0x0000000040000020L});
    public static final BitSet FOLLOW_conditionalList_in_selectCmd217 = new BitSet(new long[]{0x0000000000100002L});
    public static final BitSet FOLLOW_20_in_selectCmd228 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_STRING_in_selectCmd232 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_21_in_dropCmd251 = new BitSet(new long[]{0x0000000000400000L});
    public static final BitSet FOLLOW_22_in_dropCmd253 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_pipelineName_in_dropCmd257 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_23_in_loadCmd273 = new BitSet(new long[]{0x0000000000400000L});
    public static final BitSet FOLLOW_22_in_loadCmd275 = new BitSet(new long[]{0x0000000001000000L});
    public static final BitSet FOLLOW_24_in_loadCmd277 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_quoteStr_in_loadCmd281 = new BitSet(new long[]{0x0000000002000000L});
    public static final BitSet FOLLOW_25_in_loadCmd283 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_pipelineName_in_loadCmd287 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_stackName305 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_26_in_stackName307 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_STRING_in_stackName311 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_stackName320 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_26_in_stackName322 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_STRING_in_stackName326 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_26_in_stackName328 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_STRING_in_stackName332 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_27_in_stackName339 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_STRING_in_stackName343 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_pipelineName361 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_selection_in_selectionList379 = new BitSet(new long[]{0x0000000010000002L});
    public static final BitSet FOLLOW_28_in_selectionList384 = new BitSet(new long[]{0x0000000060000020L});
    public static final BitSet FOLLOW_selection_in_selectionList388 = new BitSet(new long[]{0x0000000010000002L});
    public static final BitSet FOLLOW_quoteStr_in_selection408 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_29_in_selection415 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_30_in_selection422 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_conditional_in_conditionalList440 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_30_in_conditional457 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_conditional459 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_quoteStr_in_conditional463 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_quoteStr_in_conditional472 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_conditional474 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_quoteStr_in_conditional478 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_QUOTESTR_in_quoteStr499 = new BitSet(new long[]{0x0000000000000002L});

}