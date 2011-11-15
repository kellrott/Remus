grammar RemusCli;

options {
  language = Java;
}

@header {
package org.remus.tools.antlr;

import org.remus.tools.CLICommand;
import org.remus.tools.Selection;
import org.remus.tools.Conditional;
import java.util.LinkedList;
} 

@lexer::header {
package org.remus.tools.antlr;

import org.remus.tools.CLICommand;
import org.remus.tools.Selection;
import org.remus.tools.Conditional;
import java.util.LinkedList;
}

cmd  returns [CLICommand cmd] 
	: qc=quitCmd {$cmd=qc;}
	| lc=showCmd {$cmd=lc;}
	| uc=useCmd  {$cmd=uc;}
	| sc=selectCmd {$cmd=sc;}
	| delc=deleteCmd {$cmd=delc;}
	| dc=dropCmd {$cmd=dc;}
	| lc=loadCmd {$cmd=lc;}
;

quitCmd returns [CLICommand cmd]
	: 'quit' {$cmd = new CLICommand(CLICommand.QUIT);}
;


showCmd returns [CLICommand cmd]
	: 'show' 'servers' {$cmd = new CLICommand(CLICommand.LIST); $cmd.setSystem(CLICommand.SERVERS);}
	| 'show' 'pipelines' {$cmd = new CLICommand(CLICommand.LIST); $cmd.setSystem(CLICommand.PIPELINES);}
	| 'show' 'tables' {$cmd = new CLICommand(CLICommand.LIST); $cmd.setSystem(CLICommand.STACKS);}
	| 'show' 'applets' {$cmd = new CLICommand(CLICommand.LIST); $cmd.setSystem(CLICommand.APPLETS);}
;

useCmd returns [CLICommand cmd]
	: 'use' pn=pipelineName {$cmd = new CLICommand(CLICommand.USE); $cmd.setPipeline(pn);}
;


selectCmd returns [CLICommand cmd]
	: 'select' f=selectionList 'from' s=stackName {$cmd = new CLICommand(CLICommand.SELECT); $cmd.setSelection(f); $cmd.setStack(s);}
	( | 'where' c=conditionalList { $cmd.setConditional(c); } )
	( | 'limit' st=STRING { $cmd.setLimit(Integer.parseInt(st.getText()));} )
;

deleteCmd returns [CLICommand cmd]
	: 'delete' 'from' s=stackName {$cmd = new CLICommand(CLICommand.DELETE); $cmd.setStack(s);}
	( | 'where' c=conditionalList { $cmd.setConditional(c); } )
;

dropCmd returns [CLICommand cmd]
	: 'drop' 'pipeline' n=pipelineName {$cmd = new CLICommand(CLICommand.DROP); $cmd.setPipeline(n);}
;

loadCmd returns [CLICommand cmd]
	: 'load' 'pipeline' 'infile' pa=quoteStr 'into' p=pipelineName {$cmd = new CLICommand(CLICommand.LOAD); $cmd.setPipeline(p); $cmd.setPath(pa);}
;

stackName returns [String name]
	: n=STRING ':' m=STRING {$name=n.getText() + ":" + m.getText();}
	| n1=STRING ':' m1=STRING ':' o1=STRING {$name=n1.getText() + ":" + m1.getText() + ":" + o1.getText();}
	| '@' n1=STRING { $name= "@" + n1.getText(); }
;

pipelineName returns [String name]
	: n=STRING {$name=n.getText();}
;

selectionList returns [List<Selection> out=new LinkedList<Selection>();]
	: w=selection {$out.add(w);} (',' w=selection {$out.add(w);})*
;

selection returns [Selection select]
	: s=quoteStr {$select=new Selection(s);}
	| '*' {$select=new Selection(Selection.ALL);}
	| 'KEY' {$select=new Selection(Selection.KEY);}
;

conditionalList returns [List<Conditional> out=new LinkedList<Conditional>();]
	: w=conditional {$out.add(w);}
;


conditional returns [Conditional cond]
	: 'KEY' op=operation e=quoteStr { 
		$cond=new Conditional(op); 
		$cond.setLeftType(Conditional.KEY); 
		$cond.setRightType(Conditional.STRING);
		$cond.setRight(e);
	}
	| e1=quoteStr op=operation e2=quoteStr {
		$cond=new Conditional(op); 
		$cond.setLeftType(Conditional.FIELD); 
		$cond.setLeft(e1);
		$cond.setRightType(Conditional.STRING);
		$cond.setRight(e2);
	}
;


operation returns [int op]
	: '=' {$op=Conditional.EQUAL;}
	| '!=' {$op=Conditional.NOT_EQUAL;}
	| 'like' {$op=Conditional.LIKE;}
	| 'not' 'like' {$op=Conditional.NOT_LIKE;}
;
	

quoteStr returns [String str]
	: s=QUOTESTR {String t=s.getText(); $str=t.substring(1,t.length()-1);}
;

STRING : ('a'..'z'|'A'..'Z'|'0'..'9'|'_'|'\.')+ ;


QUOTE : '"';

QUOTESTR : QUOTE (options {greedy=false;} : .)* QUOTE;

SEMICOLON : ';';

WHITESPACE : ( '\t' | ' ' | '\r' | '\n'| '\u000C' )+ 	{ $channel = HIDDEN; } ;

fragment DIGIT	: '0'..'9' ;
