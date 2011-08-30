grammar RemusCli;

options {
  language = Java;
}

@header {
package org.remus.tools.antlr;

import org.remus.tools.CLICommand;
} 

@lexer::header {
package org.remus.tools.antlr;

import org.remus.tools.CLICommand;
}

cmd  returns [CLICommand cmd] 
	: qc=quitCmd {$cmd=qc;}
	| lc=showCmd {$cmd=lc;}
	| uc=useCmd  {$cmd=uc;}
	| sc=selectCmd {$cmd=sc;}
	| dc=dropCmd {$cmd=dc;}
	| lc=loadCmd {$cmd=lc;}
;

quitCmd returns [CLICommand cmd]
	: 'quit' {$cmd = new CLICommand(CLICommand.QUIT);}
;


showCmd returns [CLICommand cmd]
	: 'show' 'servers' {$cmd = new CLICommand(CLICommand.LIST); $cmd.setSystem(CLICommand.SERVERS);}
	| 'show' 'pipelines' {$cmd = new CLICommand(CLICommand.LIST); $cmd.setSystem(CLICommand.PIPELINES);}
	| 'show' 'stacks' {$cmd = new CLICommand(CLICommand.LIST); $cmd.setSystem(CLICommand.STACKS);}
	| 'show' 'applets' {$cmd = new CLICommand(CLICommand.LIST); $cmd.setSystem(CLICommand.APPLETS);}
;

useCmd returns [CLICommand cmd]
	: 'use' pn=pipelineName {$cmd = new CLICommand(CLICommand.USE); $cmd.setPipeline(pn);}
;


selectCmd returns [CLICommand cmd]
	: 'select' f=fieldSelect 'from' s=stackName {$cmd = new CLICommand(CLICommand.SELECT); $cmd.setField(f); $cmd.setStack(s);}
;


dropCmd returns [CLICommand cmd]
	: 'drop' 'pipeline' n=pipelineName {$cmd = new CLICommand(CLICommand.DROP); $cmd.setPipeline(n);}
;

loadCmd returns [CLICommand cmd]
	: 'load' 'pipeline' 'infile' pa=quoteStr 'into' p=pipelineName {$cmd = new CLICommand(CLICommand.LOAD); $cmd.setPipeline(p); $cmd.setPath(pa);}
;

stackName returns [String name]
	: n=STRING ':' m=STRING {$name=n.getText() + ":" + m.getText();}
;

pipelineName returns [String name]
	: n=STRING {$name=n.getText();}
;

fieldSelect returns [String field]
	: n=STRING {$field=n.getText();}
	| '*' {$field="*";}
;

quoteStr returns [String str]
	: s=QUOTESTR {String t=s.getText(); $str=t.substring(1,t.length()-1);}
;

STRING : ('a'..'z'|'A'..'Z'|'0'..'9'|'_')+ ;


QUOTE : '"';

QUOTESTR : QUOTE (options {greedy=false;} : .)* QUOTE;

SEMICOLON : ';';

WHITESPACE : ( '\t' | ' ' | '\r' | '\n'| '\u000C' )+ 	{ $channel = HIDDEN; } ;

fragment DIGIT	: '0'..'9' ;
