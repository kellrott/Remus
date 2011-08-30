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
	| lc=listCmd {$cmd=lc;}
	| uc=useCmd  {$cmd=uc;}
;

quitCmd returns [CLICommand cmd]
	: 'quit' {$cmd = new CLICommand(CLICommand.QUIT);}
;


listCmd returns [CLICommand cmd]
	: 'list' 'servers' {$cmd = new CLICommand(CLICommand.LIST); $cmd.setSystem(CLICommand.SERVERS);}
	| 'list' 'pipelines' {$cmd = new CLICommand(CLICommand.LIST); $cmd.setSystem(CLICommand.PIPELINES);}
	| 'list' 'instances' {$cmd = new CLICommand(CLICommand.LIST); $cmd.setSystem(CLICommand.INSTANCES);}
	| 'list' 'applets' {$cmd = new CLICommand(CLICommand.LIST); $cmd.setSystem(CLICommand.APPLETS);}
;

useCmd returns [CLICommand cmd]
	: 'use' pn=pipelineName {$cmd = new CLICommand(CLICommand.USE); $cmd.setPipeline(pn);}
;

pipelineName returns [String name]
	: n=STRING {$name=n.getText();}
;


STRING : ('a'..'z'|'A'..'Z'|'0'..'9'|'_')+ ;

SEMICOLON : ';';

WHITESPACE : ( '\t' | ' ' | '\r' | '\n'| '\u000C' )+ 	{ $channel = HIDDEN; } ;

fragment DIGIT	: '0'..'9' ;
