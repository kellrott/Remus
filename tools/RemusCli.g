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
;

quitCmd returns [CLICommand cmd]
	: 'quit' {$cmd = new CLICommand(CLICommand.QUIT);}
;


listCmd returns [CLICommand cmd]
	: 'list' {$cmd = new CLICommand(CLICommand.LIST);}
;


NUMBER	: (DIGIT)+ ;

WHITESPACE : ( '\t' | ' ' | '\r' | '\n'| '\u000C' )+ 	{ $channel = HIDDEN; } ;

fragment DIGIT	: '0'..'9' ;
