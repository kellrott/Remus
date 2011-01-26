package org.remus;


public class RemusApplet {
	public RemusApplet(InputReference self, InputReference input, CodeFragment code) {
		this.self = self;
		this.input = input;
		this.code = code;
	}
	
	public CodeFragment getCode() {
		return code;
	}
	public InputReference getInput() {
		return input;
	}
	public InputReference getSelf() {
		return self;
	}
	
	InputReference input, self;
	CodeFragment code;

}
