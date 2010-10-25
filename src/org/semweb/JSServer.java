package org.semweb;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mozilla.javascript.*;


public class JSServer extends HttpServlet{

	Context cx;
	Scriptable scope;
	
	public JSServer() {
		cx = Context.enter();
		scope = cx.initStandardObjects();

	}
	
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException{
		
		String code ="var a=1;";
		Object result = cx.evaluateString(scope, code, "<code>", 1, null );
		
		res.getWriter().print( Context.toString(result) );
		
	}
}
