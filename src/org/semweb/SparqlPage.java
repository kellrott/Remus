package org.semweb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.nativerdf.NativeStore;

public class SparqlPage extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3002607162096773136L;

	Repository repo;
	String dataDir;

	@Override
	public void init() throws ServletException {
		try {
			dataDir = getServletContext().getRealPath( "" ) + "/WEB-INF/rdfStore";
			repo = new SailRepository( new NativeStore( new File(dataDir) ) );
			repo.initialize();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	
	public void writeHTML( TupleQueryResult result, PrintWriter out ) throws QueryEvaluationException {
		List<String> colNames = result.getBindingNames();		
		out.print("<html><table><tr>");
		for ( String col : colNames ) {
			out.print("<td>" + col + "</td>");
		}
		out.print("</tr>");
		while ( result.hasNext() ) {
			BindingSet row = result.next();
			out.print("<tr>");
			for ( String col : colNames)  {
				out.print( "<td>" + row.getValue(col) + "</td>" );					
			}
			out.print("</tr>");
		}
		out.print("</table></html>");
	}
	
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException{
		String queryStr = req.getParameter("query");		
		if ( queryStr != null ) {
			try {
				RepositoryConnection conn = repo.getConnection();
				TupleQuery query = conn.prepareTupleQuery( QueryLanguage.SPARQL , queryStr);
				TupleQueryResult result = query.evaluate();
				writeHTML( result, res.getWriter() );
			} catch (RepositoryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MalformedQueryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (QueryEvaluationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			InputStream is = SparqlPage.class.getResourceAsStream("sparql.html");
			OutputStream os = res.getOutputStream();
			byte[] buffer = new byte[1024]; // Adjust if you want
			int bytesRead;
			while ((bytesRead = is.read(buffer)) != -1) {
				os.write(buffer, 0, bytesRead);
			}
			is.close();
		}
	}
}
