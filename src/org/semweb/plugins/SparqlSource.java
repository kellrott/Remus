package org.semweb.plugins;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.rio.RDFFormat;
import org.semweb.config.PluginConfig;
import org.semweb.pluginterface.SplitCallback;
import org.semweb.pluginterface.SpliterInterface;


public class SparqlSource implements SpliterInterface {

	@Override
	public void prepSpliter(String config) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void split(InputStream input, SplitCallback callback) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void init(PluginConfig config) {
		// TODO Auto-generated method stub
		
	}

	/*
	RepoHandler repo;

	public SparqlSource() {
		
	}
	
	public void AddRDF( File rdfFile, String graph ) {
		try {
			FileInputStream fis = new FileInputStream( rdfFile );
			repo.add(fis, graph, RDFFormat.RDFXML, new URIImpl( graph ) );
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	public String link() {
		return "http://test.org/";	
	}

	public Object [] query(String queryStr) {
		try {
			List<Object> out = new LinkedList<Object>();
			TupleQueryResult result = repo.query(queryStr);
			List<String> colNames = result.getBindingNames();
			while ( result.hasNext() ) {
				BindingSet row = result.next();
				Map<String, Value> map = new HashMap<String,Value>();
				for ( String col : colNames)  {
					map.put(col, row.getValue(col) );					
				}
				out.add( map );
			}
			return out.toArray();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void init(ExtConfig config) throws InitException {
		String path = (String) config.get("endpoint");
		File dataDir = new File(path);
		repo = RepoHandler.getRepoHandler( dataDir.getAbsolutePath() );		
	}

*/
}
