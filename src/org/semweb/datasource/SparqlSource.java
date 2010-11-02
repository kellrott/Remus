package org.semweb.datasource;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.http.HTTPRepository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.nativerdf.NativeStore;
import org.semweb.config.ConfigMap;


public class SparqlSource extends DataSource {

	Repository repo;

	public void AddRDF( File rdfFile, String graph ) {
		try {
			RepositoryConnection conn = repo.getConnection();
			FileInputStream fis = new FileInputStream( rdfFile );
			conn.add(fis, graph, RDFFormat.RDFXML, new URIImpl( graph ) );
			conn.commit();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RDFParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
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

			RepositoryConnection conn = repo.getConnection();
			//System.err.println( queryStr );
			TupleQuery query = conn.prepareTupleQuery( QueryLanguage.SPARQL , queryStr);
			TupleQueryResult result = query.evaluate();
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
		return null;
	}

	@Override
	public void init(ConfigMap configMap) throws InitException {		
		repo = new HTTPRepository( (String) configMap.get("endpoint") );
		/*
		File dataDir = new File(path);
		//if ( !dataDir.exists() ) {
		repo = new SailRepository( new NativeStore(dataDir) );
		try {
			repo.initialize();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//} else {
		//	repo = new SailRepository( new NativeStore(dataDir) );
		//}
		 		
		 */
	}

}
