package org.semweb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.nativerdf.NativeStore;

public class RepoHandler {

	final static Map<String,RepoHandler> repoMap = new HashMap<String,RepoHandler>();

	static RepoHandler getRepoHandler(String dataDir) {
		if ( !repoMap.containsKey(dataDir) ) {
			RepoHandler repo = new RepoHandler( dataDir );
			repoMap.put(dataDir, repo);
		}
		return repoMap.get(dataDir);
	}

	private Repository repo;
	private RepoHandler(String dataDir) {	
		try {
			repo = new SailRepository( new NativeStore( new File(dataDir) ) );
			repo.initialize();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public TupleQueryResult query(String queryStr) {
		try {
			synchronized ( repo ) {
				RepositoryConnection conn = repo.getConnection();
				TupleQuery query = conn.prepareTupleQuery( QueryLanguage.SPARQL , queryStr);
				TupleQueryResult result = query.evaluate();
				return result;				
			}
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
	public void add(FileInputStream fis, String graph, RDFFormat rdfxml, URIImpl uriImpl) {
		try{
			synchronized ( repo ) {
				RepositoryConnection conn = repo.getConnection();
				conn.add(fis, graph, RDFFormat.RDFXML, new URIImpl( graph ) );
				conn.commit();
			}
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
}
