package org.semweb.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class PageRequest {

	PageRequest( PageManager parent, File sourceFile, String requestPage, int type, SemWebPage cachePage ) {
		this.sourceFile = sourceFile;
		this.type = type;
		this.parent = parent;
		this.cachedPage = cachePage;
	}
	
	public static final int STATIC=1;
	public static final int DYNAMIC=2;

	String requestPath;
	File sourceFile;
	int type;
	PageManager parent;
	SemWebPage cachedPage;
	
	Map<String,String> params;
	
	public InputStream open() {
		if ( cachedPage != null ) {
			return cachedPage.render(params);
		}
		if ( type == STATIC ) {
			try {
				return new FileInputStream(sourceFile);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if ( type == DYNAMIC ) {
			try {
				PageParser parser = new PageParser(parent);
				InputStream is = new FileInputStream( sourceFile );
				SemWebPage os = parser.parse( is, requestPath);
				is.close();
				return os.render(params);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		return null;
	}
	
	@Override
	public String toString() {
		return sourceFile.toString();
	}
}
