package org.semweb.config;


import org.semweb.app.SemWebApp;
import org.semweb.template.TemplateInterface;

public class TemplateManager {

	TemplateInterface templateInterface;
	
	public TemplateManager(SemWebApp app) {
		try {
			String templateClass = app.templatingClass;
			//System.out.println("LOADING: " + templateClass );
			System.out.flush();
			Class<?> c = Class.forName(templateClass);
			templateInterface = (TemplateInterface) c.newInstance();				
			templateInterface.init();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public TemplateInterface getTemplateInterface() {
		return templateInterface;
	}
	
}

