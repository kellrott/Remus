package org.semweb.work;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.semweb.app.PageManager;
import org.semweb.app.PageParser;
import org.semweb.app.PageRequest;
import org.semweb.app.SemWebApp;
import org.semweb.app.SemWebApplet;
import org.semweb.config.PluginManager;
import org.semweb.pluginterface.InterfaceBase;
import org.semweb.pluginterface.WriterInterface;

public class WorkManager {
	SemWebApp parent;
	File workdir;
	private PluginManager plugMan;

	public WorkManager(SemWebApp app, File workdir) {
		this.parent = app;	
		plugMan = new PluginManager( parent );	
		this.workdir = workdir;
	}

	List<WorkRequest> workList;

	public void scanWork() {
		workList = new LinkedList<WorkRequest>();
		scanWork( parent.getPageBase() );
	}

	public void scanWork(File file) {
		if ( file.isDirectory() ) {
			for ( File child : file.listFiles() ) {
				scanWork(child);
			}
		} else if ( file.getName().endsWith(".semweb") ) {
			PageManager pageMan = parent.getPageManager();
			PageRequest req = pageMan.openPage( 
					file.getAbsolutePath().replaceFirst( parent.getPageBase().getAbsolutePath(), "" ).replaceFirst(PageParser.PageExt + "$", "" ) );
			Map<String,SemWebApplet> applets = req.getApplets();
			for ( String appletID : applets.keySet() ) {
				workList.add( new WorkRequest(parent, applets.get(appletID) ) );				
			}
		}
	}

	public void scheduleWork() {
		for (int i =0; i < 10; i++) {
			for ( WorkRequest request : workList ) {
				SemWebApplet applet = request.getApplet();
				if ( applet.getInput() == null ) {
					InterfaceBase plug = plugMan.getPlugin( applet.getCode().lang );
					WriterInterface writer = (WriterInterface)plug;
					writer.prepWriter( applet.getCode().source );
					File outFile = new File( workdir, applet.getSelf().getURL() );
					System.out.println( outFile.getAbsolutePath() );
					System.out.println( writer.write(null) );
				}
			}
		}
	}



	public static void main(String []args) {
		SemWebApp app = new SemWebApp( new File(args[0]) );
		WorkManager wm = new WorkManager(app, new File(args[1]));
		wm.scanWork();
		wm.scheduleWork();

	}




}
