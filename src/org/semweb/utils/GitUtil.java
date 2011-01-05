package org.semweb.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class GitUtil {

	String srcPath, workPath;
	File srcFile, workFile;
	
	public GitUtil(String srcRepo, String workRepo) {
		this.srcPath = srcRepo;
		this.workPath = workRepo;
		this.srcFile = new File(this.srcPath);
		this.workFile = new File(this.workPath);
	}

	public void syncWork() {
		if ( !workFile.exists() ) {
			String cmd = "/usr/bin/git clone " + srcPath + " " + workPath;
			Runtime run = Runtime.getRuntime();
			try {
				System.out.println( cmd );
				Process pr = run.exec(cmd);				
				pr.waitFor();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			List<String> cmd = new ArrayList<String>();
			cmd.add("/usr/bin/git");
			cmd.add("pull");
			cmd.add(srcPath);
			ProcessBuilder pb = new ProcessBuilder( cmd );
			pb.directory( workFile );
			try {
				Process pr = pb.start();
				pr.waitFor();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
	}
	
	
	
	
	public static void main(String args[]) {
		GitUtil git = new GitUtil("/opt/webapps/tcga", "/tmp/tcga_work");
		git.syncWork();
	}
	
}
