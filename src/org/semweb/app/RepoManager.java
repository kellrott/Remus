package org.semweb.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

public class RepoManager {	

	String originPath, workPath, srcPath;
	File originFile, workFile, srcFile;
	
	public RepoManager(String originRepo, String workDir) {
		this.originPath = originRepo;
		this.workPath = workDir;
		this.srcFile = (new File(workDir, "src"));
		this.srcPath = this.srcFile.getAbsolutePath();
		this.originFile = new File(this.originPath);
		this.workFile = new File(this.workPath);
	}

	public void syncWork() {
		if ( !workFile.exists() ) {
			String cmd = "/usr/bin/git clone " + originPath + " " + workPath;
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
			//TODO:Work on branch selection
			List<String> cmd = Arrays.asList( new String [] {"/usr/bin/git", "pull", originPath, "master" } );
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
	
	//git show-ref master
	public String getSHA1(String branch) {
		List<String> cmd = Arrays.asList( new String [] {"/usr/bin/git", "show-ref", branch } );
		ProcessBuilder pb = new ProcessBuilder( cmd );
		pb.directory( workFile );pb.directory( workFile );
		try {
			Process pr = pb.start();
			//BufferedInputStream bis = new BufferedInputStream( pr.getInputStream() );
			BufferedReader reader = new BufferedReader( new InputStreamReader( pr.getInputStream() ) );
			String line = reader.readLine();
			pr.waitFor();
			String []tmp = line.split(" ");
			return tmp[0];
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public File getBase() {
		return workFile;
	}
		
}
