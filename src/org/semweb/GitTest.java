package org.semweb;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.eclipse.jgit.*;
import org.eclipse.jgit.storage.file.FileRepository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LogCommand;
import org.eclipse.jgit.api.errors.ConcurrentRefUpdateException;
import org.eclipse.jgit.api.errors.JGitInternalException;
import org.eclipse.jgit.api.errors.NoHeadException;
import org.eclipse.jgit.api.errors.NoMessageException;
import org.eclipse.jgit.api.errors.WrongRepositoryStateException;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.RepositoryBuilder;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;


public class GitTest {

	public static void main(String []args) throws IOException, NoHeadException, NoMessageException, ConcurrentRefUpdateException, JGitInternalException, WrongRepositoryStateException {
		
		String baseDir = "test_git/.git";
		
		File baseFile = new File( baseDir );
		
		//if ( !baseFile.exists() )
		//	baseFile.mkdirs();
		
		/*
		FileRepositoryBuilder builder = new FileRepositoryBuilder();		
		FileRepository repository = builder.setGitDir( baseFile )
		.readEnvironment() // scan environment GIT_* variables
		.findGitDir() // scan up the file system tree
		.build();
		*/
		//if ( repository.isBare() )
		//	repository.create();

		//Git git = new Git(repository);
		
		Git git = new Git( new FileRepository(baseDir) );

		LogCommand log = git.log();
		
		
		Repository repository = git.getRepository();
		
		Ref HEAD = repository.getRef("refs/heads/master");
		System.out.println( HEAD );
		
		RevWalk walk = new RevWalk( repository );
		System.out.println( walk.lookupTree(HEAD.getObjectId()) );

		//while (walk != null ) {
		System.out.println( walk );
		System.out.println( walk.next() );
		for ( RevCommit i : walk ) {
			System.out.println(i);
		}

		//CommitCommand commit = git.commit();
		//commit.setMessage("initial commit").call();

	}
	
}
