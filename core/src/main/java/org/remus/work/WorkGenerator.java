package org.remus.work;


import org.remus.RemusDB;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;

/**
 * Base interface for applet work generators.
 * These are the classes that write jobIDs into the
 * database in the <applet>/@work stack
 * @see org.remus.work.MapGenerator
 * @see org.remus.work.ReduceGenerator
 * @see org.remus.work.PipeGenerator
 * @see org.remus.work.MatchGenerator
 * @see org.remus.work.MergeGenerator
 * @see org.remus.work.SplitGenerator
 * @author kellrott
 *
 */

public interface WorkGenerator {
	public void writeWorkTable(RemusPipeline pipeline, RemusApplet applet, RemusInstance instance, RemusDB datastore);

	public void finalizeWork(RemusPipeline pipeline, RemusApplet applet,
			RemusInstance instance, RemusDB datastore);
}
