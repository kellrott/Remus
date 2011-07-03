package org.remus.work;


import org.remus.RemusInstance;

/**
 * Base interface for applet work generators.
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
	public void writeWorkTable(RemusApplet applet, RemusInstance instance);
}
