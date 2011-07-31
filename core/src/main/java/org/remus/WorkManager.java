package org.remus;

import java.util.Collection;
import java.util.Map;

import org.remus.WorkStatus;

public interface WorkManager {

	Map getParams();

	WorkStatus requestWorkStack(WorkAgent jobTreeManager, Collection<String> codeTypes);

	void returnWorkStack(WorkStatus workStack);

	void completeWorkStack(WorkStatus activeStack);

}
