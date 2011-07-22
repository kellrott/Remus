package org.remus;

import java.util.Map;

public interface RemusPipeline {

	String getID();

	RemusApplet getApplet(String name);

	RemusInstance handleSubmission(String key, Map value);

}
