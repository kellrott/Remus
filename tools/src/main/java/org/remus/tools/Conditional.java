package org.remus.tools;

import java.util.Map;

import org.remus.JSON;

public class Conditional {

	public static final int EQUALS = 1;

	public static final int KEY = 2;
	public static final int STRING = 3;
	public static final int FIELD = 4;

	public int condType;
	public int leftType;
	public int rightType;

	public String rightString;
	public String leftString;

	public Conditional(int t) {
		condType = t;
	}

	public void setLeftType(int lt) {
		leftType = lt;
	}

	public void setRightType(int rt) {
		rightType = rt;
	}

	public void setRight(String e) {
		rightString = e;
	}
	
	public void setLeft(String e) {
		leftString = e;
	}

	public String getRight(String key, Object val) {
		if (rightType == KEY) {
			return key;
		}
		if (rightType == STRING) {
			return rightString;
		}
		if (rightType == FIELD) {
			return JSON.dumps(((Map)val).get(rightString));
		}
		return null;
	}

	public String getLeft(String key, Object val) {
		if (leftType == KEY) {
			return key;
		}
		if (leftType == STRING) {
			return leftString;
		}
		if (leftType == FIELD) {
			return JSON.dumps(((Map)val).get(leftString));
		}
		return null;
	}
	
	public boolean evaluate(String key, Object val) {
		if (condType==EQUALS) {
			String right = getRight(key, val);
			String left  = getLeft(key, val);			
			if ( right != null && left != null && right.compareTo(left) == 0 ) {
				return true;
			}
		}
		return false;
	}
}
