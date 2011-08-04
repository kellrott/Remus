/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.remus.thrift;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum WorkMode implements org.apache.thrift.TEnum {
  SPLIT(0),
  MAP(1),
  REDUCE(2),
  PIPE(3),
  MATCH(4),
  MERGE(5);

  private final int value;

  private WorkMode(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static WorkMode findByValue(int value) { 
    switch (value) {
      case 0:
        return SPLIT;
      case 1:
        return MAP;
      case 2:
        return REDUCE;
      case 3:
        return PIPE;
      case 4:
        return MATCH;
      case 5:
        return MERGE;
      default:
        return null;
    }
  }
}