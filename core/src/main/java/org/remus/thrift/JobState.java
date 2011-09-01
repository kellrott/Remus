/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.remus.thrift;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum JobState implements org.apache.thrift.TEnum {
  QUEUED(0),
  WORKING(1),
  DONE(2),
  ERROR(3),
  UNKNOWN(4);

  private final int value;

  private JobState(int value) {
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
  public static JobState findByValue(int value) { 
    switch (value) {
      case 0:
        return QUEUED;
      case 1:
        return WORKING;
      case 2:
        return DONE;
      case 3:
        return ERROR;
      case 4:
        return UNKNOWN;
      default:
        return null;
    }
  }
}