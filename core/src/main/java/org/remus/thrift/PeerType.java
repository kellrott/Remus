/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.remus.thrift;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum PeerType implements org.apache.thrift.TEnum {
  MANAGER(0),
  NAME_SERVER(1),
  DB_SERVER(2),
  ATTACH_SERVER(3),
  WORKER(4),
  WEB_SERVER(5);

  private final int value;

  private PeerType(int value) {
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
  public static PeerType findByValue(int value) { 
    switch (value) {
      case 0:
        return MANAGER;
      case 1:
        return NAME_SERVER;
      case 2:
        return DB_SERVER;
      case 3:
        return ATTACH_SERVER;
      case 4:
        return WORKER;
      case 5:
        return WEB_SERVER;
      default:
        return null;
    }
  }
}
