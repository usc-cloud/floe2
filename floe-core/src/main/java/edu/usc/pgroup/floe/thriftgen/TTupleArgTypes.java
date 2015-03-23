/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package edu.usc.pgroup.floe.thriftgen;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum TTupleArgTypes implements org.apache.thrift.TEnum {
  i32_arg(0),
  i64_arg(1),
  string_arg(2),
  binary_argT(3);

  private final int value;

  private TTupleArgTypes(int value) {
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
  public static TTupleArgTypes findByValue(int value) { 
    switch (value) {
      case 0:
        return i32_arg;
      case 1:
        return i64_arg;
      case 2:
        return string_arg;
      case 3:
        return binary_argT;
      default:
        return null;
    }
  }
}
