/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.remusNet.thrift;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkDesc implements org.apache.thrift.TBase<WorkDesc, WorkDesc._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("WorkDesc");

  private static final org.apache.thrift.protocol.TField LANG_FIELD_DESC = new org.apache.thrift.protocol.TField("lang", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField MODE_FIELD_DESC = new org.apache.thrift.protocol.TField("mode", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField INPUT_FIELD_DESC = new org.apache.thrift.protocol.TField("input", org.apache.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.thrift.protocol.TField OUTPUT_FIELD_DESC = new org.apache.thrift.protocol.TField("output", org.apache.thrift.protocol.TType.STRUCT, (short)4);

  public String lang;
  /**
   * 
   * @see WorkMode
   */
  public WorkMode mode;
  public StackRef input;
  public StackRef output;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    LANG((short)1, "lang"),
    /**
     * 
     * @see WorkMode
     */
    MODE((short)2, "mode"),
    INPUT((short)3, "input"),
    OUTPUT((short)4, "output");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // LANG
          return LANG;
        case 2: // MODE
          return MODE;
        case 3: // INPUT
          return INPUT;
        case 4: // OUTPUT
          return OUTPUT;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments

  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.LANG, new org.apache.thrift.meta_data.FieldMetaData("lang", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.MODE, new org.apache.thrift.meta_data.FieldMetaData("mode", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, WorkMode.class)));
    tmpMap.put(_Fields.INPUT, new org.apache.thrift.meta_data.FieldMetaData("input", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, StackRef.class)));
    tmpMap.put(_Fields.OUTPUT, new org.apache.thrift.meta_data.FieldMetaData("output", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, StackRef.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(WorkDesc.class, metaDataMap);
  }

  public WorkDesc() {
  }

  public WorkDesc(
    String lang,
    WorkMode mode,
    StackRef input,
    StackRef output)
  {
    this();
    this.lang = lang;
    this.mode = mode;
    this.input = input;
    this.output = output;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public WorkDesc(WorkDesc other) {
    if (other.isSetLang()) {
      this.lang = other.lang;
    }
    if (other.isSetMode()) {
      this.mode = other.mode;
    }
    if (other.isSetInput()) {
      this.input = new StackRef(other.input);
    }
    if (other.isSetOutput()) {
      this.output = new StackRef(other.output);
    }
  }

  public WorkDesc deepCopy() {
    return new WorkDesc(this);
  }

  @Override
  public void clear() {
    this.lang = null;
    this.mode = null;
    this.input = null;
    this.output = null;
  }

  public String getLang() {
    return this.lang;
  }

  public WorkDesc setLang(String lang) {
    this.lang = lang;
    return this;
  }

  public void unsetLang() {
    this.lang = null;
  }

  /** Returns true if field lang is set (has been assigned a value) and false otherwise */
  public boolean isSetLang() {
    return this.lang != null;
  }

  public void setLangIsSet(boolean value) {
    if (!value) {
      this.lang = null;
    }
  }

  /**
   * 
   * @see WorkMode
   */
  public WorkMode getMode() {
    return this.mode;
  }

  /**
   * 
   * @see WorkMode
   */
  public WorkDesc setMode(WorkMode mode) {
    this.mode = mode;
    return this;
  }

  public void unsetMode() {
    this.mode = null;
  }

  /** Returns true if field mode is set (has been assigned a value) and false otherwise */
  public boolean isSetMode() {
    return this.mode != null;
  }

  public void setModeIsSet(boolean value) {
    if (!value) {
      this.mode = null;
    }
  }

  public StackRef getInput() {
    return this.input;
  }

  public WorkDesc setInput(StackRef input) {
    this.input = input;
    return this;
  }

  public void unsetInput() {
    this.input = null;
  }

  /** Returns true if field input is set (has been assigned a value) and false otherwise */
  public boolean isSetInput() {
    return this.input != null;
  }

  public void setInputIsSet(boolean value) {
    if (!value) {
      this.input = null;
    }
  }

  public StackRef getOutput() {
    return this.output;
  }

  public WorkDesc setOutput(StackRef output) {
    this.output = output;
    return this;
  }

  public void unsetOutput() {
    this.output = null;
  }

  /** Returns true if field output is set (has been assigned a value) and false otherwise */
  public boolean isSetOutput() {
    return this.output != null;
  }

  public void setOutputIsSet(boolean value) {
    if (!value) {
      this.output = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case LANG:
      if (value == null) {
        unsetLang();
      } else {
        setLang((String)value);
      }
      break;

    case MODE:
      if (value == null) {
        unsetMode();
      } else {
        setMode((WorkMode)value);
      }
      break;

    case INPUT:
      if (value == null) {
        unsetInput();
      } else {
        setInput((StackRef)value);
      }
      break;

    case OUTPUT:
      if (value == null) {
        unsetOutput();
      } else {
        setOutput((StackRef)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case LANG:
      return getLang();

    case MODE:
      return getMode();

    case INPUT:
      return getInput();

    case OUTPUT:
      return getOutput();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case LANG:
      return isSetLang();
    case MODE:
      return isSetMode();
    case INPUT:
      return isSetInput();
    case OUTPUT:
      return isSetOutput();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof WorkDesc)
      return this.equals((WorkDesc)that);
    return false;
  }

  public boolean equals(WorkDesc that) {
    if (that == null)
      return false;

    boolean this_present_lang = true && this.isSetLang();
    boolean that_present_lang = true && that.isSetLang();
    if (this_present_lang || that_present_lang) {
      if (!(this_present_lang && that_present_lang))
        return false;
      if (!this.lang.equals(that.lang))
        return false;
    }

    boolean this_present_mode = true && this.isSetMode();
    boolean that_present_mode = true && that.isSetMode();
    if (this_present_mode || that_present_mode) {
      if (!(this_present_mode && that_present_mode))
        return false;
      if (!this.mode.equals(that.mode))
        return false;
    }

    boolean this_present_input = true && this.isSetInput();
    boolean that_present_input = true && that.isSetInput();
    if (this_present_input || that_present_input) {
      if (!(this_present_input && that_present_input))
        return false;
      if (!this.input.equals(that.input))
        return false;
    }

    boolean this_present_output = true && this.isSetOutput();
    boolean that_present_output = true && that.isSetOutput();
    if (this_present_output || that_present_output) {
      if (!(this_present_output && that_present_output))
        return false;
      if (!this.output.equals(that.output))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(WorkDesc other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    WorkDesc typedOther = (WorkDesc)other;

    lastComparison = Boolean.valueOf(isSetLang()).compareTo(typedOther.isSetLang());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLang()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.lang, typedOther.lang);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMode()).compareTo(typedOther.isSetMode());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMode()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.mode, typedOther.mode);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetInput()).compareTo(typedOther.isSetInput());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetInput()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.input, typedOther.input);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetOutput()).compareTo(typedOther.isSetOutput());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOutput()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.output, typedOther.output);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    org.apache.thrift.protocol.TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == org.apache.thrift.protocol.TType.STOP) { 
        break;
      }
      switch (field.id) {
        case 1: // LANG
          if (field.type == org.apache.thrift.protocol.TType.STRING) {
            this.lang = iprot.readString();
          } else { 
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // MODE
          if (field.type == org.apache.thrift.protocol.TType.I32) {
            this.mode = WorkMode.findByValue(iprot.readI32());
          } else { 
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 3: // INPUT
          if (field.type == org.apache.thrift.protocol.TType.STRUCT) {
            this.input = new StackRef();
            this.input.read(iprot);
          } else { 
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 4: // OUTPUT
          if (field.type == org.apache.thrift.protocol.TType.STRUCT) {
            this.output = new StackRef();
            this.output.read(iprot);
          } else { 
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();

    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.lang != null) {
      oprot.writeFieldBegin(LANG_FIELD_DESC);
      oprot.writeString(this.lang);
      oprot.writeFieldEnd();
    }
    if (this.mode != null) {
      oprot.writeFieldBegin(MODE_FIELD_DESC);
      oprot.writeI32(this.mode.getValue());
      oprot.writeFieldEnd();
    }
    if (this.input != null) {
      oprot.writeFieldBegin(INPUT_FIELD_DESC);
      this.input.write(oprot);
      oprot.writeFieldEnd();
    }
    if (this.output != null) {
      oprot.writeFieldBegin(OUTPUT_FIELD_DESC);
      this.output.write(oprot);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("WorkDesc(");
    boolean first = true;

    sb.append("lang:");
    if (this.lang == null) {
      sb.append("null");
    } else {
      sb.append(this.lang);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("mode:");
    if (this.mode == null) {
      sb.append("null");
    } else {
      sb.append(this.mode);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("input:");
    if (this.input == null) {
      sb.append("null");
    } else {
      sb.append(this.input);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("output:");
    if (this.output == null) {
      sb.append("null");
    } else {
      sb.append(this.output);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (lang == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'lang' was not present! Struct: " + toString());
    }
    if (mode == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'mode' was not present! Struct: " + toString());
    }
    if (input == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'input' was not present! Struct: " + toString());
    }
    if (output == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'output' was not present! Struct: " + toString());
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

}

