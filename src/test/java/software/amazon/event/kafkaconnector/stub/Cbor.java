/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.stub;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal CBOR (RFC 8949) codec for the eventsv2 test stub. Dependency-free so the stub can run in
 * a plain JRE container from compiled test classes. Decodes maps, arrays, text/byte strings
 * (definite and indefinite length), integers, floats, booleans, null, and skips tags; encodes maps,
 * lists, strings, byte arrays, integers, booleans, and null.
 */
final class Cbor {

  private static final Object BREAK = new Object();

  private Cbor() {}

  static Object decode(byte[] bytes) {
    var buffer = ByteBuffer.wrap(bytes);
    var value = read(buffer);
    if (value == BREAK) {
      throw new IllegalArgumentException("unexpected CBOR break");
    }
    return value;
  }

  static byte[] encode(Object value) {
    var out = new ByteArrayOutputStream();
    write(out, value);
    return out.toByteArray();
  }

  private static Object read(ByteBuffer in) {
    int initial = in.get() & 0xFF;
    int major = initial >>> 5;
    int info = initial & 0x1F;
    if (initial == 0xFF) {
      return BREAK;
    }
    switch (major) {
      case 0:
        return readLength(in, info);
      case 1:
        return -1 - readLength(in, info);
      case 2:
        return info == 31 ? readChunks(in) : readBytes(in, (int) readLength(in, info));
      case 3:
        return new String(
            info == 31 ? readChunks(in) : readBytes(in, (int) readLength(in, info)),
            StandardCharsets.UTF_8);
      case 4:
        {
          var list = new ArrayList<Object>();
          if (info == 31) {
            for (var item = read(in); item != BREAK; item = read(in)) {
              list.add(item);
            }
          } else {
            for (long i = readLength(in, info); i > 0; i--) {
              list.add(read(in));
            }
          }
          return list;
        }
      case 5:
        {
          var map = new LinkedHashMap<String, Object>();
          if (info == 31) {
            for (var key = read(in); key != BREAK; key = read(in)) {
              map.put(String.valueOf(key), read(in));
            }
          } else {
            for (long i = readLength(in, info); i > 0; i--) {
              map.put(String.valueOf(read(in)), read(in));
            }
          }
          return map;
        }
      case 6:
        readLength(in, info); // tag number, ignored
        return read(in);
      default:
        return readSimple(in, info);
    }
  }

  private static Object readSimple(ByteBuffer in, int info) {
    switch (info) {
      case 20:
        return Boolean.FALSE;
      case 21:
        return Boolean.TRUE;
      case 22:
      case 23:
        return null;
      case 25:
        return halfToDouble(in.getShort());
      case 26:
        return (double) in.getFloat();
      case 27:
        return in.getDouble();
      default:
        throw new IllegalArgumentException("unsupported CBOR simple value " + info);
    }
  }

  private static long readLength(ByteBuffer in, int info) {
    if (info < 24) {
      return info;
    }
    switch (info) {
      case 24:
        return in.get() & 0xFFL;
      case 25:
        return in.getShort() & 0xFFFFL;
      case 26:
        return in.getInt() & 0xFFFFFFFFL;
      case 27:
        return in.getLong();
      default:
        throw new IllegalArgumentException("unsupported CBOR length encoding " + info);
    }
  }

  private static byte[] readBytes(ByteBuffer in, int length) {
    var bytes = new byte[length];
    in.get(bytes);
    return bytes;
  }

  private static byte[] readChunks(ByteBuffer in) {
    var out = new ByteArrayOutputStream();
    for (var chunk = read(in); chunk != BREAK; chunk = read(in)) {
      out.writeBytes(
          chunk instanceof String s ? s.getBytes(StandardCharsets.UTF_8) : (byte[]) chunk);
    }
    return out.toByteArray();
  }

  private static double halfToDouble(short half) {
    int exp = (half >>> 10) & 0x1F;
    int mant = half & 0x3FF;
    double value =
        exp == 0
            ? mant * Math.pow(2, -24)
            : exp == 31
                ? (mant == 0 ? Double.POSITIVE_INFINITY : Double.NaN)
                : (mant + 1024) * Math.pow(2, exp - 25);
    return (half & 0x8000) != 0 ? -value : value;
  }

  private static void write(ByteArrayOutputStream out, Object value) {
    if (value == null) {
      out.write(0xF6);
    } else if (value instanceof Boolean b) {
      out.write(b ? 0xF5 : 0xF4);
    } else if (value instanceof Number n) {
      long v = n.longValue();
      if (v >= 0) {
        writeHead(out, 0, v);
      } else {
        writeHead(out, 1, -1 - v);
      }
    } else if (value instanceof byte[] bytes) {
      writeHead(out, 2, bytes.length);
      out.writeBytes(bytes);
    } else if (value instanceof String s) {
      var bytes = s.getBytes(StandardCharsets.UTF_8);
      writeHead(out, 3, bytes.length);
      out.writeBytes(bytes);
    } else if (value instanceof List<?> list) {
      writeHead(out, 4, list.size());
      list.forEach(item -> write(out, item));
    } else if (value instanceof Map<?, ?> map) {
      writeHead(out, 5, map.size());
      map.forEach(
          (k, v) -> {
            write(out, String.valueOf(k));
            write(out, v);
          });
    } else {
      throw new IllegalArgumentException("unsupported CBOR value type " + value.getClass());
    }
  }

  private static void writeHead(ByteArrayOutputStream out, int major, long length) {
    int prefix = major << 5;
    if (length < 24) {
      out.write(prefix | (int) length);
    } else if (length <= 0xFF) {
      out.write(prefix | 24);
      out.write((int) length);
    } else if (length <= 0xFFFF) {
      out.write(prefix | 25);
      out.writeBytes(ByteBuffer.allocate(2).putShort((short) length).array());
    } else if (length <= 0xFFFFFFFFL) {
      out.write(prefix | 26);
      out.writeBytes(ByteBuffer.allocate(4).putInt((int) length).array());
    } else {
      out.write(prefix | 27);
      out.writeBytes(ByteBuffer.allocate(8).putLong(length).array());
    }
  }
}
