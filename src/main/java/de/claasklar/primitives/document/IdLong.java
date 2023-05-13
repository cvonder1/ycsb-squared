package de.claasklar.primitives.document;

public record IdLong(long id) {
  public Id toId() {
    var hash = IdLong.fnvhash64(id);
    byte[] result = new byte[12];
    for (int i = Long.BYTES - 1; i >= 0; i--) {
      result[i] = (byte) (hash & 0xFF);
      hash >>= Byte.SIZE;
    }
    return new Id(result);
  }

  /**
   * Copyright (c) 2010 Yahoo! Inc., 2016 YCSB contributors. All rights reserved.
   *
   * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
   * except in compliance with the License. You may obtain a copy of the License at
   *
   * <p>http://www.apache.org/licenses/LICENSE-2.0
   *
   * <p>Unless required by applicable law or agreed to in writing, software distributed under the
   * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
   * either express or implied. See the License for the specific language governing permissions and
   * limitations under the License. See accompanying LICENSE file.
   */
  public static final long FNV_OFFSET_BASIS_64 = 0xCBF29CE484222325L;

  public static final long FNV_PRIME_64 = 1099511628211L;

  /**
   * 64 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
   *
   * @param val The value to hash.
   * @return The hash value
   */
  private static long fnvhash64(long val) {
    // from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
    long hashval = FNV_OFFSET_BASIS_64;

    for (int i = 0; i < 8; i++) {
      long octet = val & 0x00ff;
      val = val >> 8;

      hashval = hashval ^ octet;
      hashval = hashval * FNV_PRIME_64;
      // hashval = hashval ^ octet;
    }
    return Math.abs(hashval);
  }
}
