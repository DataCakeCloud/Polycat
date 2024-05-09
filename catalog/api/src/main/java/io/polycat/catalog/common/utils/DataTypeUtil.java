/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polycat.catalog.common.utils;

import io.polycat.catalog.common.exception.CatalogException;
import io.polycat.catalog.common.model.stats.Decimal;
import io.polycat.catalog.common.model.stats.DecimalColumnStatsData;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;

public class DataTypeUtil {

    public static Decimal toCatalogDecimal(String value) {
        BigDecimal decimal = new BigDecimal(value);
        return new Decimal((short) decimal.scale(), decimal.unscaledValue().toByteArray());
    }

    public static String toCatalogDecimalString(Decimal decimal) {
        return new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()).toString();
    }

    public static String toString(Object o) {
        if (o != null) {
            return String.valueOf(o);
        }
        return null;
    }

    public static <T> T mapToColumnStatsData(Object dataValue, Class<T> targetClass) {
        try {
            if (dataValue instanceof Map) {
                return mapToColumnStatsData((Map<String, ?>) dataValue, targetClass);
            }
            return (T) dataValue;
        } catch (Exception e) {
            e.printStackTrace();
            throw new CatalogException("Column data value: " + dataValue + " parse error: " + e.getMessage());
        }
    }
    public static <T> T mapToColumnStatsData(Map<String, ?> dataValue, Class<T> targetClass) {
        try {
            T target = targetClass.newInstance();
            if(target instanceof DecimalColumnStatsData) {
                DecimalColumnStatsData data = new DecimalColumnStatsData();
                if (dataValue.get("bitVectors") instanceof List) {
                    data.setBitVectors(DataTypeUtil.toByte((List)dataValue.get("bitVectors")));
                } else {
                    data.setBitVectors(DataTypeUtil.getBitVectors(dataValue.get("bitVectors")));
                }
                data.setNumDVs(toLong(dataValue.get("numDVs")));
                data.setNumNulls(toLong(dataValue.get("numNulls")));
                data.setLowValue(getDecimalByMap(dataValue, "lowValue"));
                data.setHighValue(getDecimalByMap(dataValue, "highValue"));
                target =  (T) data;
            } else {
                BeanUtils.populate(target, dataValue);
            }
            return target;
        } catch (Exception e) {
            e.printStackTrace();
            throw new CatalogException("Column data value: " + dataValue + " parse error: " + e.getMessage());
        }
    }

    private static Decimal getDecimalByMap(Map<String, ?> dataValue, String valueKey) {
        if (dataValue != null && dataValue.containsKey(valueKey)) {
            Object value = dataValue.get(valueKey);
            if (value instanceof Decimal) {
                return (Decimal) value;
            }
            return new Decimal(toShort(((Map<String, ?>)value).get("scale").toString()),
                    DataTypeUtil.toBytes(((Map<String, ?>)value).get("unscaled")));
        }
        return null;
    }


    public static Double toDouble(Object o) {
        String s = toString(o);
        if (s != null) {
            return Double.parseDouble(s);
        }
        return null;
    }

    public static Long toLong(Object o) {
        Double toDouble = toDouble(o);
        if (toDouble != null) {
            return (long) toDouble.doubleValue();
        }
        return null;
    }

    public static Short toShort(Object o) {
        Double toDouble = toDouble(o);
        if (toDouble != null) {
            return (short) toDouble.doubleValue();
        }
        return null;
    }

    public static String getBitVectors(byte[] bitVectors) {
        if (bitVectors != null) {
            return new String(bitVectors);
        }
        return null;
    }

    public static byte[] toBytes(Object bitVectors) {
        if (bitVectors != null) {
            return toBytes(bitVectors.toString());
        }
        return null;
    }

    public static byte[] getBitVectors(Object bitVectors) {
        if (bitVectors != null) {
            return getBitVectors(bitVectors.toString());
        }
        return null;
    }

    public static byte[] getBitVectors(String bitVectors) {
        if (bitVectors != null) {
            return bitVectors.getBytes();
        }
        return null;
    }

    public static byte[] toBytes(String bitVectors) {
        if (bitVectors != null) {
            if ("null".equals(bitVectors)) {
                return null;
            }
            int iMax = bitVectors.length() - 1;
            if (iMax == -1) {
                return new byte[]{};
            }
            try {
                if (bitVectors.startsWith("[") && bitVectors.endsWith("]")) {
                    return toByte(bitVectors.substring(1, bitVectors.length() - 1));
                }
                return toByte(bitVectors);
            } catch (Exception e) {
                return bitVectors.getBytes();
            }
        }
        return null;
    }

    public static byte[] toByte(String strings) {
        if (strings != null) {
            String[] split = strings.split(", ");
            byte[] bytes = new byte[split.length];
            for (int i = 0; i < split.length; i++) {
                bytes[i] = getByte(split[i]);
            }
            return bytes;
        }
        return null;
    }

    private static byte getByte(String s) {
        return Double.valueOf(s).byteValue();
    }

    public static byte[] toByte(List<Object> list) {
        if (list != null) {
            byte[] bytes = new byte[list.size()];
            for (int i = 0; i < list.size(); i++) {
                bytes[i] = getByte(String.valueOf(list.get(i)));
            }
            return bytes;
        }
        return null;
    }
}
