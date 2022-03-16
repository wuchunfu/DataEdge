/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.flink.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.utils.TypeStringUtils;
import org.apache.flink.types.Row;
import org.apache.seatunnel.flink.enums.FormatType;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public final class SchemaUtil {

    private static final Pattern DASH_COMPILE = Pattern.compile("-");

    private SchemaUtil() {
    }

    public static void setSchema(Schema schema, Object info, FormatType format) {

        switch (format) {
            case JSON:
                getJsonSchema(schema, (JSONObject) info);
                break;
            case CSV:
                getCsvSchema(schema, (List<Map<String, String>>) info);
                break;
            case ORC:
                getOrcSchema(schema, (JSONObject) info);
                break;
            case AVRO:
                getAvroSchema(schema, (JSONObject) info);
                break;
            case PARQUET:
                getParquetSchema(schema, (JSONObject) info);
                break;
            default:
        }
    }

    public static FormatDescriptor setFormat(FormatType format, JSONObject config) throws Exception {
        FormatDescriptor formatDescriptor = null;
        switch (format) {
            case JSON:
                formatDescriptor = new Json().failOnMissingField(false).deriveSchema();
                break;
            case CSV:
                Csv csv = new Csv().deriveSchema();
                Field interPro = csv.getClass().getDeclaredField("internalProperties");
                interPro.setAccessible(true);
                Object desc = interPro.get(csv);
                Class<DescriptorProperties> descCls = DescriptorProperties.class;
                Method putMethod = descCls.getDeclaredMethod("put", String.class, String.class);
                putMethod.setAccessible(true);
                for (Map.Entry<String, Object> entry : config.entrySet()) {
                    String key = entry.getKey();
                    if (key.startsWith("format.") && !StringUtils.equals(key, "format.type")) {
                        String value = config.getString(key);
                        putMethod.invoke(desc, key, value);
                    }
                }
                formatDescriptor = csv;
                break;
            case AVRO:
                formatDescriptor = new Avro().avroSchema(config.getString("schema"));
                break;
            case ORC:
            case PARQUET:
            default:
                break;
        }
        return formatDescriptor;
    }

    private static void getJsonSchema(Schema schema, JSONObject json) {

        for (Map.Entry<String, Object> entry : json.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof String) {
                schema.field(key, Types.STRING());
            } else if (value instanceof Integer) {
                schema.field(key, Types.INT());
            } else if (value instanceof Long) {
                schema.field(key, Types.LONG());
            } else if (value instanceof BigDecimal) {
                schema.field(key, Types.JAVA_BIG_DEC());
            } else if (value instanceof JSONObject) {
                schema.field(key, getTypeInformation((JSONObject) value));
            } else if (value instanceof JSONArray) {
                Object obj = ((JSONArray) value).get(0);
                if (obj instanceof JSONObject) {
                    schema.field(key, ObjectArrayTypeInfo.getInfoFor(Row[].class, getTypeInformation((JSONObject) obj)));
                } else {
                    schema.field(key, ObjectArrayTypeInfo.getInfoFor(Object[].class, TypeInformation.of(Object.class)));
                }
            }
        }
    }

    private static void getCsvSchema(Schema schema, List<Map<String, String>> schemaList) {

        for (Map<String, String> map : schemaList) {
            String field = map.get("field");
            String type = map.get("type").toUpperCase();
            schema.field(field, type);
        }
    }

    public static TypeInformation<?>[] getCsvType(List<Map<String, String>> schemaList) {
        TypeInformation<?>[] typeInformation = new TypeInformation[schemaList.size()];
        int i = 0;
        for (Map<String, String> map : schemaList) {
            String type = map.get("type").toUpperCase();
            typeInformation[i++] = TypeStringUtils.readTypeInfo(type);
        }
        return typeInformation;
    }

    /**
     * todo
     *
     * @param schema schema
     * @param json   json
     */
    private static void getOrcSchema(Schema schema, JSONObject json) {

    }

    /**
     * todo
     *
     * @param schema schema
     * @param json   json
     */
    private static void getParquetSchema(Schema schema, JSONObject json) {

    }

    private static void getAvroSchema(Schema schema, JSONObject json) {
        RowTypeInfo typeInfo = (RowTypeInfo) AvroSchemaConverter.<Row>convertToTypeInfo(json.toString());
        String[] fieldNames = typeInfo.getFieldNames();
        for (String name : fieldNames) {
            schema.field(name, typeInfo.getTypeAt(name));
        }
    }

    public static RowTypeInfo getTypeInformation(JSONObject json) {
        int size = json.size();
        String[] fields = new String[size];
        TypeInformation<?>[] informations = new TypeInformation[size];
        int i = 0;
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            fields[i] = key;
            if (value instanceof String) {
                informations[i] = Types.STRING();
            } else if (value instanceof Integer) {
                informations[i] = Types.INT();
            } else if (value instanceof Long) {
                informations[i] = Types.LONG();
            } else if (value instanceof BigDecimal) {
                informations[i] = Types.JAVA_BIG_DEC();
            } else if (value instanceof JSONObject) {
                informations[i] = getTypeInformation((JSONObject) value);
            } else if (value instanceof JSONArray) {
                JSONObject demo = ((JSONArray) value).getJSONObject(0);
                informations[i] = ObjectArrayTypeInfo.getInfoFor(Row[].class, getTypeInformation(demo));
            }
            i++;
        }
        return new RowTypeInfo(informations, fields);
    }

    public static String getUniqueTableName() {
        return DASH_COMPILE.matcher(UUID.randomUUID().toString()).replaceAll("_");
    }
}
