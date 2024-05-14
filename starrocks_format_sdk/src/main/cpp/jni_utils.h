// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <jni.h>
#include <string>
#include <unordered_map>
#include <vector>
#include "storage/tablet_schema.h"

#define SAFE_CALL_COLUMN_FUNCATION(column, body)                          \
    if (column != nullptr) {                                              \
        try {                                                             \
            body;                                                         \
        } catch (const std::exception& ex) {                              \
            env->ThrowNew(kRuntimeExceptionClass, ex.what());             \
        }                                                                 \
    } else {                                                              \
        env->ThrowNew(kRuntimeExceptionClass, "Invalid column handler!"); \
    }

#define SAFE_CALL_CHUNK_FUNCATION(chunk, body)                           \
    if (chunk != nullptr) {                                              \
        try {                                                            \
            body;                                                        \
        } catch (const std::exception& ex) {                             \
            env->ThrowNew(kRuntimeExceptionClass, ex.what());            \
        }                                                                \
    } else {                                                             \
        env->ThrowNew(kRuntimeExceptionClass, "Invalid chunk handler!"); \
    }

#define SAFE_CALL_READER_FUNCATION(reader, body)                                 \
    if (reader != nullptr) {                                                     \
        try {                                                                    \
            body;                                                                \
        } catch (const std::exception& ex) {                                     \
            env->ThrowNew(kRuntimeExceptionClass, ex.what());                    \
        }                                                                        \
    } else {                                                                     \
        env->ThrowNew(kRuntimeExceptionClass, "Invalid tablet reader handler!"); \
    }

#define SAFE_CALL_WRITER_FUNCATION(writer, body)                                 \
    if (writer != nullptr) {                                                     \
        try {                                                                    \
            body;                                                                \
        } catch (const std::exception& ex) {                                     \
            env->ThrowNew(kRuntimeExceptionClass, ex.what());                    \
        }                                                                        \
    } else {                                                                     \
        env->ThrowNew(kRuntimeExceptionClass, "Invalid tablet writer handler!"); \
    }

static inline jclass find_class(JNIEnv* env, const std::string& class_name) {
    auto clazz = env->FindClass(class_name.c_str());
    auto g_clazz = env->NewGlobalRef(clazz);
    env->DeleteLocalRef(clazz);
    return (jclass)g_clazz;
}

static inline jmethodID get_method_id(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
    jmethodID ret = env->GetMethodID(this_class, name, sig);
    return ret;
}

static inline std::string jstring_to_cstring(JNIEnv* env, jstring& jvalue) {
    const char* str = env->GetStringUTFChars(jvalue, NULL);
    std::string value = std::string(str);
    env->ReleaseStringUTFChars(jvalue, str);
    return value;
}

static inline jstring cstring_to_jstring(JNIEnv* env, std::string string) {
    return env->NewStringUTF(string.c_str());
}

static inline std::unordered_map<std::string, std::string> jhashmap_to_cmap(JNIEnv* env, jobject hashMap) {
    std::unordered_map<std::string, std::string> result;
    jclass mapClass = env->FindClass("java/util/Map");
    if (mapClass == NULL) {
        return result;
    }
    jmethodID entrySet = env->GetMethodID(mapClass, "entrySet", "()Ljava/util/Set;");
    if (entrySet == NULL) {
        return result;
    }
    jobject set = env->CallObjectMethod(hashMap, entrySet);
    if (set == NULL) {
        return result;
    }
    // Obtain an iterator over the Set
    jclass setClass = env->FindClass("java/util/Set");
    if (setClass == NULL) {
        return result;
    }
    jmethodID iterator = env->GetMethodID(setClass, "iterator", "()Ljava/util/Iterator;");
    if (iterator == NULL) {
        return result;
    }
    jobject iter = env->CallObjectMethod(set, iterator);
    if (iter == NULL) {
        return result;
    }
    // Get the Iterator method IDs
    jclass iteratorClass = env->FindClass("java/util/Iterator");
    if (iteratorClass == NULL) {
        return result;
    }
    jmethodID hasNext = env->GetMethodID(iteratorClass, "hasNext", "()Z");
    if (hasNext == NULL) {
        return result;
    }
    jmethodID next = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");
    if (next == NULL) {
        return result;
    }
    // Get the Entry class method IDs
    jclass entryClass = env->FindClass("java/util/Map$Entry");
    if (entryClass == NULL) {
        return result;
    }
    jmethodID getKey = env->GetMethodID(entryClass, "getKey", "()Ljava/lang/Object;");
    if (getKey == NULL) {
        return result;
    }
    jmethodID getValue = env->GetMethodID(entryClass, "getValue", "()Ljava/lang/Object;");
    if (getValue == NULL) {
        return result;
    }
    // Iterate over the entry Set
    while (env->CallBooleanMethod(iter, hasNext)) {
        jobject entry = env->CallObjectMethod(iter, next);
        jstring key = (jstring)env->CallObjectMethod(entry, getKey);
        jstring value = (jstring)env->CallObjectMethod(entry, getValue);
        const char* keyStr = env->GetStringUTFChars(key, NULL);
        if (!keyStr) { // Out of memory
            return result;
        }
        const char* valueStr = env->GetStringUTFChars(value, NULL);
        if (!valueStr) { // Out of memory
            env->ReleaseStringUTFChars(key, keyStr);
            return result;
        }

        result[std::string(keyStr)] = std::string(valueStr);

        env->DeleteLocalRef(entry);
        env->ReleaseStringUTFChars(key, keyStr);
        env->DeleteLocalRef(key);
        env->ReleaseStringUTFChars(value, valueStr);
        env->DeleteLocalRef(value);
    }
    return result;
}

// The jvalue is in big-endian byte-order.
// because BigInteger.toByteArray alway return big-endian byte-order byte array
// If the first bit is 1, it is a negative number.
static inline std::vector<uint8_t> jbyteArray_to_carray(JNIEnv* env, jbyteArray jvalue) {
    std::vector<uint8_t> result;
    uint32_t jvalue_bytes = env->GetArrayLength(jvalue);
    int8_t* src_value = (int8_t*)env->GetByteArrayElements(jvalue, NULL);
    if (jvalue_bytes > 0) {
        result.resize(jvalue_bytes);
        memcpy(result.data(), src_value, jvalue_bytes);
    }
    env->ReleaseByteArrayElements(jvalue, src_value, 0);
    return result;
}

// The jvalue is in big-endian byte-order.
// because BigInteger.toByteArray alway return big-endian byte-order byte array
// If the first bit is 1, it is a negative number.
template <typename T>
static inline T BigInteger_to_native_value(JNIEnv* env, jbyteArray jvalue) {
    T value = 0;

    uint32_t jvalue_bytes = env->GetArrayLength(jvalue);
    DCHECK(sizeof(T) >= jvalue_bytes);

    int8_t* src_value = (int8_t*)env->GetByteArrayElements(jvalue, NULL);

    if (jvalue_bytes == 0) {
        env->ReleaseByteArrayElements(jvalue, src_value, 0);
        return value;
    }

    const bool is_negative = static_cast<int8_t>(src_value[0]) < 0;
    for (int i = 0; i < jvalue_bytes; i++) {
        value = value << 8;
        value = value | (src_value[i] & 0xFF);
    }
    if (is_negative) {
        typedef typename std::make_unsigned<T>::type UT;
        for (int i = sizeof(value) - 1; i >= jvalue_bytes; i--) {
            value = value | (static_cast<UT>(0xFF) << (i * 8));
        }
    }

    env->ReleaseByteArrayElements(jvalue, src_value, 0);

    return value;
}

static inline std::shared_ptr<starrocks::TabletSchema> jbyteArray_to_TableSchema(JNIEnv* env, jbyteArray schema) {
    // get schema
    uint32_t j_schema_num_bytes = env->GetArrayLength(schema);
    int8_t* p_schema = env->GetByteArrayElements(schema, NULL);
    starrocks::TabletSchemaPB schema_pb;
    bool parsed = schema_pb.ParseFromArray(p_schema, j_schema_num_bytes);
    if (!parsed) {
        LOG(INFO) << " parse schema failed!";
    }
    env->ReleaseByteArrayElements(schema, p_schema, 0);

    return starrocks::TabletSchema::create(schema_pb);
}
