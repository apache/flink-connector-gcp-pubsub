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

package org.apache.flink.streaming.connectors.gcp.pubsub.v2;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.google.pubsub.v1.PubsubMessage;

import javax.annotation.Nullable;

import java.io.Serializable;

public interface PubSubDeserializationSchemaV2<T> extends Serializable {

    static <T> PubSubDeserializationSchemaV2<T> dataOnly(DeserializationSchema<T> schema) {
        return new PubSubDeserializationSchemaV2<T>() {
            @Override
            public void open(DeserializationSchema.InitializationContext context) throws Exception {
                schema.open(context);
            }

            @Override
            public T deserialize(PubsubMessage message) throws Exception {
                return schema.deserialize(message.getData().toByteArray());
            }

            @Override
            public TypeInformation<T> getProducedType() {
                return schema.getProducedType();
            }
        };
    }

    void open(DeserializationSchema.InitializationContext context) throws Exception;

    @Nullable
    T deserialize(PubsubMessage message) throws Exception;

    TypeInformation<T> getProducedType();
}
