/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.inmar.nifi.processors;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.*;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"json", "split", "jsonpath"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription(
        "Splits a JSON File into multiple, separate FlowFiles for an array element specified by a JsonPath expression."
)
public class SplitJsonOnAttribute extends AbstractProcessor {

    static final String EMPTY_STRING_OPTION = "empty string";
    static final String NULL_STRING_OPTION = "the string 'null'";
    static final Map<String, String> NULL_REPRESENTATION_MAP = Map.of(
            EMPTY_STRING_OPTION, "",
            NULL_STRING_OPTION, "null"
    );
    public static final PropertyDescriptor NULL_VALUE_DEFAULT_REPRESENTATION = new PropertyDescriptor.Builder()
            .name("Null Value Representation")
            .description("Indicates the desired representation of JSON Path expressions resulting in a null value.")
            .required(true)
            .allowableValues(NULL_REPRESENTATION_MAP.keySet())
            .defaultValue(EMPTY_STRING_OPTION)
            .build();
    private final AtomicReference<JsonPath> JSON_PATH_REF = new AtomicReference<>();
    public static final PropertyDescriptor ARRAY_JSON_PATH_EXPRESSION = new PropertyDescriptor.Builder()
            .name("JsonPath Expression")
            .description("A JsonPath expression that indicates the array element to split into JSON/scalar fragments.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR) // Full validation/caching occurs in #customValidate
            .required(true)
            .build();

    public static final PropertyDescriptor MAX_STRING_LENGTH = new PropertyDescriptor.Builder()
            .name("Max String Length")
            .displayName("Max String Length")
            .description("The maximum allowed length of a string value when parsing the JSON document")
            .required(true)
            .defaultValue("20 MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor SPLIT_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Split attribute name")
            .description("Attribute name for split.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor SPLIT_RESULT_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Split result attribute name")
            .description("Attribute name for split result value.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Example success relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                    "If a FlowFile fails processing for any reason (for example, the FlowFile is not valid JSON or the specified "
                    + "path does not exist), it will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;
    private volatile Configuration jsonPathConfiguration;
    private Set<Relationship> relationships;
    private volatile String nullDefaultValue;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(
                ARRAY_JSON_PATH_EXPRESSION,
                NULL_VALUE_DEFAULT_REPRESENTATION,
                MAX_STRING_LENGTH,
                SPLIT_ATTRIBUTE_NAME,
                SPLIT_RESULT_ATTRIBUTE_NAME
        );
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(ProcessContext processContext) {
        nullDefaultValue = NULL_REPRESENTATION_MAP.get(
                processContext.getProperty(NULL_VALUE_DEFAULT_REPRESENTATION).getValue()
        );
        final int maxStringLength = processContext.getProperty(MAX_STRING_LENGTH).asDataSize(DataUnit.B).intValue();
        jsonPathConfiguration = createConfiguration(maxStringLength);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile original = session.get();
        if (original == null) {
            return;
        }
        final ComponentLog logger = getLogger();
        final String splitAttributeName = context.getProperty(SPLIT_ATTRIBUTE_NAME).getValue();
        final String splitAttribute = original.getAttribute(splitAttributeName);
        if (splitAttribute == null) {
            logger.error("FlowFile {} did not have attribute content by name {}.", original, splitAttributeName);
            session.transfer(original, REL_FAILURE);
            return;
        }
        final String splitResultAttributeName = context.getProperty(SPLIT_RESULT_ATTRIBUTE_NAME).getValue();
        DocumentContext documentContext;
        try {
            documentContext = validateAndEstablishJsonContext(splitAttribute, jsonPathConfiguration);
        } catch (InvalidJsonException e) {
            logger.error("FlowFile {} did not have valid JSON content in attribute {}.", original, splitAttributeName);
            session.transfer(original, REL_FAILURE);
            return;
        }
        final JsonPath jsonPath = JSON_PATH_REF.get();
        Object jsonPathResult;
        try {
            jsonPathResult = documentContext.read(jsonPath);
        } catch (PathNotFoundException e) {
            logger.warn("JsonPath {} could not be found for FlowFile {} attribute {}", jsonPath.getPath(), original, splitAttributeName);
            session.transfer(original, REL_FAILURE);
            return;
        }
        if (!(jsonPathResult instanceof List<?> resultList)) {
            logger.error(
                    "The evaluated value {} of {} was not a JSON Array compatible type and cannot be split.",
                    jsonPathResult,
                    jsonPath.getPath()
            );
            session.transfer(original, REL_FAILURE);
            return;
        }

        for (Object resultSegment : resultList) {
            FlowFile split = session.clone(original);
            String resultSegmentContent = getResultRepresentation(
                    jsonPathConfiguration.jsonProvider(),
                    resultSegment,
                    nullDefaultValue
            );
            split = session.putAttribute(split, splitResultAttributeName, resultSegmentContent);
            session.transfer(split, REL_SUCCESS);
        }
        session.remove(original);
        logger.info("Split {} into {} FlowFiles by attribute {}", original, resultList.size(), splitAttributeName);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        JsonPathValidator validator = new JsonPathValidator() {
            @Override
            public void cacheComputedValue(String subject, String input, JsonPath computedJson) {
                JSON_PATH_REF.set(computedJson);
            }

            @Override
            public boolean isStale(String subject, String input) {
                return JSON_PATH_REF.get() == null;
            }
        };
        String value = validationContext.getProperty(ARRAY_JSON_PATH_EXPRESSION).getValue();
        return Set.of(validator.validate(ARRAY_JSON_PATH_EXPRESSION.getName(), value, validationContext));
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
                        AttributeExpression.ResultType.STRING,
                        true
                ))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }


    private DocumentContext validateAndEstablishJsonContext(
            String jsonAttributeValue,
            Configuration jsonPathConfiguration
    ) {
        try {
            return JsonPath.using(jsonPathConfiguration).parse(jsonAttributeValue);
        } catch (IllegalArgumentException iae) {
            throw new InvalidJsonException(iae);
        }
    }

    private Configuration createConfiguration(final int maxStringLength) {
        final StreamReadConstraints streamReadConstraints = StreamReadConstraints.builder()
                .maxStringLength(maxStringLength)
                .build();
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.getFactory().setStreamReadConstraints(streamReadConstraints);
        final JsonProvider jsonProvider = new JacksonJsonProvider(objectMapper);
        return Configuration.builder().jsonProvider(jsonProvider).build();
    }

    private boolean isJsonScalar(Object obj) {
        return !(obj instanceof Map || obj instanceof List);
    }

    private String getResultRepresentation(JsonProvider jsonProvider, Object jsonPathResult, String defaultValue) {
        if (isJsonScalar(jsonPathResult)) {
            return Objects.toString(jsonPathResult, defaultValue);
        }
        return jsonProvider.toJson(jsonPathResult);
    }

}
