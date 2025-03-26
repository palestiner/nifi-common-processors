package dev.inmar.nifi.processors;

import com.jayway.jsonpath.JsonPath;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.util.StringUtils;


abstract class JsonPathValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            String error = null;
            if (isStale(subject, input)) {
                if (!StringUtils.isBlank(input)) {
                    try {
                        JsonPath compiledJsonPath = JsonPath.compile(input);
                        cacheComputedValue(subject, input, compiledJsonPath);
                    } catch (Exception ex) {
                        error = String.format("specified expression was not valid: %s", input);
                    }
                } else {
                    error = "the expression cannot be empty.";
                }
            }
            return new ValidationResult.Builder().subject(subject).valid(error == null).explanation(error).build();
        }

        /**
         * An optional hook to act on the compute value
         */
        abstract void cacheComputedValue(String subject, String input, JsonPath computedJsonPath);

        /**
         * A hook for implementing classes to determine if a cached value is stale for a compiled JsonPath represented by either a validation
         */
        abstract boolean isStale(String subject, String input);
    }
