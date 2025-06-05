package com.example.kafkaflink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class JsonValidatorTest {

    @Test
    public void testValidJson() {
        JsonValidator validator = new JsonValidator();
        String validJson = "{\"id\":101,\"name\":\"Alice\",\"amount\":2500.75,\"timestamp\":\"2025-05-09T10:00:00Z\"}";
        assertTrue(validator.isValid(validJson));
    }

    @Test
    public void testInvalidJson() {
        JsonValidator validator = new JsonValidator();
        String invalidJson = "{\"id\":\"invalid\",\"name\":\"Bob\",\"amt\":\"NaN\""; // Missing closing brace
        assertFalse(validator.isValid(invalidJson));
    }
}