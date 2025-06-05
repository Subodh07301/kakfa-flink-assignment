package com.example.kafkaflink;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

public class JsonValidator implements Serializable {

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Checks if the input string is valid JSON.
     *
     * @param json JSON string to validate
     * @return true if valid JSON, false otherwise
     */
    public boolean isValid(String json) {
        try {
            mapper.readTree(json);
            return true;
        } catch (Exception e) {
            // Log invalid JSON or handle as needed
            System.err.println("Invalid JSON: " + json);
            return false;
        }
    }
}
