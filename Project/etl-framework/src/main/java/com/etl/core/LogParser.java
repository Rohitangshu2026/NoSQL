package com.etl.core;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;

public class LogParser {
    // Robust Regex:
    // 1. Uses \\s+ to handle variable spacing.
    // 2. Uses (.*?) to capture the whole HTTP request, preventing failures on malformed requests like "GET /".
    // 3. Ends with \\s*$ to tolerate trailing whitespaces or carriage returns (\r), which cause matcher.matches() to fail.
    private static final Pattern PATTERN = Pattern.compile(
            "^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+\\[(.*?)\\]\\s+\"(.*?)\"\\s+(\\d{3})\\s+(\\S+)\\s*$"
    );
    
    // Updated date format to handle "01/Jul/1995:00:00:01 -0400" by explicitly capturing up to the year
    private static final DateTimeFormatter INPUT_DATE_FORMAT = DateTimeFormatter.ofPattern("dd/MMM/yyyy", Locale.ENGLISH);
    private static final DateTimeFormatter OUT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static Optional<LogRecord> parse(String line) {
        Matcher matcher = PATTERN.matcher(line);
        if (!matcher.matches()) {
            return Optional.empty();
        }

        try {
            String host = matcher.group(1);
            String datetimeStr = matcher.group(4); // e.g., "01/Jul/1995:00:00:01 -0400"
            String requestStr = matcher.group(5);  // e.g., "GET /history/apollo/ HTTP/1.0"
            String statusStr = matcher.group(6);
            String bytesStr = matcher.group(7);

            // Parse datetime
            String[] dtParts = datetimeStr.split(":");
            if (dtParts.length < 2) return Optional.empty();
            String rawDate = dtParts[0]; // "01/Jul/1995"
            String isoDate = LocalDate.parse(rawDate, INPUT_DATE_FORMAT).format(OUT_FMT);
            int hour = Integer.parseInt(dtParts[1]);

            // Parse request string safely
            String[] requestParts = requestStr.split("\\s+");
            String method = requestParts.length > 0 ? requestParts[0] : "UNKNOWN";
            String path = requestParts.length > 1 ? requestParts[1] : "UNKNOWN";
            String protocol = requestParts.length > 2 ? requestParts[2] : "UNKNOWN";

            int statusCode = Integer.parseInt(statusStr);
            long bytes = 0;
            if (!"-".equals(bytesStr)) {
                try {
                    bytes = Long.parseLong(bytesStr);
                } catch (NumberFormatException e) {
                    bytes = 0;
                }
            }

            LogRecord record = new LogRecord(host, isoDate, hour, method, path, protocol, statusCode, bytes);
            return Optional.of(record);

        } catch (DateTimeParseException | NumberFormatException e) {
            return Optional.empty();
        }
    }
}

