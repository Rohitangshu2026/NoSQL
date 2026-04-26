package com.etl.core;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;

public class LogParser {
    private static final String LOG_PATTERN = "^(\\S+) \\[(\\w{3}) (\\w{3}) (\\d{2}) (\\d{2}):\\d{2}:\\d{2} (\\d{4})\\] \"([^\"]*)\" (\\d{3}) (\\S+)$";
    private static final Pattern PATTERN = Pattern.compile(LOG_PATTERN);
    private static final DateTimeFormatter INPUT_DATE_FORMAT = DateTimeFormatter.ofPattern("dd/MMM/yyyy", Locale.ENGLISH);
    private static final DateTimeFormatter OUT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static Optional<LogRecord> parse(String line) {
        Matcher matcher = PATTERN.matcher(line);
        if (!matcher.matches()) {
            return Optional.empty();
        }

        try {
            String host = matcher.group(1);
            // Group 2 is day of week, discarded
            String month = matcher.group(3);
            String day = matcher.group(4);
            String hourStr = matcher.group(5);
            String year = matcher.group(6);
            String requestStr = matcher.group(7);
            String statusStr = matcher.group(8);
            String bytesStr = matcher.group(9);

            String rawDate = day + "/" + month + "/" + year;
            String isoDate = LocalDate.parse(rawDate, INPUT_DATE_FORMAT).format(OUT_FMT);
            int hour = Integer.parseInt(hourStr);

            String[] requestParts = requestStr.split(" ");
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
