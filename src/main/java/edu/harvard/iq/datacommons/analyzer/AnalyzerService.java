package edu.harvard.iq.datacommons.analyzer;

import dev.langchain4j.model.ollama.OllamaChatModel;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AnalyzerService {

    private static final Logger logger = Logger.getLogger(AnalyzerService.class.getName());

    private final OllamaChatModel chatModel;
    private final String searchRoot;
    private final String outputDirectory;
    private final List<String> selectionLog = new ArrayList<>();

    public AnalyzerService(OllamaChatModel chatModel, String searchRoot, String timestamp) {
        this.chatModel = chatModel;
        this.searchRoot = searchRoot;
        this.outputDirectory = "DataCommonsReady-" + timestamp;
    }

    public void run() throws Exception {
        Path root = Paths.get(searchRoot);
        logger.info("Scanning directory: " + root.toAbsolutePath());

        try (Stream<Path> paths = Files.walk(root)) {
            List<Path> dataFiles = paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".csv") || path.toString().endsWith(".tab"))
                    .collect(Collectors.toList());

            for (Path file : dataFiles) {
                try {
                    analyzeFile(file);
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Failed to analyze " + file, e);
                }
            }
        }
        writeSelectionLog();
    }

    private void analyzeFile(Path file) throws IOException {
        logger.info("Analyzing file: " + file);
        try {
            List<String> locationVariables = new ArrayList<>();
            List<String> timeVariables = new ArrayList<>();

            // Filename based time detection as fallback
            String fileName = file.getFileName().toString().toLowerCase();
            if (fileName.matches(".*[12][0-9]{3}.*")) {
                timeVariables.add("[filename pattern]");
                logger.info("Found time/date signal in filename: " + fileName);
            }

            List<String> headers = new ArrayList<>();
            List<List<String>> samples = new ArrayList<>();

            CSVFormat format = CSVFormat.DEFAULT;
            if (file.toString().endsWith(".tab")) {
                format = CSVFormat.TDF;
            }

            try (Reader reader = new FileReader(file.toFile());
                 CSVParser csvParser = new CSVParser(reader, format.withFirstRecordAsHeader())) {
                
                headers = csvParser.getHeaderNames();
                int count = 0;
                for (CSVRecord record : csvParser) {
                    if (count >= 5) break;
                    List<String> row = new ArrayList<>();
                    for (String header : headers) {
                        row.add(record.get(header));
                    }
                    samples.add(row);
                    count++;
                }
            }

            if (headers.isEmpty()) {
                logger.info("No headers found in " + file);
                return;
            }

            for (int i = 0; i < headers.size(); i++) {
                String label = headers.get(i);
                final int index = i;
                List<String> values = samples.stream()
                        .filter(row -> index < row.size())
                        .map(row -> row.get(index).replaceAll("^\"|\"$", ""))
                        .collect(Collectors.toList());

                if (values.isEmpty()) continue;

                if (isLocation(label, values)) {
                    locationVariables.add(label);
                    logger.info("Found location variable: " + label);
                }
                if (isTime(label, values)) {
                    timeVariables.add(label);
                    logger.info("Found time/date variable: " + label);
                }
            }

            boolean hasLocation = !locationVariables.isEmpty();
            boolean hasTime = !timeVariables.isEmpty();

            if (hasLocation && hasTime) {
                logger.info("RESULT: File " + file.getFileName() + " meets requirements (Has Location AND Time).");
                copyToDataCommonsReady(file);
                
                String logEntry = String.format("File: %s\n - Location variables: %s\n - Time variables: %s\n",
                        file.getFileName(), locationVariables, timeVariables);
                selectionLog.add(logEntry);
            } else {
                logger.info("RESULT: File " + file.getFileName() + " DOES NOT meet requirements.");
                if (!hasLocation) logger.info(" - Missing Location");
                if (!hasTime) logger.info(" - Missing Time/Date");
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error reading file " + file, e);
            throw e;
        }
    }

    private void writeSelectionLog() {
        if (selectionLog.isEmpty()) {
            return;
        }

        Path logFile = Paths.get(outputDirectory, "selection_log.txt");
        try {
            Files.createDirectories(logFile.getParent());
            Files.write(logFile, selectionLog, java.nio.charset.StandardCharsets.UTF_8);
            logger.info("Selection log written to: " + logFile);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to write selection log", e);
        }
    }

    private void copyToDataCommonsReady(Path file) throws IOException {
        Path root = Paths.get(searchRoot);
        Path relativePath = root.relativize(file);
        Path target = Paths.get(outputDirectory).resolve(relativePath);

        logger.info("Copying to: " + target);
        Files.createDirectories(target.getParent());
        Files.copy(file, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private static final Set<String> KNOWN_LOCATION_LABELS = Set.of(
            "country", "state", "city", "region", "nation", "province", "county",
            "lat", "latitude", "long", "longitude", "lon",
            "countrycode", "country_code", "iso3", "iso2", "fips", "statefip", "statefips",
            "zipcode", "zip", "postalcode", "postal_code", "address",
            "departamento", "provincia", "distrito", "municipality", "pueblo", "nomb",
            "geo", "location", "place", "territory", "continent"
    );

    private static final Set<String> BOOLEAN_VALUES = Set.of(
            "yes", "no", "true", "false", "y", "n", "0", "1"
    );

    private static final Set<String> LIKERT_VALUES = Set.of(
            "1", "2", "3", "4", "5"
    );

    private static final Set<String> GEO_KEYWORD_VALUES = Set.of(
            "state", "county", "city", "country", "region", "province", "district", "municipality"
    );

//    private static final Set<String> COUNTRY_NAME_HINTS = Set.of(
//            "argentina", "brasil", "brazil", "mexico", "méxico", "colombia", "chile", "peru", "perú",
//            "venezuela", "ecuador", "bolivia", "paraguay", "uruguay", "costa rica", "cuba", "guatemala",
//            "honduras", "el salvador", "nicaragua", "panama", "panamá", "republica dominicana", "república dominicana"
//    );

    private static final class ValueProfile {
        boolean mostlyBoolean;
        boolean mostlyLikert;
        boolean mostlyNumeric;
        boolean hasGeoLikeText;
    }

    private ValueProfile profileValues(List<String> values) {
        ValueProfile profile = new ValueProfile();
        if (values == null || values.isEmpty()) {
            return profile;
        }

        int nonEmpty = 0;
        int booleanCount = 0;
        int likertCount = 0;
        int numericCount = 0;
        int geoTextCount = 0;
        Set<String> distinct = new HashSet<>();

        for (String raw : values) {
            String normalized = normalizeValue(raw);
            if (normalized.isEmpty()) {
                continue;
            }

            nonEmpty++;
            String lower = normalized.toLowerCase();
            distinct.add(lower);

            if (BOOLEAN_VALUES.contains(lower)) {
                booleanCount++;
            }

            if (LIKERT_VALUES.contains(lower)) {
                likertCount++;
            }

            if (isNumeric(lower)) {
                numericCount++;
            }

            if (looksLikeGeoText(lower)) {
                geoTextCount++;
            }
        }

        if (nonEmpty == 0) {
            return profile;
        }

        profile.mostlyBoolean = ((double) booleanCount / nonEmpty) >= 0.8;
        profile.mostlyLikert = ((double) likertCount / nonEmpty) >= 0.8 && distinct.size() <= 7;
        profile.mostlyNumeric = ((double) numericCount / nonEmpty) >= 0.8;
        profile.hasGeoLikeText = geoTextCount > 0;
        return profile;
    }

    private String normalizeValue(String value) {
        if (value == null) {
            return "";
        }
        return value.replaceAll("^\\\"|\\\"$", "").trim();
    }

    private boolean isNumeric(String value) {
        try {
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean looksLikeGeoText(String lowerValue) {
        if (KNOWN_LOCATION_LABELS.contains(lowerValue)) {
            return true;
        }

        if (lowerValue.length() >= 3 && lowerValue.matches("[a-z ]+") && !isNumeric(lowerValue)) {
            for (String keyword : GEO_KEYWORD_VALUES) {
                if (lowerValue.contains(keyword)) {
                    return true;
                }
            }
        }

        return false;
    }

    private String normalizeAsciiLower(String value) {
        return java.text.Normalizer.normalize(value, java.text.Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "")
                .toLowerCase(Locale.ROOT)
                .trim();
    }

    private boolean isIsoLikeCountryCode(String value) {
        String normalized = normalizeValue(value);
        return normalized.matches("[A-Z]{3}") || normalized.matches("[a-z]{3}");
    }

    private boolean hasValueDrivenLocationSignal(List<String> values) {
        if (values == null || values.isEmpty()) {
            return false;
        }

        int nonEmpty = 0;
        int countryNameCount = 0;
        int isoCodeCount = 0;
        int geoTextCount = 0;

        for (String raw : values) {
            String normalized = normalizeValue(raw);
            if (normalized.isEmpty()) {
                continue;
            }

            nonEmpty++;
            String asciiLower = normalizeAsciiLower(normalized);
//            if (COUNTRY_NAME_HINTS.contains(asciiLower)) {
//                countryNameCount++;
//            }
            if (isIsoLikeCountryCode(normalized)) {
                isoCodeCount++;
            }
            if (looksLikeGeoText(asciiLower)) {
                geoTextCount++;
            }
        }

        if (nonEmpty < 3) {
            return false;
        }

        double countryNameRatio = (double) countryNameCount / nonEmpty;
        double isoCodeRatio = (double) isoCodeCount / nonEmpty;
        double geoTextRatio = (double) geoTextCount / nonEmpty;

        return countryNameRatio >= 0.6 || isoCodeRatio >= 0.6 || geoTextRatio >= 0.6;
    }

    private boolean isLikelyYearValue(String value) {
        String normalized = normalizeValue(value);
        if (!normalized.matches("\\d{4}")) {
            return false;
        }

        int year = Integer.parseInt(normalized);
        return year >= 1800 && year <= 2100;
    }

    private boolean hasValueDrivenTimeSignal(List<String> values) {
        if (values == null || values.isEmpty()) {
            return false;
        }

        int nonEmpty = 0;
        int yearLikeCount = 0;
        Set<String> distinctYears = new HashSet<>();

        for (String raw : values) {
            String normalized = normalizeValue(raw);
            if (normalized.isEmpty()) {
                continue;
            }

            nonEmpty++;
            if (isLikelyYearValue(normalized)) {
                yearLikeCount++;
                distinctYears.add(normalized);
            }
        }

        if (nonEmpty < 3) {
            return false;
        }

        double yearLikeRatio = (double) yearLikeCount / nonEmpty;
        return yearLikeRatio >= 0.8 && distinctYears.size() >= 2;
    }

    private boolean hasLocationLabelSignal(String lowerLabel) {
        if (KNOWN_LOCATION_LABELS.contains(lowerLabel)) {
            return true;
        }

        for (String locationLabel : KNOWN_LOCATION_LABELS) {
            if (lowerLabel.startsWith(locationLabel) || lowerLabel.endsWith(locationLabel)) {
                return true;
            }
        }

        return false;
    }

    private boolean isLocation(String label, List<String> values) {
        String lowerLabel = label.toLowerCase().replaceAll("[^a-z0-9]", "");
        ValueProfile profile = profileValues(values);

        // Quick reject by value profile for non-location fields
        if (profile.mostlyLikert && !hasLocationLabelSignal(lowerLabel)) {
            logger.info("  isLocation(" + label + ") -> NO (Likert-like values)");
            return false;
        }
        if (profile.mostlyBoolean && !profile.hasGeoLikeText) {
            logger.info("  isLocation(" + label + ") -> NO (boolean/flag values)");
            return false;
        }

        if (hasValueDrivenLocationSignal(values)) {
            logger.info("  isLocation(" + label + ") -> YES (value-driven location signal)");
            return true;
        }

        if (profile.mostlyNumeric && !profile.hasGeoLikeText && !hasLocationLabelSignal(lowerLabel)) {
            logger.info("  isLocation(" + label + ") -> NO (mostly numeric values without geo evidence)");
            return false;
        }

        // Quick accept: labels that exactly match known location terms
        if (hasLocationLabelSignal(lowerLabel)) {
            logger.info("  isLocation(" + label + ") -> YES (known location label)");
            return true;
        }

        // Fall back to LLM for ambiguous cases
        String prompt = String.format(
                "Does this variable represent a geographic location?\n\n" +
                "Variable Label: %s\n" +
                "Sample Values: %s\n\n" +
                "Answer YES if the variable's label and values indicate a place or geographic entity. Examples of location variables:\n" +
                "- Label 'country' with values like 'USA', 'France'\n" +
                "- Label 'state' or 'State' with values like 'Indiana', 'California'\n" +
                "- Label 'region' with values like 'North West', 'Southeast'\n" +
                "- Label 'nation' with values like 'England', 'Scotland'\n" +
                "- Label 'city' with values like 'Boston', 'London'\n" +
                "- Labels like 'lat', 'latitude', 'longitude', 'countrycode', 'iso3', 'fips', 'departamento', 'provincia'\n\n" +
                "Answer NO if:\n" +
                "- The variable measures a concept, sentiment, or score even if the label contains a geographic word (e.g., 'southern_resentment6a', 'london_resent3a', 'northern_resentment')\n" +
                "- The variable is a boolean/flag like 'Callable', 'Putable', 'BankQualified' with values 'Yes'/'No'\n" +
                "- The values are numeric scores (1, 2, 3, 4, 5) not geographic codes\n\n" +
                "Answer ONLY 'YES' or 'NO'.",
                label, values.toString());
        if (chatModel == null) {
            logger.info("  isLocation(" + label + ") -> NO (no chat model available)");
            return false;
        }
        String response = chatModel.generate(prompt);
        logger.info("  isLocation(" + label + ") -> " + response.trim());
        return response.trim().toUpperCase().startsWith("YES");
    }

    private static final Set<String> KNOWN_TIME_LABELS = Set.of(
            "year", "date", "time", "timestamp", "month", "quarter", "day",
            "issuedate", "maturitydate", "startdate", "enddate", "birthdate",
            "created", "updated", "datetime", "period", "semester", "week", "fecha", "ano"
    );

    private boolean isTime(String label, List<String> values) {
        String lowerLabel = label.toLowerCase().replaceAll("[^a-z0-9]", "");
        ValueProfile profile = profileValues(values);

        if (profile.mostlyLikert && !lowerLabel.contains("year")) {
            logger.info("  isTime(" + label + ") -> NO (Likert-like values)");
            return false;
        }

        if (hasValueDrivenTimeSignal(values)) {
            logger.info("  isTime(" + label + ") -> YES (value-driven year pattern)");
            return true;
        }

        // Quick accept: labels that match known time terms (check if label contains a known time term)
        for (String timeLabel : KNOWN_TIME_LABELS) {
            if (lowerLabel.equals(timeLabel) || lowerLabel.endsWith(timeLabel) || lowerLabel.startsWith(timeLabel)) {
                logger.info("  isTime(" + label + ") -> YES (known time label)");
                return true;
            }
        }

        // Fall back to LLM for ambiguous cases
        String prompt = String.format(
                "Does this variable represent a time or date?\n\n" +
                "Variable Label: %s\n" +
                "Sample Values: %s\n\n" +
                "Answer YES if the variable's label and values indicate a point in time. Examples of time/date variables:\n" +
                "- Label 'year' or 'Year' with values like '2020', '1995'\n" +
                "- Label 'date', 'IssueDate', 'MaturityDate' with values like '2023-01-01', '2003-12-10'\n" +
                "- Label 'month' with values like 'January', '01'\n" +
                "- Label 'time', 'timestamp', 'quarter', 'day'\n" +
                "- Numeric years in the range 1800-2100 are time values\n\n" +
                "Answer NO if:\n" +
                "- The variable is a survey question or sentiment score (e.g., 'southern_resentment6a', 'longlive', 'ft_labour', 'costofliving')\n" +
                "- The values are small integers (1, 2, 3, 4, 5) representing Likert scale responses, NOT years\n" +
                "- The label has a number suffix for question numbering (e.g., 'resentment6a', 'resent3a') — these are survey items, NOT dates\n" +
                "- The variable is a score, count, amount, price, or identifier\n\n" +
                "Answer ONLY 'YES' or 'NO'.",
                label, values.toString());
        if (chatModel == null) {
            logger.info("  isTime(" + label + ") -> NO (no chat model available)");
            return false;
        }
        String response = chatModel.generate(prompt);
        logger.info("  isTime(" + label + ") -> " + response.trim());
        return response.trim().toUpperCase().startsWith("YES");
    }
}
