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
import java.util.Arrays;
import java.util.List;
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
    }

    private void analyzeFile(Path file) throws IOException {
        logger.info("Analyzing file: " + file);
        try {
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

            boolean hasLocation = false;
            boolean hasTime = false;

            for (int i = 0; i < headers.size(); i++) {
                String label = headers.get(i);
                final int index = i;
                List<String> values = samples.stream()
                        .filter(row -> index < row.size())
                        .map(row -> row.get(index).replaceAll("^\"|\"$", ""))
                        .collect(Collectors.toList());

                if (values.isEmpty()) continue;

                if (isLocation(label, values)) {
                    if (!hasLocation) {
                        hasLocation = true;
                        logger.info("Found location variable: " + label);
                    }
                }
                if (isTime(label, values)) {
                    if (!hasTime) {
                        hasTime = true;
                        logger.info("Found time/date variable: " + label);
                    }
                }
            }

            if (hasLocation && hasTime) {
                logger.info("RESULT: File " + file.getFileName() + " meets requirements (Has Location AND Time).");
                copyToDataCommonsReady(file);
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
            "departamento", "provincia", "distrito", "municipality",
            "geo", "location", "place", "territory", "continent"
    );

    private static final Set<String> KNOWN_NON_LOCATION_PATTERNS = Set.of(
            "resentment", "resent", "sentiment", "opinion", "score", "index",
            "callable", "putable", "qualified", "status", "insurance"
    );

    private boolean isLocation(String label, List<String> values) {
        String lowerLabel = label.toLowerCase().replaceAll("[^a-z0-9]", "");

        // Quick reject: labels that contain known non-location patterns
        for (String pattern : KNOWN_NON_LOCATION_PATTERNS) {
            if (lowerLabel.contains(pattern)) {
                logger.info("  isLocation(" + label + ") -> NO (known non-location pattern)");
                return false;
            }
        }

        // Quick accept: labels that exactly match known location terms
        if (KNOWN_LOCATION_LABELS.contains(lowerLabel)) {
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
        String response = chatModel.generate(prompt);
        logger.info("  isLocation(" + label + ") -> " + response.trim());
        return response.trim().toUpperCase().startsWith("YES");
    }

    private static final Set<String> KNOWN_TIME_LABELS = Set.of(
            "year", "date", "time", "timestamp", "month", "quarter", "day",
            "issuedate", "maturitydate", "startdate", "enddate", "birthdate",
            "created", "updated", "datetime", "period", "semester", "week"
    );

    private static final Set<String> KNOWN_NON_TIME_PATTERNS = Set.of(
            "resentment", "resent", "sentiment", "opinion", "score", "index",
            "cost", "price", "amount", "rate", "yield", "coupon"
    );

    private boolean isTime(String label, List<String> values) {
        String lowerLabel = label.toLowerCase().replaceAll("[^a-z0-9]", "");

        // Quick reject: labels that contain known non-time patterns
        for (String pattern : KNOWN_NON_TIME_PATTERNS) {
            if (lowerLabel.contains(pattern)) {
                logger.info("  isTime(" + label + ") -> NO (known non-time pattern)");
                return false;
            }
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
        String response = chatModel.generate(prompt);
        logger.info("  isTime(" + label + ") -> " + response.trim());
        return response.trim().toUpperCase().startsWith("YES");
    }
}
