package edu.harvard.iq.datacommons.analyzer;

import dev.langchain4j.model.ollama.OllamaChatModel;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.logging.*;

public class Application {

    private static final Logger logger = Logger.getLogger(Application.class.getName());

    public static void main(String[] args) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        setupLogging(timestamp);

        try {
            Properties props = new Properties();
            try (InputStream input = Application.class.getClassLoader().getResourceAsStream("application.properties")) {
                if (input != null) {
                    props.load(input);
                }
            }

            String baseUrl = props.getProperty("ollama.url", "http://localhost:11434");
            String modelName = props.getProperty("ollama.model", "llama3.2");
            String searchRoot = props.getProperty("analyzer.search-root", "data");

            OllamaChatModel model = OllamaChatModel.builder()
                    .baseUrl(baseUrl)
                    .modelName(modelName)
                    .temperature(0.0)
                    .build();

            AnalyzerService service = new AnalyzerService(model, searchRoot, timestamp);
            service.run();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Application failed", e);
        }
    }

    private static void setupLogging(String timestamp) {
        try {
            LogManager.getLogManager().reset();
            Logger rootLogger = Logger.getLogger("");

            String logFileName = "DataCommonsReady-" + timestamp + ".log";
            FileHandler fileHandler = new FileHandler(logFileName);
            fileHandler.setFormatter(new SimpleFormatter());
            fileHandler.setLevel(Level.ALL);
            rootLogger.addHandler(fileHandler);

            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setFormatter(new SimpleFormatter());
            consoleHandler.setLevel(Level.ALL);
            rootLogger.addHandler(consoleHandler);

            rootLogger.setLevel(Level.INFO);
        } catch (Exception e) {
            System.err.println("Failed to setup logging: " + e.getMessage());
        }
    }
}
