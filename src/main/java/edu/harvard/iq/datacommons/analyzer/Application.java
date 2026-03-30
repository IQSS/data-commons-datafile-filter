package edu.harvard.iq.datacommons.analyzer;

import dev.langchain4j.model.ollama.OllamaChatModel;

import java.io.InputStream;
import java.util.Properties;

public class Application {

    public static void main(String[] args) {
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
                    .build();

            AnalyzerService service = new AnalyzerService(model, searchRoot);
            service.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
