# Data Commons datafiles filter

A Java-based application that uses LLMs (via LangChain4j and Ollama) to analyze data files (`.csv` and `.tab`) and identify variables containing location and time/date information.

## Project Overview

The application scans a specified directory (recursively), reads each data file, and uses an LLM to determine if any of the columns represent:
- **Location**: Cities, countries, coordinates, addresses, latitude, longitude, etc.
- **Time/Date**: Years, months, timestamps, dates, durations, etc.

It outputs whether each file meets the requirements (contains both a location and a time variable).

## Prerequisites

- **Java 17** or higher.
- **Maven** for building the project.
- **Ollama** installed and running locally (or accessible via network).
    - Ensure you have the `llama3.2` model (or your preferred model) pulled in Ollama: `ollama pull llama3.2`

## Configuration

The application is configured via `src/main/resources/application.properties`:

- `ollama.url`: The base URL for the Ollama API (default: `http://localhost:11434`).
- `ollama.model`: The LLM model to use (default: `llama3.2`).
- `analyzer.search-root`: The root directory to scan for data files (default: `data`).

## Usage

### Build the project

```bash
mvn clean compile
```

### Run the application

```bash
mvn exec:java
```

Alternatively, if you've already compiled:

```bash
mvn exec:java -Dexec.mainClass="edu.harvard.iq.datacommons.analyzer.Application"
```

### Overriding Configuration

You can override any property defined in `application.properties` by passing it as a system property (using `-D`) on the command line:

```bash
java -Dollama.model=llama3 -Danalyzer.search-root=/path/to/data -jar target/data-commons-datafile-filter-1.0-SNAPSHOT-jar-with-dependencies.jar
```

Or when running via Maven:

```bash
mvn exec:java -Dollama.model=llama3 -Danalyzer.search-root=/path/to/data
```

The supported properties are:
- `ollama.url`
- `ollama.model`
- `analyzer.search-root`

## How it Works

1. **Scanning**: The `AnalyzerService` walks the directory tree starting from `analyzer.search-root`.
2. **Parsing**: For each `.csv` or `.tab` file, it reads the header and the first 5 rows of data.
3. **LLM Analysis**: For each column, it sends a prompt to the Ollama model (using LangChain4j) containing the column label and sample values.
4. **Classification**: The LLM responds with `YES` or `NO` to classify if the column represents a location or time/date.
5. **Results**: The application prints the analysis results for each file to the console.
6. **Copying**: If a file is identified as having both a location and a time/date variable, it is copied to a new directory named `DataCommonsReady-<timestamp>` (e.g. `DataCommonsReady-20240315-103000`).
    - The original directory structure is preserved within this directory.
    - For example, if `data/subdir/file.csv` meets the requirements, it will be copied to `DataCommonsReady-<timestamp>/subdir/file.csv`.

## Output Directory

The `DataCommonsReady-<timestamp>` directory will contain:
1. All files that are deemed "compliant" (containing both Location and Time data). The original directory structure is preserved.
2. `selection_log.txt`: A log file detailing for each selected file all variables that contributed to its selection (location variables and time/date variables).
3. `DataCommonsReady-<timestamp>.log`: The full application execution log.

## Project Structure

- `src/main/java/edu/harvard/iq/datacommons/analyzer/Application.java`: Main entry point.
- `src/main/java/edu/harvard/iq/datacommons/analyzer/AnalyzerService.java`: Core analysis logic.
- `src/main/resources/application.properties`: Configuration settings.
- `pom.xml`: Maven dependencies and build configuration.
- `data/`: Sample data directory.
