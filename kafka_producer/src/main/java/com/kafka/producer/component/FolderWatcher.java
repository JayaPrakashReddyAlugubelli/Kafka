package com.kafka.producer.component;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import com.kafka.producer.service.CsvKafkaProducer;

import java.io.IOException;
import java.nio.file.*;
import java.util.stream.Stream;

@Component
public class FolderWatcher implements CommandLineRunner {

    private final CsvKafkaProducer producer;

    public FolderWatcher(CsvKafkaProducer producer) {
        this.producer = producer;
    }

    @Override
    public void run(String... args) throws Exception {
        Path watchDir = Paths.get("data/incoming");
        Path processedDir = Paths.get("data/processed");

        createDirectoryIfNotExists(watchDir);
        createDirectoryIfNotExists(processedDir);

        System.out.println("📂 Incoming folder: " + watchDir.toAbsolutePath());
        System.out.println("📂 Processed folder: " + processedDir.toAbsolutePath());

        processExistingFiles(watchDir, processedDir);

        WatchService watchService = FileSystems.getDefault().newWatchService();
        watchDir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

        System.out.println("👀 Watching folder: " + watchDir.toAbsolutePath());

        while (true) {
            WatchKey key = watchService.take();
            for (WatchEvent<?> event : key.pollEvents()) {
                if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
                    Path fileName = (Path) event.context();
                    Path fullPath = watchDir.resolve(fileName);

                    if (fileName.toString().endsWith(".csv")) {
                        System.out.println("📄 New CSV detected: " + fullPath);
                        processFile(fullPath, processedDir);
                    }
                }
            }
            if (!key.reset()) break;
        }
    }

    private void createDirectoryIfNotExists(Path path) throws IOException {
        if (!Files.exists(path)) {
            Files.createDirectories(path);
            System.out.println("✅ Created folder: " + path.toAbsolutePath());
        }
    }

    private void processExistingFiles(Path watchDir, Path processedDir) throws IOException {
        try (Stream<Path> files = Files.list(watchDir)) {
            files.filter(file -> file.toString().endsWith(".csv"))
                 .forEach(file -> {
                     System.out.println("⚡ Processing existing CSV: " + file);
                     processFile(file, processedDir);
                 });
        }
    }

    private void processFile(Path file, Path processedDir) {
        try {
            producer.sendCsvDataToKafka(file);
            Path targetPath = processedDir.resolve(file.getFileName());
            Files.move(file, targetPath, StandardCopyOption.REPLACE_EXISTING);
            System.out.println("📦 Moved to processed: " + targetPath);
        } catch (Exception e) {
            System.err.println("❌ Error processing file: " + file + " -> " + e.getMessage());
        }
    }
}
