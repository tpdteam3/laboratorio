package com.gfs.chunkserver.service;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

@Service
public class StorageService {

    @Value("${chunkserver.storage-path:./storage}")
    private String storagePath;

    @Value("${chunkserver.id:chunkserver-1}")
    private String chunkserverId;

    private Path resolvedStoragePath;

    @PostConstruct
    public void init() throws IOException {
        resolvedStoragePath = Paths.get(storagePath).toAbsolutePath().normalize();

        System.out.println("\n╔════════════════════════════════════════════════════════╗");
        System.out.println("║  💾 INICIALIZANDO STORAGE SERVICE                     ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   ID: " + chunkserverId);
        System.out.println("   Ruta: " + resolvedStoragePath);

        if (!Files.exists(resolvedStoragePath)) {
            Files.createDirectories(resolvedStoragePath);
            System.out.println("   ✅ Directorio creado");
        } else {
            System.out.println("   ✅ Directorio existente");
        }

        File storageDir = resolvedStoragePath.toFile();
        long freeSpace = storageDir.getFreeSpace();
        System.out.println("   💾 Espacio disponible: " + (freeSpace / (1024 * 1024)) + " MB");
        System.out.println();
    }

    /**
     * Guarda un chunk en disco
     */
    public void writeChunk(String pdfId, int chunkIndex, String base64Data) {
        try {
            String filename = generateFilename(pdfId, chunkIndex);
            Path filePath = resolvedStoragePath.resolve(filename);

            byte[] data = Base64.getDecoder().decode(base64Data);
            Files.write(filePath, data);
            System.out.println("[" + chunkserverId + "] Chunk guardado: " + filename +
                               " (" + data.length + " bytes)");
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Error decodificando Base64: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new RuntimeException("Error escribiendo chunk: " + e.getMessage(), e);
        }
    }

    /**
     * Lee un chunk desde disco
     */
    public byte[] readChunk(String pdfId, int chunkIndex) {
        try {
            String filename = generateFilename(pdfId, chunkIndex);
            Path filePath = resolvedStoragePath.resolve(filename);

            if (!Files.exists(filePath)) {
                throw new RuntimeException("Chunk no encontrado: " + filename);
            }

            byte[] data = Files.readAllBytes(filePath);
            System.out.println("📖 [" + chunkserverId + "] Chunk leído: " + filename +
                               " (" + data.length + " bytes)");
            return data;
        } catch (IOException e) {
            throw new RuntimeException("Error leyendo chunk: " + e.getMessage(), e);
        }
    }

    /**
     * Verifica si un chunk existe
     */
    public boolean chunkExists(String pdfId, int chunkIndex) {
        String filename = generateFilename(pdfId, chunkIndex);
        Path filePath = resolvedStoragePath.resolve(filename);
        return Files.exists(filePath);
    }

    /**
     * Elimina un chunk
     */
    public void deleteChunk(String pdfId, int chunkIndex) {
        try {
            String filename = generateFilename(pdfId, chunkIndex);
            Path filePath = resolvedStoragePath.resolve(filename);

            if (Files.exists(filePath)) {
                Files.delete(filePath);
                System.out.println("🗑️  [" + chunkserverId + "] Chunk eliminado: " + filename);
            }

        } catch (IOException e) {
            throw new RuntimeException("Error eliminando chunk: " + e.getMessage(), e);
        }
    }

    /**
     * Obtiene inventario de chunks almacenados
     * Formato: { "pdfId1": [0, 1, 2], "pdfId2": [0, 3] }
     */
    public Map<String, List<Integer>> getInventory() {
        try {
            if (!Files.exists(resolvedStoragePath)) {
                return new HashMap<>();
            }

            Map<String, List<Integer>> inventory = new HashMap<>();

            try (Stream<Path> files = Files.list(resolvedStoragePath)) {
                files.filter(Files::isRegularFile)
                        .forEach(path -> {
                            String filename = path.getFileName().toString();

                            // Formato: pdfId_chunk_N.bin
                            if (filename.matches(".*_chunk_\\d+\\.bin")) {
                                try {
                                    String[] parts = filename.split("_chunk_");
                                    String pdfId = parts[0];
                                    int chunkIndex = Integer.parseInt(
                                            parts[1].replace(".bin", "")
                                    );

                                    inventory.computeIfAbsent(pdfId, k -> new ArrayList<>())
                                            .add(chunkIndex);

                                } catch (Exception e) {
                                    System.err.println("⚠️  Error parseando archivo: " + filename);
                                }
                            }
                        });
            }

            // Ordenar índices
            inventory.values().forEach(Collections::sort);

            return inventory;

        } catch (Exception e) {
            System.err.println("❌ Error obteniendo inventario: " + e.getMessage());
            return new HashMap<>();
        }
    }

    /**
     * Obtiene estadísticas del chunkserver
     */
    public Map<String, Object> getStats() {
        try {
            Map<String, Object> stats = new HashMap<>();

            if (!Files.exists(resolvedStoragePath)) {
                stats.put("totalChunks", 0);
                stats.put("storageUsed", 0L);
                return stats;
            }

            try (Stream<Path> files = Files.list(resolvedStoragePath)) {
                long[] totalSize = {0};
                long count = files
                        .filter(Files::isRegularFile)
                        .peek(path -> {
                            try {
                                totalSize[0] += Files.size(path);
                            } catch (IOException e) {
                                // Ignorar
                            }
                        })
                        .count();

                File storageDir = resolvedStoragePath.toFile();
                stats.put("chunkserverId", chunkserverId);
                stats.put("totalChunks", count);
                stats.put("storageUsed", totalSize[0]);
                stats.put("storageUsedMB", totalSize[0] / (1024.0 * 1024.0));
                stats.put("freeSpaceMB", storageDir.getFreeSpace() / (1024 * 1024));
                stats.put("storagePath", resolvedStoragePath.toString());
            }

            return stats;

        } catch (IOException e) {
            throw new RuntimeException("Error obteniendo estadísticas: " + e.getMessage(), e);
        }
    }

    /**
     * Genera nombre de archivo para un chunk
     * Formato: pdfId_chunk_N.bin
     */
    private String generateFilename(String pdfId, int chunkIndex) {
        return pdfId + "_chunk_" + chunkIndex + ".bin";
    }
}
