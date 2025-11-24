package com.gfs.master.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gfs.master.model.ChunkLocation;
import com.gfs.master.model.PdfMetadata;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class MasterService {

    @Value("${gfs.chunk-size:32768}")
    private int CHUNK_SIZE;

    @Value("${gfs.replication-factor:3}")
    private int REPLICATION_FACTOR;

    @Value("${gfs.metadata-path:./metadata}")
    private String metadataPath;

    // Almacenamiento en memoria
    private final Map<String, PdfMetadata> pdfMetadataStore = new ConcurrentHashMap<>();
    private final Map<String, ChunkserverInfo> chunkservers = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        System.out.println("\n========================================================");
        System.out.println("  INICIALIZANDO MASTER SERVICE");
        System.out.println("========================================================");
        System.out.println("   Tamano de chunk: " + (CHUNK_SIZE / 1024) + " KB");
        System.out.println("   Factor de replicacion: " + REPLICATION_FACTOR + "x");
        System.out.println("   Ruta de metadatos: " + metadataPath);
        System.out.println();

        // Crear directorio de metadatos
        try {
            Path path = Paths.get(metadataPath);
            if (!Files.exists(path)) {
                Files.createDirectories(path);
                System.out.println("[OK] Directorio de metadatos creado");
            }
            loadMetadata();
        } catch (IOException e) {
            System.err.println("[WARN] Error creando directorio de metadatos: " + e.getMessage());
        }
    }

    /**
     * Planifica la subida de un PDF
     */
    public PdfMetadata planUpload(String pdfId, long size) {
        List<String> healthyServers = getHealthyChunkservers();

        if (healthyServers.isEmpty()) {
            throw new RuntimeException("No hay chunkservers disponibles");
        }

        if (healthyServers.size() < REPLICATION_FACTOR) {
            System.out.println("[WARN] Advertencia: Solo " + healthyServers.size() +
                               " servidores disponibles (recomendado: " + REPLICATION_FACTOR + ")");
        }

        int numChunks = (int) Math.ceil((double) size / CHUNK_SIZE);
        PdfMetadata metadata = new PdfMetadata(pdfId, size);

        System.out.println("   Distribuyendo chunks:");

        for (int i = 0; i < numChunks; i++) {
            List<String> selectedServers = selectServersForChunk(healthyServers, i);

            StringBuilder serversStr = new StringBuilder("[");
            for (int r = 0; r < selectedServers.size(); r++) {
                String server = selectedServers.get(r);
                ChunkLocation location = new ChunkLocation(i, server, r);
                metadata.getChunks().add(location);

                serversStr.append(server);
                if (r < selectedServers.size() - 1) serversStr.append(", ");
            }
            serversStr.append("]");

            System.out.println("      Chunk " + i + " -> " + serversStr);
        }

        pdfMetadataStore.put(pdfId, metadata);
        saveMetadata();

        return metadata;
    }

    /**
     * Selecciona servidores para un chunk específico
     */
    private List<String> selectServersForChunk(List<String> availableServers, int chunkIndex) {
        List<String> selected = new ArrayList<>();
        List<String> shuffled = new ArrayList<>(availableServers);

        // Rotación para distribuir carga
        Collections.rotate(shuffled, chunkIndex);

        int numReplicas = Math.min(REPLICATION_FACTOR, shuffled.size());
        for (int i = 0; i < numReplicas; i++) {
            selected.add(shuffled.get(i));
        }

        return selected;
    }

    /**
     * Obtiene metadatos de un PDF
     */
    public PdfMetadata getMetadata(String pdfId) {
        PdfMetadata metadata = pdfMetadataStore.get(pdfId);
        if (metadata == null) {
            throw new RuntimeException("PDF no encontrado: " + pdfId);
        }

        // Filtrar solo réplicas en servidores activos
        List<String> healthyServers = getHealthyChunkservers();
        PdfMetadata filtered = new PdfMetadata(metadata.getPdfId(), metadata.getSize());

        for (ChunkLocation chunk : metadata.getChunks()) {
            if (healthyServers.contains(chunk.getChunkserverUrl())) {
                filtered.getChunks().add(chunk);
            }
        }

        return filtered;
    }

    /**
     * Procesa heartbeat de un chunkserver
     */
    public void processHeartbeat(String url, String chunkserverId, Map<String, List<Integer>> inventory) {
        ChunkserverInfo info = chunkservers.computeIfAbsent(url,
                k -> new ChunkserverInfo(url, chunkserverId));

        info.updateHeartbeat(inventory);
    }

    /**
     * Registra un chunkserver
     */
    public void registerChunkserver(String url, String id) {
        boolean isReregistration = chunkservers.containsKey(url);

        ChunkserverInfo info = chunkservers.computeIfAbsent(url,
                k -> new ChunkserverInfo(url, id));

        // Actualizar heartbeat inmediatamente
        info.updateHeartbeat(new HashMap<>());

        if (isReregistration) {
            System.out.println("\n========================================================");
            System.out.println("  CHUNKSERVER RE-REGISTRADO");
            System.out.println("========================================================");
            System.out.println("   URL: " + url);
            System.out.println("   ID: " + id);
            System.out.println("   Estado: Reconectado despues de caida");
            System.out.println("   Total registrados: " + chunkservers.size());
            System.out.println();
        } else {
            System.out.println("\n========================================================");
            System.out.println("  CHUNKSERVER REGISTRADO");
            System.out.println("========================================================");
            System.out.println("   URL: " + url);
            System.out.println("   ID: " + id);
            System.out.println("   Total registrados: " + chunkservers.size());
            System.out.println();
        }
    }

    /**
     * Actualiza la ubicación de un chunk después de re-replicación
     */
    public void updateChunkLocation(String pdfId, int chunkIndex,
                                    String oldServerUrl, String newServerUrl) {
        PdfMetadata metadata = pdfMetadataStore.get(pdfId);
        if (metadata == null) {
            System.err.println("[WARN] No se encontró metadata para PDF: " + pdfId);
            return;
        }

        boolean updated = false;
        for (ChunkLocation chunk : metadata.getChunks()) {
            if (chunk.getChunkIndex() == chunkIndex &&
                chunk.getChunkserverUrl().equals(oldServerUrl)) {

                chunk.setChunkserverUrl(newServerUrl);
                updated = true;

                System.out.println("[METADATA] Ubicación actualizada:");
                System.out.println("   PDF: " + pdfId);
                System.out.println("   Chunk: " + chunkIndex);
                System.out.println("   Antiguo: " + oldServerUrl);
                System.out.println("   Nuevo: " + newServerUrl);
                break;
            }
        }

        if (updated) {
            saveMetadata();
        } else {
            System.err.println("[WARN] No se encontró chunk para actualizar: " +
                               pdfId + " chunk " + chunkIndex + " en " + oldServerUrl);
        }
    }

    /**
     * Obtiene lista de chunkservers activos
     */
    public List<String> getHealthyChunkservers() {
        List<String> healthy = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (ChunkserverInfo info : chunkservers.values()) {
            if (info.isHealthy(now)) {
                healthy.add(info.getUrl());
            }
        }

        return healthy;
    }

    /**
     * Obtiene estado del sistema
     */
    public Map<String, Object> getSystemStatus() {
        List<String> healthy = getHealthyChunkservers();

        Map<String, Object> status = new HashMap<>();
        status.put("totalChunkservers", chunkservers.size());
        status.put("healthyChunkservers", healthy.size());
        status.put("unhealthyChunkservers", chunkservers.size() - healthy.size());
        status.put("totalPdfs", pdfMetadataStore.size());
        status.put("chunkSize", CHUNK_SIZE);
        status.put("replicationFactor", REPLICATION_FACTOR);
        status.put("healthyServers", healthy);

        // Calcular estadísticas de chunks
        int totalChunks = 0;
        int totalReplicas = 0;
        for (PdfMetadata metadata : pdfMetadataStore.values()) {
            Set<Integer> uniqueChunks = new HashSet<>();
            for (ChunkLocation chunk : metadata.getChunks()) {
                uniqueChunks.add(chunk.getChunkIndex());
                totalReplicas++;
            }
            totalChunks += uniqueChunks.size();
        }

        status.put("totalChunks", totalChunks);
        status.put("totalReplicas", totalReplicas);

        return status;
    }

    /**
     * Lista todos los PDFs
     */
    public List<PdfMetadata> listAllPdfs() {
        return new ArrayList<>(pdfMetadataStore.values());
    }

    /**
     * Elimina un PDF
     */
    public void deletePdf(String pdfId) {
        pdfMetadataStore.remove(pdfId);
        saveMetadata();
        System.out.println("[DELETE] PDF eliminado de metadatos: " + pdfId);
    }

    /**
     * Guarda metadatos en disco
     */
    private void saveMetadata() {
        try {
            File file = new File(metadataPath + "/pdfs.json");
            objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValue(file, pdfMetadataStore);
        } catch (IOException e) {
            System.err.println("[WARN] Error guardando metadatos: " + e.getMessage());
        }
    }

    /**
     * Carga metadatos desde disco
     */
    private void loadMetadata() {
        try {
            File file = new File(metadataPath + "/pdfs.json");
            if (file.exists()) {
                Map<String, PdfMetadata> loaded = objectMapper.readValue(file,
                        objectMapper.getTypeFactory().constructMapType(
                                HashMap.class, String.class, PdfMetadata.class));
                pdfMetadataStore.putAll(loaded);
                System.out.println("[OK] Metadatos cargados: " + pdfMetadataStore.size() + " PDFs");
            }
        } catch (IOException e) {
            System.err.println("[WARN] Error cargando metadatos: " + e.getMessage());
        }
    }

    /**
     * Obtiene inventario de un chunkserver
     */
    public Map<String, List<Integer>> getChunkserverInventory(String url) {
        ChunkserverInfo info = chunkservers.get(url);
        return info != null ? info.getLastInventory() : new HashMap<>();
    }

    /**
     * Clase interna para información de chunkserver
     */
    private static class ChunkserverInfo {
        private final String url;
        private final String id;
        private long lastHeartbeat;
        private Map<String, List<Integer>> lastInventory;
        private static final long HEARTBEAT_TIMEOUT = 30000; // 30 segundos

        public ChunkserverInfo(String url, String id) {
            this.url = url;
            this.id = id;
            this.lastHeartbeat = System.currentTimeMillis();
            this.lastInventory = new HashMap<>();
        }

        public void updateHeartbeat(Map<String, List<Integer>> inventory) {
            this.lastHeartbeat = System.currentTimeMillis();
            this.lastInventory = inventory;
        }

        public boolean isHealthy(long currentTime) {
            return (currentTime - lastHeartbeat) < HEARTBEAT_TIMEOUT;
        }

        public String getUrl() {
            return url;
        }

        public String getId() {
            return id;
        }

        public Map<String, List<Integer>> getLastInventory() {
            return lastInventory;
        }
    }
}