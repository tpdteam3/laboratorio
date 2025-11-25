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
import java.util.stream.Collectors;

@Service
public class MasterService {

    @Value("${gfs.chunk-size:65536}")
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
        System.out.println("   Tamaño de chunk: " + (CHUNK_SIZE / 1024) + " KB");
        System.out.println("   Factor de replicación: " + REPLICATION_FACTOR + "x");
        System.out.println("   Ruta de metadatos: " + metadataPath);
        System.out.println("   Balanceo de carga: ACTIVADO");
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
     * Planifica la subida de un PDF con balanceo de carga mejorado
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

        System.out.println("   Distribuyendo chunks con balanceo de carga:");

        for (int i = 0; i < numChunks; i++) {
            // Usar balanceo de carga mejorado
            List<String> selectedServers = selectServersForChunkWithLoadBalancing(
                    healthyServers, i
            );

            StringBuilder serversStr = new StringBuilder("[");
            for (int r = 0; r < selectedServers.size(); r++) {
                String server = selectedServers.get(r);
                ChunkLocation location = new ChunkLocation(i, server, r);
                metadata.getChunks().add(location);

                serversStr.append(extractServerId(server));
                if (r < selectedServers.size() - 1) serversStr.append(", ");
            }
            serversStr.append("]");

            System.out.println("      Chunk " + i + " -> " + serversStr);
        }

        // Mostrar distribución final
        showLoadDistribution(metadata);

        pdfMetadataStore.put(pdfId, metadata);
        saveMetadata();

        return metadata;
    }

    /**
     * Selección de servidores con balanceo de carga mejorado
     */
    private List<String> selectServersForChunkWithLoadBalancing(
            List<String> availableServers, int chunkIndex) {

        // Calcular carga actual de cada servidor
        Map<String, ServerLoad> serverLoads = calculateServerLoads(availableServers);

        // Ordenar servidores por carga (menor primero)
        List<String> sortedServers = availableServers.stream()
                .sorted((s1, s2) -> {
                    ServerLoad load1 = serverLoads.get(s1);
                    ServerLoad load2 = serverLoads.get(s2);

                    // Primero por número de chunks
                    int cmp = Integer.compare(load1.chunkCount, load2.chunkCount);
                    if (cmp != 0) return cmp;

                    // Luego por espacio usado
                    return Long.compare(load1.storageUsed, load2.storageUsed);
                })
                .collect(Collectors.toList());

        // Aplicar rotación ligera para evitar siempre elegir los mismos
        Collections.rotate(sortedServers, -(chunkIndex % sortedServers.size()));

        // Seleccionar los N menos cargados
        int numReplicas = Math.min(REPLICATION_FACTOR, sortedServers.size());
        return new ArrayList<>(sortedServers.subList(0, numReplicas));
    }

    /**
     * Calcula la carga actual de cada servidor
     */
    private Map<String, ServerLoad> calculateServerLoads(List<String> servers) {
        Map<String, ServerLoad> loads = new HashMap<>();

        for (String server : servers) {
            ChunkserverInfo info = chunkservers.get(server);
            ServerLoad load = new ServerLoad();

            if (info != null && info.getLastInventory() != null) {
                // Contar chunks totales en este servidor
                Map<String, List<Integer>> inventory = info.getLastInventory();
                load.chunkCount = inventory.values().stream()
                        .mapToInt(List::size)
                        .sum();

                // Calcular espacio usado (estimado)
                load.storageUsed = load.chunkCount * CHUNK_SIZE;
            }

            loads.put(server, load);
        }

        return loads;
    }

    /**
     * Muestra distribución de carga después de la asignación
     */
    private void showLoadDistribution(PdfMetadata metadata) {
        Map<String, Integer> distribution = new HashMap<>();

        for (ChunkLocation chunk : metadata.getChunks()) {
            String serverId = extractServerId(chunk.getChunkserverUrl());
            distribution.put(serverId, distribution.getOrDefault(serverId, 0) + 1);
        }

        System.out.println("\n   📊 Distribución de carga para este PDF:");
        distribution.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry ->
                        System.out.println("      " + entry.getKey() + ": " +
                                           entry.getValue() + " chunks")
                );
    }

    /**
     * NUEVO: Agrega una nueva réplica de chunk
     */
    public void addChunkReplica(String pdfId, ChunkLocation newReplica) {
        PdfMetadata metadata = pdfMetadataStore.get(pdfId);
        if (metadata == null) {
            throw new RuntimeException("PDF no encontrado: " + pdfId);
        }

        // Verificar que no exista ya esta réplica
        boolean exists = metadata.getChunks().stream()
                .anyMatch(c -> c.getChunkIndex() == newReplica.getChunkIndex() &&
                               c.getChunkserverUrl().equals(newReplica.getChunkserverUrl()));

        if (!exists) {
            metadata.getChunks().add(newReplica);
            saveMetadata();
        }
    }

    /**
     * NUEVO: Elimina una réplica específica de chunk
     */
    public void removeChunkReplica(String pdfId, int chunkIndex, String chunkserverUrl) {
        PdfMetadata metadata = pdfMetadataStore.get(pdfId);
        if (metadata == null) {
            return;
        }

        // Remover la réplica específica
        boolean removed = metadata.getChunks().removeIf(c ->
                c.getChunkIndex() == chunkIndex &&
                c.getChunkserverUrl().equals(chunkserverUrl)
        );

        if (removed) {
            saveMetadata();
            System.out.println("   [METADATA] Réplica removida: PDF=" + pdfId +
                               ", Chunk=" + chunkIndex + ", Server=" + chunkserverUrl);
        }
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

        info.updateHeartbeat(new HashMap<>());

        if (isReregistration) {
            System.out.println("\n========================================================");
            System.out.println("  CHUNKSERVER RE-REGISTRADO");
            System.out.println("========================================================");
            System.out.println("   URL: " + url);
            System.out.println("   ID: " + id);
            System.out.println("   Estado: Reconectado después de caída");
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
     * NUEVO: Obtiene lista de TODOS los chunkservers (saludables o no)
     */
    public List<String> getAllChunkservers() {
        return new ArrayList<>(chunkservers.keySet());
    }

    /**
     * Obtiene estado del sistema con estadísticas de carga
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

        // Estadísticas de balanceo de carga
        Map<String, Integer> loadPerServer = new HashMap<>();
        for (PdfMetadata metadata : pdfMetadataStore.values()) {
            for (ChunkLocation chunk : metadata.getChunks()) {
                String serverId = extractServerId(chunk.getChunkserverUrl());
                loadPerServer.put(serverId, loadPerServer.getOrDefault(serverId, 0) + 1);
            }
        }
        status.put("loadDistribution", loadPerServer);

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
     * Extrae el ID del servidor desde la URL
     */
    private String extractServerId(String url) {
        // Extrae el puerto de la URL como ID
        // http://localhost:9001 -> cs-9001
        if (url.contains(":")) {
            String port = url.substring(url.lastIndexOf(":") + 1);
            return "cs-" + port;
        }
        return url;
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

    /**
     * Clase interna para tracking de carga de servidor
     */
    private static class ServerLoad {
        int chunkCount = 0;
        long storageUsed = 0;
    }
}