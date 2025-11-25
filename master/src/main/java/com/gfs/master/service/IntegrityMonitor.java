package com.gfs.master.service;

import com.gfs.master.model.ChunkLocation;
import com.gfs.master.model.PdfMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Servicio mejorado de monitoreo de integridad
 * - Detecta y repara chunks faltantes
 * - Re-replicación automática proactiva
 * - Garbage collection de chunks huérfanos
 * - Eliminación de sobre-replicación
 */
@Service
public class IntegrityMonitor {

    @Autowired
    private MasterService masterService;

    @Value("${gfs.replication-factor:3}")
    private int REPLICATION_FACTOR;

    private final RestTemplate restTemplate = new RestTemplate();

    // Estadísticas
    private long totalRepairs = 0;
    private long totalChecks = 0;
    private long totalGarbageCollected = 0;
    private long totalReReplications = 0;
    private long totalOverReplicasRemoved = 0;

    /**
     * Verifica integridad cada 30 segundos
     * Detecta chunks faltantes y los repara
     */
    @Scheduled(fixedDelay = 30000, initialDelay = 10000)
    public void checkIntegrity() {
        totalChecks++;

        System.out.println("\n╔════════════════════════════════════════════════════════╗");
        System.out.println("║  🔍 VERIFICACIÓN DE INTEGRIDAD                        ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");

        List<String> healthyServers = masterService.getHealthyChunkservers();
        if (healthyServers.isEmpty()) {
            System.out.println("   ⚠️  No hay servidores activos para verificar");
            return;
        }

        List<PdfMetadata> allPdfs = masterService.listAllPdfs();
        int issuesFound = 0;
        int issuesRepaired = 0;

        for (PdfMetadata pdf : allPdfs) {
            // Agrupar chunks por índice
            Map<Integer, List<ChunkLocation>> chunksByIndex = new HashMap<>();
            for (ChunkLocation chunk : pdf.getChunks()) {
                chunksByIndex.computeIfAbsent(chunk.getChunkIndex(), k -> new ArrayList<>())
                        .add(chunk);
            }

            // Verificar cada chunk
            for (Map.Entry<Integer, List<ChunkLocation>> entry : chunksByIndex.entrySet()) {
                int chunkIndex = entry.getKey();
                List<ChunkLocation> replicas = entry.getValue();

                // Verificar si cada réplica existe físicamente
                for (ChunkLocation replica : replicas) {
                    if (!healthyServers.contains(replica.getChunkserverUrl())) {
                        continue; // Servidor caído, skip
                    }

                    if (!chunkExists(pdf.getPdfId(), chunkIndex, replica.getChunkserverUrl())) {
                        System.out.println("   ❌ Chunk faltante detectado:");
                        System.out.println("      PDF: " + pdf.getPdfId());
                        System.out.println("      Chunk: " + chunkIndex);
                        System.out.println("      Servidor: " + replica.getChunkserverUrl());

                        issuesFound++;

                        // Intentar reparar
                        if (repairChunk(pdf.getPdfId(), chunkIndex, replica.getChunkserverUrl(), replicas)) {
                            issuesRepaired++;
                            totalRepairs++;
                        }
                    }
                }
            }
        }

        if (issuesFound > 0) {
            System.out.println("\n   📊 Resultado:");
            System.out.println("      Problemas detectados: " + issuesFound);
            System.out.println("      Problemas reparados: " + issuesRepaired);
            System.out.println("      Total reparaciones históricas: " + totalRepairs);
        } else {
            System.out.println("   ✅ Sistema íntegro - sin problemas detectados");
        }
        System.out.println();
    }

    /**
     * Re-replicación automática proactiva
     * Mantiene el factor de replicación deseado
     * Se ejecuta cada 60 segundos
     */
    @Scheduled(fixedDelay = 60000, initialDelay = 20000)
    public void checkReplicationFactor() {
        System.out.println("\n╔════════════════════════════════════════════════════════╗");
        System.out.println("║  📦 VERIFICACIÓN DE FACTOR DE REPLICACIÓN             ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");

        List<String> healthyServers = masterService.getHealthyChunkservers();

        if (healthyServers.isEmpty()) {
            System.out.println("   ⚠️  No hay servidores activos");
            return;
        }

        if (healthyServers.size() < REPLICATION_FACTOR) {
            System.out.println("   ⚠️  Solo " + healthyServers.size() +
                               " servidores disponibles (requerido: " + REPLICATION_FACTOR + ")");
        }

        List<PdfMetadata> allPdfs = masterService.listAllPdfs();
        int chunksUnderReplicated = 0;
        int chunksOverReplicated = 0;
        int replicasCreated = 0;
        int replicasRemoved = 0;

        for (PdfMetadata pdf : allPdfs) {
            Map<Integer, List<ChunkLocation>> chunksByIndex = groupByIndex(pdf.getChunks());

            for (Map.Entry<Integer, List<ChunkLocation>> entry : chunksByIndex.entrySet()) {
                int chunkIndex = entry.getKey();
                List<ChunkLocation> replicas = entry.getValue();

                // Contar réplicas activas (en servidores saludables Y que existen físicamente)
                List<ChunkLocation> activeReplicas = replicas.stream()
                        .filter(r -> healthyServers.contains(r.getChunkserverUrl()))
                        .filter(r -> chunkExists(pdf.getPdfId(), chunkIndex, r.getChunkserverUrl()))
                        .collect(Collectors.toList());

                int targetReplicas = Math.min(REPLICATION_FACTOR, healthyServers.size());

                // CASO 1: Sub-replicación (faltan réplicas)
                if (activeReplicas.size() < targetReplicas) {
                    int neededReplicas = targetReplicas - activeReplicas.size();

                    System.out.println("   ⚠️  Chunk sub-replicado:");
                    System.out.println("      PDF: " + pdf.getPdfId());
                    System.out.println("      Chunk: " + chunkIndex);
                    System.out.println("      Réplicas activas: " + activeReplicas.size() +
                                       "/" + REPLICATION_FACTOR);

                    chunksUnderReplicated++;

                    int created = replicateChunk(pdf.getPdfId(), chunkIndex,
                            activeReplicas, neededReplicas, healthyServers);
                    replicasCreated += created;
                    totalReReplications += created;
                }
                // CASO 2: Sobre-replicación (demasiadas réplicas)
                else if (activeReplicas.size() > targetReplicas) {
                    int excessReplicas = activeReplicas.size() - targetReplicas;

                    System.out.println("   ⚠️  Chunk sobre-replicado:");
                    System.out.println("      PDF: " + pdf.getPdfId());
                    System.out.println("      Chunk: " + chunkIndex);
                    System.out.println("      Réplicas activas: " + activeReplicas.size() +
                                       "/" + REPLICATION_FACTOR);

                    chunksOverReplicated++;

                    int removed = removeExcessReplicas(pdf.getPdfId(), chunkIndex,
                            activeReplicas, excessReplicas);
                    replicasRemoved += removed;
                    totalOverReplicasRemoved += removed;
                }
            }
        }

        // Mostrar resultado
        boolean hadIssues = chunksUnderReplicated > 0 || chunksOverReplicated > 0;

        if (hadIssues) {
            System.out.println("\n   📊 Resultado:");
            if (chunksUnderReplicated > 0) {
                System.out.println("      Chunks sub-replicados: " + chunksUnderReplicated);
                System.out.println("      Nuevas réplicas creadas: " + replicasCreated);
            }
            if (chunksOverReplicated > 0) {
                System.out.println("      Chunks sobre-replicados: " + chunksOverReplicated);
                System.out.println("      Réplicas excedentes eliminadas: " + replicasRemoved);
            }
            System.out.println("      Total re-replicaciones históricas: " + totalReReplications);
            System.out.println("      Total sobre-réplicas eliminadas: " + totalOverReplicasRemoved);
        } else {
            System.out.println("   ✅ Factor de replicación óptimo en todos los chunks");
        }
        System.out.println();
    }

    /**
     * NUEVO: Elimina réplicas excedentes para mantener el factor de replicación
     */
    private int removeExcessReplicas(String pdfId, int chunkIndex,
                                     List<ChunkLocation> activeReplicas,
                                     int excessCount) {

        System.out.println("      🗑️  Eliminando " + excessCount + " réplicas excedentes...");

        // Ordenar réplicas por índice (mantener las primarias, eliminar las últimas)
        List<ChunkLocation> sortedReplicas = activeReplicas.stream()
                .sorted(Comparator.comparingInt(ChunkLocation::getReplicaIndex).reversed())
                .collect(Collectors.toList());

        int removed = 0;

        // Eliminar las últimas N réplicas
        for (int i = 0; i < excessCount && i < sortedReplicas.size(); i++) {
            ChunkLocation replicaToRemove = sortedReplicas.get(i);
            String serverUrl = replicaToRemove.getChunkserverUrl();

            try {
                // Eliminar físicamente del chunkserver
                if (deleteChunkFromServer(pdfId, chunkIndex, serverUrl)) {
                    // Eliminar de metadatos
                    masterService.removeChunkReplica(pdfId, chunkIndex, serverUrl);

                    System.out.println("         ✅ Réplica eliminada de: " + serverUrl);
                    removed++;
                } else {
                    System.out.println("         ❌ Error eliminando de: " + serverUrl);
                }

            } catch (Exception e) {
                System.err.println("         ⚠️  Error eliminando réplica: " + e.getMessage());
            }
        }

        return removed;
    }

    /**
     * Limpia réplicas de metadatos que apuntan a servidores caídos
     * Se ejecuta cada 2 minutos
     */
    @Scheduled(fixedDelay = 120000, initialDelay = 45000)
    public void cleanupStaleMetadata() {
        System.out.println("\n╔════════════════════════════════════════════════════════╗");
        System.out.println("║  🧹 LIMPIEZA DE METADATOS OBSOLETOS                   ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");

        List<String> healthyServers = masterService.getHealthyChunkservers();
        List<String> allServers = masterService.getAllChunkservers();

        // Encontrar servidores que están registrados pero no saludables
        Set<String> unhealthyServers = new HashSet<>(allServers);
        unhealthyServers.removeAll(healthyServers);

        if (unhealthyServers.isEmpty()) {
            System.out.println("   ✅ Todos los servidores están saludables");
            return;
        }

        System.out.println("   ⚠️  Servidores no saludables: " + unhealthyServers.size());

        int metadataEntriesRemoved = 0;
        List<PdfMetadata> allPdfs = masterService.listAllPdfs();

        for (PdfMetadata pdf : allPdfs) {
            List<ChunkLocation> chunksToRemove = new ArrayList<>();

            for (ChunkLocation chunk : pdf.getChunks()) {
                // Si el chunk apunta a un servidor no saludable Y no existe físicamente
                if (unhealthyServers.contains(chunk.getChunkserverUrl())) {
                    if (!chunkExists(pdf.getPdfId(), chunk.getChunkIndex(),
                            chunk.getChunkserverUrl())) {
                        chunksToRemove.add(chunk);
                    }
                }
            }

            // Remover entradas de metadatos obsoletas
            for (ChunkLocation chunk : chunksToRemove) {
                masterService.removeChunkReplica(pdf.getPdfId(),
                        chunk.getChunkIndex(),
                        chunk.getChunkserverUrl());
                metadataEntriesRemoved++;

                System.out.println("   🗑️  Metadata removido:");
                System.out.println("      PDF: " + pdf.getPdfId());
                System.out.println("      Chunk: " + chunk.getChunkIndex());
                System.out.println("      Servidor: " + chunk.getChunkserverUrl());
            }
        }

        if (metadataEntriesRemoved > 0) {
            System.out.println("\n   📊 Resultado:");
            System.out.println("      Entradas de metadata eliminadas: " + metadataEntriesRemoved);
        } else {
            System.out.println("   ✅ No hay metadata obsoleto");
        }
        System.out.println();
    }

    /**
     * Garbage Collection de chunks huérfanos
     * Elimina chunks que no pertenecen a ningún PDF registrado
     * Se ejecuta cada 5 minutos
     */
    @Scheduled(fixedDelay = 300000, initialDelay = 60000)
    public void garbageCollection() {
        System.out.println("\n╔════════════════════════════════════════════════════════╗");
        System.out.println("║  🗑️  GARBAGE COLLECTION                               ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");

        List<String> healthyServers = masterService.getHealthyChunkservers();

        if (healthyServers.isEmpty()) {
            System.out.println("   ⚠️  No hay servidores activos");
            return;
        }

        // Construir conjunto de chunks válidos (que DEBERÍAN existir)
        Set<String> validChunks = new HashSet<>();
        List<PdfMetadata> allPdfs = masterService.listAllPdfs();

        for (PdfMetadata pdf : allPdfs) {
            for (ChunkLocation chunk : pdf.getChunks()) {
                String chunkId = pdf.getPdfId() + ":" + chunk.getChunkIndex();
                validChunks.add(chunkId);
            }
        }

        System.out.println("   📝 Chunks válidos esperados: " + validChunks.size());

        int orphansFound = 0;
        int orphansDeleted = 0;

        // Verificar cada chunkserver
        for (String server : healthyServers) {
            try {
                Map<String, List<Integer>> inventory = getServerInventory(server);

                for (Map.Entry<String, List<Integer>> entry : inventory.entrySet()) {
                    String pdfId = entry.getKey();

                    for (Integer chunkIndex : entry.getValue()) {
                        String chunkId = pdfId + ":" + chunkIndex;

                        // Si el chunk NO está en la lista de válidos, es huérfano
                        if (!validChunks.contains(chunkId)) {
                            System.out.println("   🗑️  Chunk huérfano detectado:");
                            System.out.println("      PDF: " + pdfId);
                            System.out.println("      Chunk: " + chunkIndex);
                            System.out.println("      Servidor: " + server);

                            orphansFound++;

                            // Eliminar chunk huérfano
                            if (deleteChunkFromServer(pdfId, chunkIndex, server)) {
                                orphansDeleted++;
                                totalGarbageCollected++;
                                System.out.println("      ✅ Eliminado exitosamente");
                            } else {
                                System.out.println("      ❌ Error al eliminar");
                            }
                        }
                    }
                }

            } catch (Exception e) {
                System.err.println("   ⚠️  Error verificando servidor " + server + ": " + e.getMessage());
            }
        }

        if (orphansFound > 0) {
            System.out.println("\n   📊 Resultado:");
            System.out.println("      Chunks huérfanos encontrados: " + orphansFound);
            System.out.println("      Chunks huérfanos eliminados: " + orphansDeleted);
            System.out.println("      Total GC histórico: " + totalGarbageCollected);
        } else {
            System.out.println("   ✅ No se encontraron chunks huérfanos");
        }
        System.out.println();
    }

    /**
     * Re-replica un chunk en nuevos servidores
     */
    private int replicateChunk(String pdfId, int chunkIndex,
                               List<ChunkLocation> existingReplicas,
                               int neededReplicas,
                               List<String> healthyServers) {

        // Encontrar una réplica fuente saludable
        ChunkLocation source = existingReplicas.stream()
                .filter(r -> healthyServers.contains(r.getChunkserverUrl()))
                .findFirst()
                .orElse(null);

        if (source == null) {
            System.err.println("      ❌ No hay réplica fuente disponible");
            return 0;
        }

        // Seleccionar servidores destino (que no tengan ya este chunk)
        Set<String> serversWithChunk = existingReplicas.stream()
                .map(ChunkLocation::getChunkserverUrl)
                .collect(Collectors.toSet());

        List<String> targetServers = healthyServers.stream()
                .filter(s -> !serversWithChunk.contains(s))
                .limit(neededReplicas)
                .collect(Collectors.toList());

        if (targetServers.isEmpty()) {
            System.err.println("      ⚠️  No hay servidores disponibles para replicar");
            return 0;
        }

        int created = 0;

        try {
            // Leer chunk desde la fuente
            byte[] chunkData = readChunk(pdfId, chunkIndex, source.getChunkserverUrl());

            // Copiar a cada servidor destino
            for (String targetServer : targetServers) {
                try {
                    writeChunk(pdfId, chunkIndex, chunkData, targetServer);

                    // Calcular siguiente índice de réplica
                    int nextReplicaIndex = existingReplicas.stream()
                                                   .mapToInt(ChunkLocation::getReplicaIndex)
                                                   .max()
                                                   .orElse(-1) + 1;

                    // Actualizar metadatos en el Master
                    ChunkLocation newReplica = new ChunkLocation(chunkIndex, targetServer, nextReplicaIndex);
                    masterService.addChunkReplica(pdfId, newReplica);

                    System.out.println("      ✅ Nueva réplica creada en: " + targetServer);
                    created++;

                } catch (Exception e) {
                    System.err.println("      ❌ Error replicando a " + targetServer + ": " + e.getMessage());
                }
            }

        } catch (Exception e) {
            System.err.println("      ❌ Error leyendo chunk fuente: " + e.getMessage());
        }

        return created;
    }

    /**
     * Verifica si un chunk existe en un chunkserver
     */
    private boolean chunkExists(String pdfId, int chunkIndex, String chunkserverUrl) {
        try {
            String url = chunkserverUrl + "/api/chunk/exists?pdfId=" + pdfId +
                         "&chunkIndex=" + chunkIndex;

            @SuppressWarnings("unchecked")
            Map<String, Object> response = restTemplate.getForObject(url, Map.class);

            return response != null && Boolean.TRUE.equals(response.get("exists"));
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Repara un chunk faltante copiándolo desde otra réplica
     */
    private boolean repairChunk(String pdfId, int chunkIndex, String targetServer,
                                List<ChunkLocation> replicas) {
        System.out.println("      🔧 Intentando reparar...");

        List<String> healthyServers = masterService.getHealthyChunkservers();

        // Buscar réplica fuente disponible
        for (ChunkLocation replica : replicas) {
            String sourceServer = replica.getChunkserverUrl();

            if (sourceServer.equals(targetServer)) continue;
            if (!healthyServers.contains(sourceServer)) continue;

            if (chunkExists(pdfId, chunkIndex, sourceServer)) {
                try {
                    byte[] chunkData = readChunk(pdfId, chunkIndex, sourceServer);
                    writeChunk(pdfId, chunkIndex, chunkData, targetServer);

                    System.out.println("         ✅ Reparado desde " + sourceServer);
                    return true;

                } catch (Exception e) {
                    System.err.println("         ⚠️  Fallo copiando desde " + sourceServer);
                }
            }
        }

        System.err.println("         ❌ No se pudo reparar");
        return false;
    }

    /**
     * Lee un chunk desde un chunkserver
     */
    private byte[] readChunk(String pdfId, int chunkIndex, String chunkserverUrl) {
        String url = chunkserverUrl + "/api/chunk/read?pdfId=" + pdfId +
                     "&chunkIndex=" + chunkIndex;

        ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error leyendo chunk");
        }

        Map<String, Object> body = response.getBody();
        String base64Data = (String) body.get("data");

        return Base64.getDecoder().decode(base64Data);
    }

    /**
     * Escribe un chunk a un chunkserver
     */
    private void writeChunk(String pdfId, int chunkIndex, byte[] data, String chunkserverUrl) {
        String url = chunkserverUrl + "/api/chunk/write";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> request = new HashMap<>();
        request.put("pdfId", pdfId);
        request.put("chunkIndex", chunkIndex);
        request.put("data", Base64.getEncoder().encodeToString(data));

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);
        restTemplate.postForEntity(url, entity, String.class);
    }

    /**
     * Obtiene inventario de un chunkserver
     */
    private Map<String, List<Integer>> getServerInventory(String chunkserverUrl) {
        try {
            String url = chunkserverUrl + "/api/chunk/inventory";

            @SuppressWarnings("unchecked")
            Map<String, List<Integer>> inventory = restTemplate.getForObject(url, Map.class);

            return inventory != null ? inventory : new HashMap<>();
        } catch (Exception e) {
            return new HashMap<>();
        }
    }

    /**
     * Elimina un chunk de un chunkserver
     */
    private boolean deleteChunkFromServer(String pdfId, int chunkIndex, String chunkserverUrl) {
        try {
            String url = chunkserverUrl + "/api/chunk/delete?pdfId=" + pdfId +
                         "&chunkIndex=" + chunkIndex;

            restTemplate.delete(url);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Agrupa chunks por índice
     */
    private Map<Integer, List<ChunkLocation>> groupByIndex(List<ChunkLocation> chunks) {
        Map<Integer, List<ChunkLocation>> grouped = new HashMap<>();
        for (ChunkLocation chunk : chunks) {
            grouped.computeIfAbsent(chunk.getChunkIndex(), k -> new ArrayList<>())
                    .add(chunk);
        }
        return grouped;
    }

    /**
     * Obtiene estadísticas del monitor
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalChecks", totalChecks);
        stats.put("totalRepairs", totalRepairs);
        stats.put("totalReReplications", totalReReplications);
        stats.put("totalGarbageCollected", totalGarbageCollected);
        stats.put("totalOverReplicasRemoved", totalOverReplicasRemoved);
        return stats;
    }
}