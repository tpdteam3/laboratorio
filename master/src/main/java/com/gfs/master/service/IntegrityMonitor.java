package com.gfs.master.service;

import com.gfs.master.model.ChunkLocation;
import com.gfs.master.model.PdfMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;

/**
 * Servicio de integridad con re-replicación inteligente
 * Detecta chunks faltantes y crea nuevas réplicas en servidores saludables
 */
@Service
public class IntegrityMonitor {

    @Autowired
    private MasterService masterService;

    private final RestTemplate restTemplate = new RestTemplate();
    private long totalRepairs = 0;
    private long totalChecks = 0;

    /**
     * Verifica integridad cada 30 segundos
     */
    @Scheduled(fixedDelay = 30000, initialDelay = 10000)
    public void checkIntegrity() {
        totalChecks++;

        System.out.println("\n[INTEGRITY] Verificando integridad del sistema...");

        List<String> healthyServers = masterService.getHealthyChunkservers();
        if (healthyServers.isEmpty()) {
            System.out.println("   [WARN] No hay servidores activos para verificar");
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

                // Contar réplicas disponibles
                int availableReplicas = 0;
                List<ChunkLocation> unavailableReplicas = new ArrayList<>();

                for (ChunkLocation replica : replicas) {
                    String serverUrl = replica.getChunkserverUrl();

                    if (!healthyServers.contains(serverUrl)) {
                        // Servidor caído
                        unavailableReplicas.add(replica);
                        System.out.println("   [ERROR] Réplica en servidor caído:");
                        System.out.println("      PDF: " + pdf.getPdfId());
                        System.out.println("      Chunk: " + chunkIndex);
                        System.out.println("      Servidor caído: " + serverUrl);
                        issuesFound++;
                    } else if (!chunkExists(pdf.getPdfId(), chunkIndex, serverUrl)) {
                        // Chunk faltante en servidor activo
                        unavailableReplicas.add(replica);
                        System.out.println("   [ERROR] Chunk faltante en servidor activo:");
                        System.out.println("      PDF: " + pdf.getPdfId());
                        System.out.println("      Chunk: " + chunkIndex);
                        System.out.println("      Servidor: " + serverUrl);
                        issuesFound++;
                    } else {
                        availableReplicas++;
                    }
                }

                // Si hay réplicas no disponibles, intentar re-replicar
                if (!unavailableReplicas.isEmpty() && availableReplicas > 0) {
                    for (ChunkLocation unavailable : unavailableReplicas) {
                        if (reReplicateChunk(pdf.getPdfId(), chunkIndex, replicas,
                                unavailable, healthyServers)) {
                            issuesRepaired++;
                            totalRepairs++;
                        }
                    }
                }
            }
        }

        if (issuesFound > 0) {
            System.out.println("\n[INTEGRITY] Resultado:");
            System.out.println("   Problemas detectados: " + issuesFound);
            System.out.println("   Problemas reparados: " + issuesRepaired);
            System.out.println("   Total reparaciones históricas: " + totalRepairs);
        } else {
            System.out.println("   [OK] Sistema íntegro - sin problemas detectados");
        }
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
     * Re-replica un chunk faltante en un nuevo servidor saludable
     */
    private boolean reReplicateChunk(String pdfId, int chunkIndex,
                                     List<ChunkLocation> replicas,
                                     ChunkLocation unavailableReplica,
                                     List<String> healthyServers) {

        System.out.println("   [REPAIR] Iniciando re-replicación...");
        System.out.println("      Réplica perdida: " + unavailableReplica.getChunkserverUrl());

        // 1. Encontrar servidor fuente con el chunk disponible
        String sourceServer = null;
        for (ChunkLocation replica : replicas) {
            String serverUrl = replica.getChunkserverUrl();

            if (!serverUrl.equals(unavailableReplica.getChunkserverUrl()) &&
                healthyServers.contains(serverUrl) &&
                chunkExists(pdfId, chunkIndex, serverUrl)) {

                sourceServer = serverUrl;
                break;
            }
        }

        if (sourceServer == null) {
            System.err.println("      [ERROR] No hay réplicas disponibles como fuente");
            return false;
        }

        System.out.println("      Servidor fuente encontrado: " + sourceServer);

        // 2. Encontrar servidor destino (saludable y que no tenga ya este chunk)
        String targetServer = null;
        Set<String> serversWithChunk = new HashSet<>();
        for (ChunkLocation replica : replicas) {
            serversWithChunk.add(replica.getChunkserverUrl());
        }

        for (String server : healthyServers) {
            if (!serversWithChunk.contains(server) ||
                server.equals(unavailableReplica.getChunkserverUrl())) {

                // Este servidor está saludable y no tiene el chunk (o es el caído)
                if (!server.equals(unavailableReplica.getChunkserverUrl()) ||
                    healthyServers.contains(server)) {
                    targetServer = server;
                    break;
                }
            }
        }

        // Si no encontramos un servidor nuevo, usar cualquier servidor saludable
        if (targetServer == null) {
            for (String server : healthyServers) {
                if (!server.equals(sourceServer)) {
                    targetServer = server;
                    break;
                }
            }
        }

        if (targetServer == null) {
            System.err.println("      [ERROR] No hay servidores disponibles para re-replicación");
            return false;
        }

        System.out.println("      Servidor destino seleccionado: " + targetServer);

        // 3. Copiar chunk de fuente a destino
        try {
            // Leer chunk desde fuente
            byte[] chunkData = readChunk(pdfId, chunkIndex, sourceServer);
            System.out.println("      Chunk leído desde fuente (" + chunkData.length + " bytes)");

            // Escribir en destino
            writeChunk(pdfId, chunkIndex, chunkData, targetServer);
            System.out.println("      [OK] Chunk escrito en servidor destino");

            // 4. Actualizar metadatos (cambiar la ubicación de la réplica)
            masterService.updateChunkLocation(pdfId, chunkIndex,
                    unavailableReplica.getChunkserverUrl(),
                    targetServer);

            System.out.println("      [OK] Re-replicación completada:");
            System.out.println("         " + sourceServer + " → " + targetServer);

            return true;

        } catch (Exception e) {
            System.err.println("      [ERROR] Fallo en re-replicación: " + e.getMessage());
            return false;
        }
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
     * Obtiene estadísticas del monitor
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalChecks", totalChecks);
        stats.put("totalRepairs", totalRepairs);
        return stats;
    }
}