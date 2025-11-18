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
 * Servicio simplificado de integridad
 * Detecta chunks faltantes y los repara
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

        System.out.println("\n🔍 [INTEGRITY] Verificando integridad del sistema...");

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
            System.out.println("\n📊 [INTEGRITY] Resultado:");
            System.out.println("   ❌ Problemas detectados: " + issuesFound);
            System.out.println("   ✅ Problemas reparados: " + issuesRepaired);
            System.out.println("   🔧 Total reparaciones históricas: " + totalRepairs);
        } else {
            System.out.println("   ✅ Sistema íntegro - sin problemas detectados");
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
     * Repara un chunk faltante copiándolo desde otra réplica
     */
    private boolean repairChunk(String pdfId, int chunkIndex, String targetServer,
                                List<ChunkLocation> replicas) {
        System.out.println("   🔧 Intentando reparar...");

        List<String> healthyServers = masterService.getHealthyChunkservers();

        // Buscar réplica fuente disponible
        for (ChunkLocation replica : replicas) {
            String sourceServer = replica.getChunkserverUrl();

            if (sourceServer.equals(targetServer)) continue; // No copiar de sí mismo
            if (!healthyServers.contains(sourceServer)) continue; // Servidor caído

            if (chunkExists(pdfId, chunkIndex, sourceServer)) {
                try {
                    // Leer chunk desde fuente
                    byte[] chunkData = readChunk(pdfId, chunkIndex, sourceServer);

                    // Escribir en destino
                    writeChunk(pdfId, chunkIndex, chunkData, targetServer);

                    System.out.println("      ✅ Chunk reparado desde " + sourceServer);
                    return true;

                } catch (Exception e) {
                    System.err.println("      ⚠️  Fallo copiando desde " + sourceServer +
                                       ": " + e.getMessage());
                }
            }
        }

        System.err.println("      ❌ No se pudo reparar - no hay réplicas disponibles");
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
     * Obtiene estadísticas del monitor
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalChecks", totalChecks);
        stats.put("totalRepairs", totalRepairs);
        return stats;
    }
}