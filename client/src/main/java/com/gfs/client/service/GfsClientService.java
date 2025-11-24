package com.gfs.client.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.util.*;

@Service
public class GfsClientService {

    @Value("${gfs.master-url}")
    private String masterUrl;

    @Value("${gfs.chunk-size:32768}")
    private int CHUNK_SIZE;

    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * Sube un PDF al sistema GFS
     */
    public String uploadPdf(MultipartFile file) throws Exception {
        String pdfId = file.getOriginalFilename();
        byte[] data = file.getBytes();

        // 1. Solicitar plan de upload al Master
        Map<String, Object> planRequest = new HashMap<>();
        planRequest.put("pdfId", pdfId);
        planRequest.put("size", data.length);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(planRequest, headers);

        ResponseEntity<Map> planResponse = restTemplate.postForEntity(
                masterUrl + "/api/master/upload",
                entity,
                Map.class
        );

        if (!planResponse.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error obteniendo plan de upload del Master");
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> chunks =
                (List<Map<String, Object>>) planResponse.getBody().get("chunks");

        // 2. Fragmentar y enviar chunks
        Map<Integer, List<Map<String, Object>>> chunksByIndex = new HashMap<>();
        for (Map<String, Object> chunk : chunks) {
            int chunkIndex = (Integer) chunk.get("chunkIndex");
            chunksByIndex.computeIfAbsent(chunkIndex, k -> new ArrayList<>()).add(chunk);
        }

        System.out.println("   📦 Enviando " + chunksByIndex.size() + " chunks...");

        int successCount = 0;
        int failCount = 0;

        for (Map.Entry<Integer, List<Map<String, Object>>> entry : chunksByIndex.entrySet()) {
            int chunkIndex = entry.getKey();
            List<Map<String, Object>> replicas = entry.getValue();

            // Extraer datos del chunk
            int offset = chunkIndex * CHUNK_SIZE;
            int length = Math.min(CHUNK_SIZE, data.length - offset);
            byte[] chunkData = Arrays.copyOfRange(data, offset, offset + length);
            String base64Data = Base64.getEncoder().encodeToString(chunkData);

            // Enviar a cada réplica
            for (Map<String, Object> replica : replicas) {
                String chunkserverUrl = (String) replica.get("chunkserverUrl");
                int replicaIndex = (Integer) replica.get("replicaIndex");

                try {
                    writeChunkToServer(pdfId, chunkIndex, base64Data, chunkserverUrl);

                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.out.println("      ✅ Chunk " + chunkIndex + " [" + replicaType + "] → " +
                                       chunkserverUrl);
                    successCount++;

                } catch (Exception e) {
                    System.err.println("      ❌ Error enviando chunk " + chunkIndex +
                                       " a " + chunkserverUrl + ": " + e.getMessage());
                    failCount++;
                }
            }
        }

        System.out.println("\n   📊 Resultado:");
        System.out.println("      ✅ Exitosos: " + successCount);
        if (failCount > 0) {
            System.out.println("      ❌ Fallidos: " + failCount);
        }

        return pdfId;
    }

    /**
     * Descarga un PDF desde el sistema GFS
     */
    public byte[] downloadPdf(String pdfId) throws Exception {
        // 1. Obtener metadatos del Master
        ResponseEntity<Map> metadataResponse = restTemplate.getForEntity(
                masterUrl + "/api/master/metadata/" + pdfId,
                Map.class
        );

        if (!metadataResponse.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("PDF no encontrado: " + pdfId);
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = metadataResponse.getBody();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> allChunks =
                (List<Map<String, Object>>) metadata.get("chunks");

        // 2. Agrupar chunks por índice
        Map<Integer, List<Map<String, Object>>> chunksByIndex = new HashMap<>();
        for (Map<String, Object> chunk : allChunks) {
            int chunkIndex = (Integer) chunk.get("chunkIndex");
            chunksByIndex.computeIfAbsent(chunkIndex, k -> new ArrayList<>()).add(chunk);
        }

        System.out.println("   📦 Descargando " + chunksByIndex.size() + " chunks...");

        // 3. Descargar chunks en orden
        List<byte[]> chunkDataList = new ArrayList<>(chunksByIndex.size());

        for (int i = 0; i < chunksByIndex.size(); i++) {
            List<Map<String, Object>> replicas = chunksByIndex.get(i);

            if (replicas == null || replicas.isEmpty()) {
                throw new RuntimeException("Chunk " + i + " no disponible");
            }

            byte[] chunkData = null;
            int attemptCount = 0;

            // Intentar leer desde cualquier réplica disponible
            for (Map<String, Object> replica : replicas) {
                attemptCount++;
                String chunkserverUrl = (String) replica.get("chunkserverUrl");
                int replicaIndex = (Integer) replica.get("replicaIndex");

                try {
                    // Comunicacion con el chunkserver
                    chunkData = readChunkFromServer(pdfId, i, chunkserverUrl);

                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.out.println("      ✅ Chunk " + i + " [" + replicaType + "] ← " +
                                       chunkserverUrl);

                    if (attemptCount > 1) {
                        System.out.println("         🔄 FALLBACK usado (intento #" + attemptCount + ")");
                    }

                    break;

                } catch (Exception e) {
                    String replicaType = replicaIndex == 0 ? "PRIMARIA" : "RÉPLICA " + replicaIndex;
                    System.err.println("      ⚠️  Chunk " + i + " [" + replicaType + "] fallo en " +
                                       chunkserverUrl);

                    if (attemptCount < replicas.size()) {
                        System.out.println("         🔄 Intentando siguiente réplica...");
                    }
                }
            }

            if (chunkData == null) {
                throw new RuntimeException("No se pudo leer chunk " + i +
                                           " desde ninguna réplica");
            }

            chunkDataList.add(chunkData);
        }

        // 4. Ensamblar PDF completo
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (byte[] chunk : chunkDataList) {
            outputStream.write(chunk);
        }

        return outputStream.toByteArray();
    }

    /**
     * Lista todos los PDFs
     */
    public List<Map<String, Object>> listPdfs() {
        try {
            ResponseEntity<List> response = restTemplate.getForEntity(
                    masterUrl + "/api/master/pdfs",
                    List.class
            );

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> pdfs = response.getBody();

            return pdfs != null ? pdfs : new ArrayList<>();

        } catch (Exception e) {
            throw new RuntimeException("Error obteniendo lista de PDFs: " + e.getMessage());
        }
    }

    /**
     * Obtiene estado del sistema
     */
    public Map<String, Object> getSystemStatus() {
        try {
            ResponseEntity<Map> response = restTemplate.getForEntity(
                    masterUrl + "/api/master/status",
                    Map.class
            );

            return response.getBody();

        } catch (Exception e) {
            throw new RuntimeException("Error obteniendo estado del sistema: " + e.getMessage());
        }
    }

    /**
     * Escribe un chunk a un chunkserver
     */
    private void writeChunkToServer(String pdfId, int chunkIndex, String base64Data,
                                    String chunkserverUrl) {
        String url = chunkserverUrl + "/api/chunk/write";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> request = new HashMap<>();
        request.put("pdfId", pdfId);
        request.put("chunkIndex", chunkIndex);
        request.put("data", base64Data);

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);
        restTemplate.postForEntity(url, entity, String.class);
    }

    /**
     * Lee un chunk desde un chunkserver
     */
    private byte[] readChunkFromServer(String pdfId, int chunkIndex, String chunkserverUrl) {
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
}
