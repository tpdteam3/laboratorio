package com.gfs.chunkserver.service;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * Servicio que envía heartbeats al Master cada 15 segundos
 */
@Service
public class HeartbeatService {

    @Value("${server.port}")
    private int serverPort;

    @Value("${chunkserver.id}")
    private String chunkserverId;

    @Value("${chunkserver.master-url}")
    private String masterUrl;

    @Value("${chunkserver.hostname:localhost}")
    private String hostname;

    @Autowired
    private StorageService storageService;

    private final RestTemplate restTemplate = new RestTemplate();
    private String chunkserverUrl;
    private int consecutiveFailures = 0;

    @PostConstruct
    public void init() {
        chunkserverUrl = "http://" + hostname + ":" + serverPort;

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  💓 HEARTBEAT SERVICE ACTIVADO                        ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   Chunkserver URL: " + chunkserverUrl);
        System.out.println("   Master URL: " + masterUrl);
        System.out.println("   Intervalo: 15 segundos");
        System.out.println();

        // Primer heartbeat inmediato para registro rápido
        sendHeartbeat();
    }

    /**
     * Envía heartbeat cada 15 segundos
     */
    @Scheduled(fixedDelay = 15000, initialDelay = 15000)
    public void sendHeartbeat() {
        try {
            String heartbeatUrl = masterUrl + "/api/master/heartbeat";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            // Construir payload con inventario actual
            Map<String, Object> heartbeat = new HashMap<>();
            heartbeat.put("chunkserverId", chunkserverId);
            heartbeat.put("url", chunkserverUrl);
            heartbeat.put("status", "UP");
            heartbeat.put("timestamp", System.currentTimeMillis());
            heartbeat.put("inventory", storageService.getInventory());

            // Agregar métricas
            Map<String, Object> stats = storageService.getStats();
            heartbeat.put("totalChunks", stats.get("totalChunks"));
            heartbeat.put("storageUsedMB", stats.get("storageUsedMB"));

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(heartbeat, headers);
            restTemplate.postForEntity(heartbeatUrl, entity, Map.class);

            if (consecutiveFailures > 0) {
                System.out.println("✅ [" + chunkserverId + "] Conexión con Master restaurada");
            }
            consecutiveFailures = 0;

        } catch (Exception e) {
            consecutiveFailures++;

            if (consecutiveFailures == 1) {
                System.err.println("⚠️  [" + chunkserverId + "] Error enviando heartbeat: " +
                                   e.getMessage());
            }

            if (consecutiveFailures >= 3) {
                System.err.println("❌ [" + chunkserverId + "] No se puede contactar al Master (" +
                                   consecutiveFailures + " intentos)");
            }
        }
    }
}