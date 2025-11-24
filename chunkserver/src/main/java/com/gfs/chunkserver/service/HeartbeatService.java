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
 * Con reconexión automática cuando el Master se recupera
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
    private boolean wasDisconnected = false;

    @PostConstruct
    public void init() {
        chunkserverUrl = "http://" + hostname + ":" + serverPort;

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║     HEARTBEAT SERVICE ACTIVADO                         ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   Chunkserver URL: " + chunkserverUrl);
        System.out.println("   Master URL: " + masterUrl);
        System.out.println("   Intervalo: 15 segundos");
        System.out.println();

        // Envío inmediato de heartbeat para registro
        sendImmediateHeartbeat();
    }

    /**
     * Envía heartbeat inmediatamente al registrarse
     */
    public void sendImmediateHeartbeat() {
        System.out.println("🚀 [" + chunkserverId + "] Enviando heartbeat de registro...");
        sendHeartbeat();
    }

    /**
     * Envía heartbeat cada 15 segundos
     * Con lógica de reconexión automática
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

            // Reconexión exitosa después de fallo
            if (consecutiveFailures > 0 || wasDisconnected) {
                System.out.println("║      RECONEXIÓN EXITOSA AL MASTER        ║");
                System.out.println("   [" + chunkserverId + "] Master recuperado");
                System.out.println("   Conexión restaurada después de " + consecutiveFailures + " intentos");
                System.out.println();

                // Re-registrar para asegurar que el Master nos reconozca
                if (wasDisconnected) {
                    reregisterWithMaster();
                }

                wasDisconnected = false;
            }

            consecutiveFailures = 0;

        } catch (Exception e) {
            consecutiveFailures++;

            if (consecutiveFailures == 1) {
                System.err.println("CONEXIÓN CON MASTER PERDIDA");
                System.err.println("[" + chunkserverId + "] Error enviando heartbeat");
                System.err.println("Intentando reconectar automáticamente...");
                System.err.println();
            }

            if (consecutiveFailures >= 3) {
                wasDisconnected = true;
                if (consecutiveFailures % 4 == 0) { // Log cada 4 intentos (cada minuto)
                    System.err.println("[" + chunkserverId + "] Master aún no disponible (" +
                                       consecutiveFailures + " intentos fallidos)");
                    System.err.println("Continuando intentos de reconexión...");
                }
            }
        }
    }

    /**
     * Re-registra el chunkserver con el Master después de una desconexión
     */
    private void reregisterWithMaster() {
        try {
            System.out.println("   📡 Re-registrando con el Master...");

            String registerUrl = masterUrl + "/api/master/register";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, String> request = new HashMap<>();
            request.put("url", chunkserverUrl);
            request.put("id", chunkserverId);

            HttpEntity<Map<String, String>> entity = new HttpEntity<>(request, headers);
            restTemplate.postForEntity(registerUrl, entity, Map.class);

            System.out.println("   ✅ Re-registro completado exitosamente");

        } catch (Exception e) {
            System.err.println("   ⚠️  Error en re-registro (se reintentará): " + e.getMessage());
        }
    }
}