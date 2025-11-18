package com.gfs.chunkserver.service;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * Servicio que auto-registra el chunkserver con el Master al iniciar
 * Con reintentos y manejo de errores mejorado
 */
@Service
public class RegistrationService {

    @Value("${server.port}")
    private int serverPort;

    @Value("${chunkserver.id}")
    private String chunkserverId;

    @Value("${chunkserver.master-url}")
    private String masterUrl;

    @Value("${chunkserver.hostname:localhost}")
    private String hostname;

    private final RestTemplate restTemplate = new RestTemplate();

    @PostConstruct
    public void autoRegister() {
        String chunkserverUrl = "http://" + hostname + ":" + serverPort;

        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  📡 AUTO-REGISTRO CON MASTER                          ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
        System.out.println("   Registrando: " + chunkserverUrl);
        System.out.println("   ID: " + chunkserverId);
        System.out.println("   Master: " + masterUrl);

        // Intentar registro con reintentos
        int maxRetries = 10;
        int attempt = 0;
        boolean registered = false;

        while (attempt < maxRetries && !registered) {
            attempt++;

            try {
                System.out.println("\n   Intento " + attempt + "/" + maxRetries + "...");

                String registerUrl = masterUrl + "/api/master/register";

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);

                Map<String, String> request = new HashMap<>();
                request.put("url", chunkserverUrl);
                request.put("id", chunkserverId);

                HttpEntity<Map<String, String>> entity = new HttpEntity<>(request, headers);
                restTemplate.postForEntity(registerUrl, entity, Map.class);

                System.out.println("   ✅ Registro exitoso!");
                System.out.println();
                registered = true;

            } catch (Exception e) {
                System.err.println("   ❌ Error en intento " + attempt + ": " + e.getMessage());

                if (attempt < maxRetries) {
                    int waitTime = Math.min(5, attempt); // Backoff progresivo hasta 5 segundos
                    try {
                        System.out.println("   ⏳ Reintentando en " + waitTime + " segundos...");
                        Thread.sleep(waitTime * 1000L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        if (!registered) {
            System.err.println("\n╔════════════════════════════════════════════════════════╗");
            System.err.println("║  ⚠️  ADVERTENCIA: REGISTRO INICIAL FALLIDO            ║");
            System.err.println("╚════════════════════════════════════════════════════════╝");
            System.err.println("   ❌ No se pudo registrar con el Master");
            System.err.println("   🔄 El HeartbeatService intentará reconectar automáticamente");
            System.err.println("   💡 El chunkserver continuará ejecutándose");
            System.err.println("   ⚙️  Verifica que el Master esté disponible en: " + masterUrl);
            System.err.println();
        } else {
            System.out.println("╔════════════════════════════════════════════════════════╗");
            System.out.println("║  ✅ CHUNKSERVER LISTO                                 ║");
            System.out.println("╚════════════════════════════════════════════════════════╝");
            System.out.println("   🟢 Conectado al Master");
            System.out.println("   💓 Heartbeats activos (cada 15s)");
            System.out.println();
        }
    }
}