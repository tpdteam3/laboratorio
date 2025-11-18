package com.gfs.master.controller;

import com.gfs.master.model.PdfMetadata;
import com.gfs.master.service.MasterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/master")
@CrossOrigin(origins = "*")
public class MasterController {

    @Autowired
    private MasterService masterService;

    /**
     * Planifica el upload de un PDF
     * Retorna las ubicaciones donde guardar cada chunk
     */
    @PostMapping("/upload")
    public ResponseEntity<Map<String, Object>> planUpload(@RequestBody Map<String, Object> request) {
        try {
            String pdfId = (String) request.get("pdfId");
            Long size = ((Number) request.get("size")).longValue();

            System.out.println("\n========================================================");
            System.out.println("  PLANIFICANDO UPLOAD DE PDF");
            System.out.println("========================================================");
            System.out.println("   PDF ID: " + pdfId);
            System.out.println("   Tamano: " + size + " bytes");

            PdfMetadata metadata = masterService.planUpload(pdfId, size);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("pdfId", metadata.getPdfId());
            response.put("chunks", metadata.getChunks());
            response.put("replicationFactor", 3);

            System.out.println("   Chunks: " + metadata.getChunks().size() / 3);
            System.out.println("   Replicas totales: " + metadata.getChunks().size());
            System.out.println();

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            System.err.println("[ERROR] ERROR en planUpload: " + e.getMessage());
            e.printStackTrace();
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Obtiene metadatos de un PDF (ubicaciones de chunks)
     */
    @GetMapping("/metadata/{pdfId}")
    public ResponseEntity<Map<String, Object>> getMetadata(@PathVariable String pdfId) {
        try {
            PdfMetadata metadata = masterService.getMetadata(pdfId);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("pdfId", metadata.getPdfId());
            response.put("size", metadata.getSize());
            response.put("chunks", metadata.getChunks());

            return ResponseEntity.ok(response);

        } catch (RuntimeException e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "PDF no encontrado: " + pdfId);
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
        }
    }

    /**
     * Recibe heartbeats de chunkservers
     */
    @PostMapping("/heartbeat")
    public ResponseEntity<Map<String, Object>> receiveHeartbeat(@RequestBody Map<String, Object> heartbeat) {
        try {
            String url = (String) heartbeat.get("url");
            String chunkserverId = (String) heartbeat.get("chunkserverId");

            @SuppressWarnings("unchecked")
            Map<String, List<Integer>> inventory =
                    (Map<String, List<Integer>>) heartbeat.get("inventory");

            masterService.processHeartbeat(url, chunkserverId, inventory);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Heartbeat received");
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Registra un nuevo chunkserver
     */
    @PostMapping("/register")
    public ResponseEntity<Map<String, String>> registerChunkserver(@RequestBody Map<String, String> request) {
        try {
            String url = request.get("url");
            String id = request.get("id");

            masterService.registerChunkserver(url, id);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Chunkserver registrado");
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
        }
    }

    /**
     * Estado del sistema
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = masterService.getSystemStatus();
        return ResponseEntity.ok(status);
    }

    /**
     * Lista todos los PDFs almacenados
     */
    @GetMapping("/pdfs")
    public ResponseEntity<List<PdfMetadata>> listPdfs() {
        List<PdfMetadata> pdfs = masterService.listAllPdfs();
        return ResponseEntity.ok(pdfs);
    }

    /**
     * Elimina un PDF
     */
    @DeleteMapping("/pdf/{pdfId}")
    public ResponseEntity<Map<String, String>> deletePdf(@PathVariable String pdfId) {
        try {
            masterService.deletePdf(pdfId);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "PDF eliminado");
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }
}
