package com.gfs.chunkserver.controller;

import com.gfs.chunkserver.service.StorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/chunk")
@CrossOrigin(origins = "*")
public class ChunkController {

    @Autowired
    private StorageService storageService;

    /**
     * Escribe un chunk en disco
     */
    @PostMapping("/write")
    public ResponseEntity<Map<String, String>> writeChunk(@RequestBody Map<String, Object> request) {
        try {
            String pdfId = (String) request.get("pdfId");
            Integer chunkIndex = (Integer) request.get("chunkIndex");
            String data = (String) request.get("data");

            if (pdfId == null || chunkIndex == null || data == null) {
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "Campos requeridos: pdfId, chunkIndex, data");
                return ResponseEntity.badRequest().body(error);
            }

            storageService.writeChunk(pdfId, chunkIndex, data);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Chunk guardado");

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Lee un chunk desde disco
     */
    @GetMapping("/read")
    public ResponseEntity<Map<String, Object>> readChunk(
            @RequestParam String pdfId,
            @RequestParam int chunkIndex) {
        try {
            byte[] data = storageService.readChunk(pdfId, chunkIndex);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("pdfId", pdfId);
            response.put("chunkIndex", chunkIndex);
            response.put("data", Base64.getEncoder().encodeToString(data));
            response.put("size", data.length);

            return ResponseEntity.ok(response);

        } catch (RuntimeException e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Verifica si un chunk existe
     */
    @GetMapping("/exists")
    public ResponseEntity<Map<String, Object>> chunkExists(
            @RequestParam String pdfId,
            @RequestParam int chunkIndex) {
        try {
            boolean exists = storageService.chunkExists(pdfId, chunkIndex);

            Map<String, Object> response = new HashMap<>();
            response.put("exists", exists);
            response.put("pdfId", pdfId);
            response.put("chunkIndex", chunkIndex);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Obtiene inventario de chunks almacenados
     */
    @GetMapping("/inventory")
    public ResponseEntity<Map<String, List<Integer>>> getInventory() {
        try {
            Map<String, List<Integer>> inventory = storageService.getInventory();
            return ResponseEntity.ok(inventory);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Elimina un chunk
     */
    @DeleteMapping("/delete")
    public ResponseEntity<Map<String, String>> deleteChunk(
            @RequestParam String pdfId,
            @RequestParam int chunkIndex) {
        try {
            storageService.deleteChunk(pdfId, chunkIndex);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Chunk eliminado");

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Obtiene estadísticas del chunkserver
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        try {
            Map<String, Object> stats = storageService.getStats();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
