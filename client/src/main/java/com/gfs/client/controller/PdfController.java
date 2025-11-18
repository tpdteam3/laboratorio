package com.gfs.client.controller;

import com.gfs.client.service.GfsClientService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/pdf")
@CrossOrigin(origins = "*")
public class PdfController {

    @Autowired
    private GfsClientService gfsClientService;

    /**
     * Sube un PDF al sistema GFS
     */
    @PostMapping("/upload")
    public ResponseEntity<Map<String, String>> uploadPdf(@RequestParam("file") MultipartFile file) {
        try {
            if (file.isEmpty()) {
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "Archivo vacío");
                return ResponseEntity.badRequest().body(error);
            }

            // Validar que sea PDF
            String contentType = file.getContentType();
            if (contentType == null || !contentType.equals("application/pdf")) {
                Map<String, String> error = new HashMap<>();
                error.put("status", "error");
                error.put("message", "Solo se permiten archivos PDF");
                return ResponseEntity.badRequest().body(error);
            }

            System.out.println("\n╔════════════════════════════════════════════════════════╗");
            System.out.println("║  📤 CLIENTE: SUBIENDO PDF                             ║");
            System.out.println("╚════════════════════════════════════════════════════════╝");
            System.out.println("   Archivo: " + file.getOriginalFilename());
            System.out.println("   Tamaño: " + file.getSize() + " bytes");

            String pdfId = gfsClientService.uploadPdf(file);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "PDF subido exitosamente");
            response.put("pdfId", pdfId);

            System.out.println("   ✅ Upload completado: " + pdfId);
            System.out.println();

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", "Error al subir PDF: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Descarga un PDF desde el sistema GFS
     */
    @GetMapping("/download/{pdfId}")
    public ResponseEntity<byte[]> downloadPdf(@PathVariable String pdfId) {
        try {
            System.out.println("\n╔════════════════════════════════════════════════════════╗");
            System.out.println("║  📥 CLIENTE: DESCARGANDO PDF                          ║");
            System.out.println("╚════════════════════════════════════════════════════════╝");
            System.out.println("   PDF ID: " + pdfId);

            byte[] pdfData = gfsClientService.downloadPdf(pdfId);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_PDF);
            headers.setContentLength(pdfData.length);
            headers.setContentDispositionFormData("inline", pdfId + ".pdf");

            System.out.println("   ✅ Download completado: " + pdfData.length + " bytes");
            System.out.println();

            return new ResponseEntity<>(pdfData, headers, HttpStatus.OK);

        } catch (RuntimeException e) {
            System.err.println("   ❌ Error: " + e.getMessage());
            System.out.println();
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Lista todos los PDFs almacenados
     */
    @GetMapping("/list")
    public ResponseEntity<Map<String, Object>> listPdfs() {
        try {
            List<Map<String, Object>> pdfs = gfsClientService.listPdfs();

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("pdfs", pdfs);
            response.put("total", pdfs.size());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Obtiene estado del sistema GFS
     */
    @GetMapping("/system-status")
    public ResponseEntity<Map<String, Object>> getSystemStatus() {
        try {
            Map<String, Object> status = gfsClientService.getSystemStatus();
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }
}