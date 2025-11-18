package com.gfs.master.model;

import java.util.ArrayList;
import java.util.List;

public class PdfMetadata {
    private String pdfId;
    private long size;
    private List<ChunkLocation> chunks;
    private long timestamp;

    public PdfMetadata() {
        this.chunks = new ArrayList<>();
        this.timestamp = System.currentTimeMillis();
    }

    public PdfMetadata(String pdfId, long size) {
        this();
        this.pdfId = pdfId;
        this.size = size;
    }

    // Getters y Setters
    public String getPdfId() {
        return pdfId;
    }

    public void setPdfId(String pdfId) {
        this.pdfId = pdfId;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public List<ChunkLocation> getChunks() {
        return chunks;
    }

    public void setChunks(List<ChunkLocation> chunks) {
        this.chunks = chunks;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
