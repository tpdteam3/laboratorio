package com.gfs.master.model;

public class ChunkLocation {
    private int chunkIndex;
    private String chunkserverUrl;
    private int replicaIndex;

    public ChunkLocation() {
    }

    public ChunkLocation(int chunkIndex, String chunkserverUrl, int replicaIndex) {
        this.chunkIndex = chunkIndex;
        this.chunkserverUrl = chunkserverUrl;
        this.replicaIndex = replicaIndex;
    }

    // Getters y Setters
    public int getChunkIndex() {
        return chunkIndex;
    }

    public void setChunkIndex(int chunkIndex) {
        this.chunkIndex = chunkIndex;
    }

    public String getChunkserverUrl() {
        return chunkserverUrl;
    }

    public void setChunkserverUrl(String chunkserverUrl) {
        this.chunkserverUrl = chunkserverUrl;
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public void setReplicaIndex(int replicaIndex) {
        this.replicaIndex = replicaIndex;
    }

    @Override
    public String toString() {
        return "ChunkLocation{" +
               "chunkIndex=" + chunkIndex +
               ", chunkserverUrl='" + chunkserverUrl + '\'' +
               ", replicaIndex=" + replicaIndex +
               '}';
    }
}