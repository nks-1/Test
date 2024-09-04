package com.example.chunkuploads3;

import java.io.IOException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import software.amazon.awssdk.services.s3.model.UploadPartResponse;

@RestController
@RequestMapping("/api/upload")
public class ChunkUploadController {

    private final ChunkUploadService chunkUploadService;

    public ChunkUploadController(ChunkUploadService chunkUploadService) {
        this.chunkUploadService = chunkUploadService;
    }

    @PostMapping("/initiate")
    public ResponseEntity<String> initiateUpload(@RequestParam String fileKey) {
        String uploadId = chunkUploadService.initiateMultipartUpload(fileKey);
        return ResponseEntity.ok(uploadId);
    }

    @PostMapping("/chunk")
    public ResponseEntity<?> uploadChunk(
            @RequestParam String fileKey,
            @RequestParam int chunkNumber,
            @RequestParam MultipartFile chunk
    ) {
        try {
        	 chunkUploadService.uploadChunk(fileKey, chunkNumber, chunk);
        	
            return ResponseEntity.ok("Chunk " + chunkNumber + " uploaded successfully");
        } catch (IOException e) {
            return ResponseEntity.status(500).body("Failed to upload chunk " + chunkNumber);
        }
    }

    @PostMapping("/complete")
    public ResponseEntity<String> completeUpload(@RequestParam String fileKey, @RequestParam int totalChunks) {
        chunkUploadService.completeUpload(fileKey, totalChunks);
        return ResponseEntity.ok("File upload completed and chunks deleted successfully");
    }
}
