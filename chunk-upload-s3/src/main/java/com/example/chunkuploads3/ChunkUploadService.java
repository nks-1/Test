package com.example.chunkuploads3;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ChunkUploadService {

    private final S3Client s3Client;
    private final String bucketName = "av-sptm-sem-dev";
    private final Map<String, String> uploadIdMap = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, String>> etagMap = new ConcurrentHashMap<>();
    public ChunkUploadService(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public String initiateMultipartUpload(String fileKey) {
        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(fileKey)
                .build();

        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(createMultipartUploadRequest);
        uploadIdMap.put(fileKey, response.uploadId());
        etagMap.put(fileKey, new ConcurrentHashMap<>()); 
        return response.uploadId();
    }

    public void uploadChunk(String fileKey, int chunkNumber, MultipartFile chunk) throws IOException {
      String uploadId = uploadIdMap.get(fileKey);
        UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .bucket(bucketName)
                .key(fileKey)
                .uploadId(uploadId)
                .partNumber(chunkNumber)
                .build();

        UploadPartResponse response= s3Client.uploadPart(uploadPartRequest, RequestBody.fromBytes(chunk.getBytes()));
        etagMap.get(fileKey).put(chunkNumber, response.eTag());
    }

    public void completeUpload(String fileKey, int totalChunks) {
        String uploadId = uploadIdMap.get(fileKey);
        Map<Integer, String> partETags = etagMap.get(fileKey);
        List<CompletedPart> completedParts = new ArrayList<>();
        for (int i = 1; i <= totalChunks; i++) {
            completedParts.add(CompletedPart.builder()
                    .partNumber(i)
                    .eTag(partETags.get(i))
                    .build());
        }

        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(fileKey)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                .build();

        s3Client.completeMultipartUpload(completeMultipartUploadRequest);
        uploadIdMap.remove(fileKey);

       //Optionally delete chunks
       deleteChunks(fileKey, totalChunks);
    }

//    private String getETag(String fileKey) {
//    	 HeadObjectResponse headObjectResponse = s3Client.headObject(HeadObjectRequest.builder()
//                 .bucket(bucketName)
//                 .key(fileKey)
//                 .build());
//         return headObjectResponse.eTag();
//	}

	public void deleteChunks(String fileKey, int totalChunks) {
        for (int i = 1; i <= totalChunks; i++) {
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileKey)
                    .build());
        }
    }
}
