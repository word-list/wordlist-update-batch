package tech.gaul.wordlist.updatebatch.models;

import java.util.Date;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
@Setter
public class ActiveBatchRequest {
    
    private String id;
    private String batchRequestId;
    private String uploadedFileId;    
    private String status;

    private Date createdAt;
    private Date updatedAt;

}
