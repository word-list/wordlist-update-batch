package tech.gaul.wordlist.updatebatch.models;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Setter
@Getter
public class BatchQueryResponse {
    
    private String word;
    private String[] types;
    private int offensiveness;
    private int commonness;
    private int sentiment;
    private String note;

}
