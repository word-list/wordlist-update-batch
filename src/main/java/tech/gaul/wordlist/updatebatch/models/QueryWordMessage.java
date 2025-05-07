package tech.gaul.wordlist.updatebatch.models;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
@Setter
public class QueryWordMessage {
    
    private String word;
    private boolean force;

}
