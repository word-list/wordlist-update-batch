package tech.gaul.wordlist.updatebatch.models;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
@Setter
public class UpdateWordMessage {
    
    private String word;
    private String[] types;
    private int offensiveness;
    private int commonness;
    private int sentiment;    

}
