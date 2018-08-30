package eu.unicorn.kafka.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class LongMessageDto extends MessageDto {
	
	private String data;

}
