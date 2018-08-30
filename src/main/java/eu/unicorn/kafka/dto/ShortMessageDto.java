package eu.unicorn.kafka.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ShortMessageDto extends MessageDto {
	
	private Integer data;

}
