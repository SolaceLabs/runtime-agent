package com.solace.maas.ep.event.management.agent.plugin.confluentSchemaRegistry.processor.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class ConfluentSchemaRegistrySchemaReference implements Serializable {
    private String name;
    private String subject;
    private String version;
}
