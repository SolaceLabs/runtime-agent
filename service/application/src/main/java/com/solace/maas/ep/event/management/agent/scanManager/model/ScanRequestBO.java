package com.solace.maas.ep.event.management.agent.scanManager.model;

import com.solace.maas.ep.event.management.agent.model.AbstractBaseBO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@Data
@EqualsAndHashCode(callSuper = true)
@SuppressWarnings("PMD")
public class ScanRequestBO extends AbstractBaseBO<String> {

    private String orgId;

    private String messagingServiceId;

    private String scanId;

    private String traceId;

    private String actorId;

    private List<String> scanTypes;

    private List<String> destinations;

    @Override
    public String toString() {
        return "{ messaging service id:[" + messagingServiceId + ']' +
                ", scan id: [" + scanId + ']' +
                ", trace id: " + traceId +
                ", actor id: " + actorId +
                ", scan types: " + scanTypes +
                ", destinations: " + destinations +
                " }";
    }
}
