package com.solace.maas.ep.event.management.agent.plugin.command.model;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@SuppressWarnings("PMD.AvoidFieldNameMatchingTypeName")
public class Command {
    @NotNull
    private CommandType commandType; // "terraform", "bash", SEMP, etc...
    @NotNull
    private String command; // A tf command like `apply` or `state rm`
    private String body; // The body of the .tf file
    private Map<String, String> parameters;
    private Boolean ignoreResult;
    private CommandResult result;
}
