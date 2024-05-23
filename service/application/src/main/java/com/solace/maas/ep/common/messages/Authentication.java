package com.solace.maas.ep.common.messages;

import lombok.Data;

@Data
public class Authentication {
    private String type;
    private String protocol;
    private Credential credential;
}
