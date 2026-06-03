package com.dremio.talend.components.datastore;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.meta.Documentation;

@DataStore("DremioDataStore")
@Checkable("dremioConnection")
@GridLayout({
    @GridLayout.Row({ "host" }),
    @GridLayout.Row({ "port" }),
    @GridLayout.Row({ "enableSsl" }),
    @GridLayout.Row({ "username" }),
    @GridLayout.Row({ "personalAccessToken" })
})
@Documentation("Dremio connection configuration for Arrow Flight")
public class DremioDataStore implements Serializable {

    @Option
    @Documentation("Dremio Coordinator Host (e.g. localhost or dremio.company.com)")
    private String host = "localhost";

    @Option
    @Documentation("Arrow Flight Port (Default is 32010)")
    private int port = 32010;

    @Option
    @Documentation("Enable SSL/TLS for secure connections (Required for Dremio Cloud)")
    private boolean enableSsl = false;

    @Option
    @Documentation("Username")
    private String username;

    @Option
    @Credential
    @Documentation("Personal Access Token or Password")
    private String personalAccessToken;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public boolean isEnableSsl() { return enableSsl; }
    public void setEnableSsl(boolean enableSsl) { this.enableSsl = enableSsl; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getPersonalAccessToken() { return personalAccessToken; }
    public void setPersonalAccessToken(String personalAccessToken) { this.personalAccessToken = personalAccessToken; }
}
