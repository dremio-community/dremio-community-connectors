package com.dremio.talend.components.service;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth.BasicClientAuthHandler;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import com.dremio.talend.components.datastore.DremioDataStore;

@Service
public class DremioService {

    @HealthCheck("dremioConnection")
    public HealthCheckStatus validateConnection(@Option("datastore") final DremioDataStore datastore) {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Location location;
            if (datastore.isEnableSsl()) {
                location = Location.forGrpcTls(datastore.getHost(), datastore.getPort());
            } else {
                location = Location.forGrpcInsecure(datastore.getHost(), datastore.getPort());
            }
            ClientIncomingAuthHeaderMiddleware.Factory factory = new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
            
            try (FlightClient client = FlightClient.builder(allocator, location)
                    .intercept(factory)
                    .build()) {
                
                String user = datastore.getUsername();
                if (user == null || user.isEmpty()) {
                    user = "_dremio";
                }
                
                client.authenticate(new BasicClientAuthHandler(user, datastore.getPersonalAccessToken()), new CallOption[0]);
                
                return new HealthCheckStatus(HealthCheckStatus.Status.OK, "Connection successful to Dremio Flight");
            }
        } catch (Exception e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, "Connection failed: " + e.getMessage());
        }
    }
}
