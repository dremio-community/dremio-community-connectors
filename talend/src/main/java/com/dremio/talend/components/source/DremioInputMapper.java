package com.dremio.talend.components.source;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth.BasicClientAuthHandler;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.dremio.talend.components.dataset.DremioDataSet;
import com.dremio.talend.components.datastore.DremioDataStore;

@Version(1)
@Icon(value = Icon.IconType.DB_INPUT)
@PartitionMapper(name = "DremioInput")
@Documentation("Dremio Input Component using Arrow Flight with Parallel Partitioning")
public class DremioInputMapper implements Serializable {

    private final DremioDataSet configuration;
    private final RecordBuilderFactory recordBuilderFactory;
    
    private byte[] ticket;
    private String endpointLocation;

    public DremioInputMapper(@Option("configuration") final DremioDataSet configuration,
                             final RecordBuilderFactory recordBuilderFactory) {
        this.configuration = configuration;
        this.recordBuilderFactory = recordBuilderFactory;
    }

    public void setTicket(byte[] ticket) {
        this.ticket = ticket;
    }

    public void setEndpointLocation(String endpointLocation) {
        this.endpointLocation = endpointLocation;
    }

    @Assessor
    public long estimateSize() {
        return 1L; 
    }

    @Split
    public List<DremioInputMapper> split(@PartitionSize final long bundles) {
        // If this mapper has already been assigned a ticket, don't split further
        if (ticket != null) {
            return Collections.singletonList(this);
        }

        DremioDataStore store = configuration.getDatastore();
        
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Location location;
            if (store.isEnableSsl()) {
                location = Location.forGrpcTls(store.getHost(), store.getPort());
            } else {
                location = Location.forGrpcInsecure(store.getHost(), store.getPort());
            }
            ClientIncomingAuthHeaderMiddleware.Factory factory = new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
            
            try (FlightClient client = FlightClient.builder(allocator, location).intercept(factory).build()) {
                String user = store.getUsername();
                if (user == null || user.isEmpty()) {
                    user = "_dremio";
                }
                
                client.authenticate(new BasicClientAuthHandler(user, store.getPersonalAccessToken()), new CallOption[0]);
                CallOption[] callOptions = new CallOption[]{factory.getCredentialCallOption()};

                FlightDescriptor descriptor = FlightDescriptor.command(configuration.getSqlQuery().getBytes(StandardCharsets.UTF_8));
                FlightInfo info = client.getInfo(descriptor, callOptions);
                
                List<FlightEndpoint> endpoints = info.getEndpoints();
                List<DremioInputMapper> mappers = new ArrayList<>(endpoints.size());
                
                for (FlightEndpoint endpoint : endpoints) {
                    DremioInputMapper mapper = new DremioInputMapper(configuration, recordBuilderFactory);
                    mapper.setTicket(endpoint.getTicket().getBytes());
                    
                    if (!endpoint.getLocations().isEmpty()) {
                        mapper.setEndpointLocation(endpoint.getLocations().get(0).getUri().toString());
                    } else {
                        // Fallback to coordinator if no specific executor location is provided
                        mapper.setEndpointLocation(location.getUri().toString());
                    }
                    mappers.add(mapper);
                }
                
                return mappers;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve FlightInfo for parallel partitioning: " + e.getMessage(), e);
        }
    }

    @Emitter
    public DremioInputSource createWorker() {
        return new DremioInputSource(configuration, recordBuilderFactory, ticket, endpointLocation);
    }
}
