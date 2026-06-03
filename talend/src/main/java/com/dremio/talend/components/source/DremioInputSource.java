package com.dremio.talend.components.source;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth.BasicClientAuthHandler;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.dremio.talend.components.dataset.DremioDataSet;
import com.dremio.talend.components.datastore.DremioDataStore;

public class DremioInputSource implements Serializable {

    private final DremioDataSet configuration;
    private final RecordBuilderFactory recordBuilderFactory;
    private final byte[] ticket;
    private final String endpointLocation;
    
    private transient BufferAllocator allocator;
    private transient FlightClient client;
    private transient FlightStream currentStream;
    private transient VectorSchemaRoot currentRoot;
    private transient int currentRowIndex = 0;
    
    public DremioInputSource(final DremioDataSet configuration, final RecordBuilderFactory recordBuilderFactory, byte[] ticket, String endpointLocation) {
        this.configuration = configuration;
        this.recordBuilderFactory = recordBuilderFactory;
        this.ticket = ticket;
        this.endpointLocation = endpointLocation;
    }

    @PostConstruct
    public void init() {
        if (ticket == null) {
            throw new IllegalStateException("No Arrow Flight ticket assigned to this worker.");
        }

        DremioDataStore store = configuration.getDatastore();
        allocator = new RootAllocator(Long.MAX_VALUE);
        
        Location location;
        if (endpointLocation != null && !endpointLocation.isEmpty()) {
            URI uri = URI.create(endpointLocation);
            if (store.isEnableSsl() || uri.getScheme().equalsIgnoreCase("grpcs")) {
                location = Location.forGrpcTls(uri.getHost(), uri.getPort() > 0 ? uri.getPort() : store.getPort());
            } else {
                location = Location.forGrpcInsecure(uri.getHost(), uri.getPort() > 0 ? uri.getPort() : store.getPort());
            }
        } else {
            if (store.isEnableSsl()) {
                location = Location.forGrpcTls(store.getHost(), store.getPort());
            } else {
                location = Location.forGrpcInsecure(store.getHost(), store.getPort());
            }
        }
        
        ClientIncomingAuthHeaderMiddleware.Factory factory = new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
        client = FlightClient.builder(allocator, location)
                .intercept(factory)
                .build();

        String user = store.getUsername();
        if (user == null || user.isEmpty()) {
            user = "_dremio";
        }
        
        client.authenticate(new BasicClientAuthHandler(user, store.getPersonalAccessToken()), new CallOption[0]);
        CallOption[] callOptions = new CallOption[]{factory.getCredentialCallOption()};

        currentStream = client.getStream(new Ticket(ticket), callOptions);
    }

    @Producer
    public Record next() {
        while (true) {
            if (currentRoot != null && currentRowIndex < currentRoot.getRowCount()) {
                Record record = createRecord(currentRoot, currentRowIndex);
                currentRowIndex++;
                return record;
            }

            if (currentStream != null && currentStream.next()) {
                currentRoot = currentStream.getRoot();
                currentRowIndex = 0;
                if (currentRoot.getRowCount() > 0) {
                    continue;
                }
            }

            return null; // Stream finished
        }
    }

    private Record createRecord(VectorSchemaRoot root, int index) {
        Record.Builder builder = recordBuilderFactory.newRecordBuilder();
        for (Field field : root.getSchema().getFields()) {
            FieldVector vector = root.getVector(field.getName());
            if (vector.isNull(index)) {
                continue;
            }
            
            String name = field.getName();
            
            if (vector instanceof VarCharVector) {
                builder.withString(name, new String(((VarCharVector) vector).get(index), StandardCharsets.UTF_8));
            } else if (vector instanceof IntVector) {
                builder.withInt(name, ((IntVector) vector).get(index));
            } else if (vector instanceof BigIntVector) {
                builder.withLong(name, ((BigIntVector) vector).get(index));
            } else if (vector instanceof Float8Vector) {
                builder.withDouble(name, ((Float8Vector) vector).get(index));
            } else if (vector instanceof Float4Vector) {
                builder.withFloat(name, ((Float4Vector) vector).get(index));
            } else if (vector instanceof BitVector) {
                builder.withBoolean(name, ((BitVector) vector).get(index) == 1);
            } else if (vector instanceof SmallIntVector) {
                builder.withInt(name, ((SmallIntVector) vector).get(index));
            } else if (vector instanceof TinyIntVector) {
                builder.withInt(name, ((TinyIntVector) vector).get(index));
            } else if (vector instanceof TimeStampVector) {
                Object obj = vector.getObject(index);
                if (obj instanceof java.time.LocalDateTime) {
                    builder.withDateTime(name, ZonedDateTime.of((java.time.LocalDateTime)obj, ZoneOffset.UTC));
                } else if (obj instanceof Long) {
                    builder.withDateTime(name, ZonedDateTime.ofInstant(Instant.ofEpochMilli((Long)obj), ZoneOffset.UTC));
                } else {
                    builder.withString(name, obj.toString());
                }
            } else if (vector instanceof DecimalVector) {
                builder.withDouble(name, ((DecimalVector) vector).getObject(index).doubleValue());
            } else if (vector instanceof DateDayVector || vector instanceof DateMilliVector) {
                Object obj = vector.getObject(index);
                if (obj instanceof java.time.LocalDateTime) {
                    builder.withDateTime(name, ZonedDateTime.of((java.time.LocalDateTime)obj, ZoneOffset.UTC));
                } else {
                    builder.withString(name, obj.toString());
                }
            } else {
                // Fallback for complex types or unhandled types
                builder.withString(name, vector.getObject(index).toString());
            }
        }
        return builder.build();
    }

    @PreDestroy
    public void release() {
        try {
            if (currentStream != null) {
                currentStream.close();
            }
            if (client != null) {
                client.close();
            }
            if (allocator != null) {
                allocator.close();
            }
        } catch (Exception e) {
            // ignore
        }
    }
}
