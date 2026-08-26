/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A named ES|QL query stored in cluster state and resolved like an index in {@code FROM}.
 * Optional {@link #description()}, {@link #managed()}, and {@link #metadata()} ({@code _meta})
 * travel with the definition. Managed views are hidden from wildcard listing and {@code FROM *}
 * the same way hidden indices are; they remain reachable by concrete name and remain writable.
 */
public final class View implements Writeable, ToXContentObject, IndexAbstraction {
    static final TransportVersion VIEW_METADATA_VERSION = TransportVersion.fromName("esql_view_managed_and_meta");

    public static final int MAX_DESCRIPTION_LENGTH = 1_000;
    public static final int MAX_META_BYTES = 4_096;

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField QUERY = new ParseField("query");
    private static final ParseField DESCRIPTION = new ParseField("description");
    private static final ParseField MANAGED = new ParseField("managed");
    private static final ParseField META = new ParseField("_meta");

    // Parser that includes the name field (eg. serializing/deserializing the full object)
    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<View, Void> PARSER = new ConstructingObjectParser<>(
        "view",
        false,
        (args, ctx) -> new View(
            (String) args[0],
            (String) args[1],
            (String) args[2],
            booleanManaged(args[3]),
            (Map<String, Object>) args[4]
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), QUERY);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DESCRIPTION);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), MANAGED);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), META);
    }

    // Parser that excludes the name field (eg. when the name is provided externally, in the URL path)
    public static ConstructingObjectParser<View, Void> parser(String name) {
        @SuppressWarnings("unchecked")
        ConstructingObjectParser<View, Void> parser = new ConstructingObjectParser<>(
            "view",
            false,
            (args, ctx) -> new View(name, (String) args[0], (String) args[1], booleanManaged(args[2]), (Map<String, Object>) args[3])
        );
        parser.declareString(ConstructingObjectParser.constructorArg(), QUERY);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), DESCRIPTION);
        parser.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), MANAGED);
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), META);
        return parser;
    }

    private static boolean booleanManaged(Object value) {
        return Boolean.TRUE.equals(value);
    }

    private final String name;
    private final String query;
    @Nullable
    private final String description;
    private final boolean managed;
    @Nullable
    private final Map<String, Object> metadata;

    public View(String name, String query) {
        this(name, query, null, false, null);
    }

    public View(String name, String query, @Nullable String description, boolean managed, @Nullable Map<String, Object> metadata) {
        this.name = Objects.requireNonNull(name, "view name must not be null");
        this.query = Objects.requireNonNull(query, "view query must not be null");
        this.description = Strings.hasText(description) ? description : null;
        this.managed = managed;
        this.metadata = copyMetadata(metadata);
        validate();
    }

    public View(StreamInput in) throws IOException {
        this.name = in.readString();
        this.query = in.readString();
        if (in.getTransportVersion().supports(VIEW_METADATA_VERSION)) {
            this.description = in.readOptionalString();
            this.managed = in.readBoolean();
            this.metadata = copyMetadata(in.readGenericMap());
        } else {
            this.description = null;
            this.managed = false;
            this.metadata = null;
        }
        validate();
    }

    public static View fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(query);
        if (out.getTransportVersion().supports(VIEW_METADATA_VERSION)) {
            out.writeOptionalString(description);
            out.writeBoolean(managed);
            out.writeGenericMap(metadata);
        }
    }

    public String name() {
        return name;
    }

    public String query() {
        return query;
    }

    @Nullable
    public String description() {
        return description;
    }

    /**
     * System-owned views set this so default list APIs and {@code FROM *} hide them.
     * Users can still {@code PUT}/{@code DELETE} them; owners typically overwrite on reinstall.
     */
    public boolean managed() {
        return managed;
    }

    /**
     * Optional free-form metadata bag ({@code _meta} on the wire), same convention as ingest
     * pipelines and index templates. Elasticsearch does not interpret keys.
     */
    @Nullable
    public Map<String, Object> metadata() {
        return metadata;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), name);
        builder.field(QUERY.getPreferredName(), query);
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        builder.field(MANAGED.getPreferredName(), managed);
        if (metadata != null) {
            builder.field(META.getPreferredName(), metadata);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        View other = (View) o;
        return managed == other.managed
            && Objects.equals(name, other.name)
            && Objects.equals(query, other.query)
            && Objects.equals(description, other.description)
            && Objects.equals(metadata, other.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, query, description, managed, metadata);
    }

    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public Type getType() {
        return Type.VIEW;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<Index> getIndices() {
        return List.of();
    }

    @Override
    public Index getWriteIndex() {
        return null;
    }

    @Override
    public DataStream getParentDataStream() {
        return null;
    }

    @Override
    public boolean isHidden() {
        return managed;
    }

    @Override
    public boolean isSystem() {
        return false;
    }

    private void validate() {
        if (description != null && description.length() > MAX_DESCRIPTION_LENGTH) {
            throw new IllegalArgumentException(
                "view [description] is too large: " + description.length() + " characters, the maximum allowed is " + MAX_DESCRIPTION_LENGTH
            );
        }
        if (metadata != null) {
            final int size = metadataSizeInBytes(metadata);
            if (size > MAX_META_BYTES) {
                throw new IllegalArgumentException(
                    "view [_meta] is too large: " + size + " bytes, the maximum allowed is " + MAX_META_BYTES
                );
            }
        }
    }

    private static Map<String, Object> copyMetadata(@Nullable Map<String, Object> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return null;
        }
        return Collections.unmodifiableMap(new HashMap<>(metadata));
    }

    private static int metadataSizeInBytes(Map<String, Object> metadata) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(metadata);
            return BytesReference.bytes(builder).length();
        } catch (IOException e) {
            throw new IllegalArgumentException("view [_meta] cannot be serialized", e);
        }
    }
}
