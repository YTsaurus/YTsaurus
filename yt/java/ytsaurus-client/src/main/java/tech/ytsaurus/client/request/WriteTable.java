package tech.ytsaurus.client.request;

import java.io.ByteArrayOutputStream;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.TReqWriteTable;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

import static java.nio.charset.StandardCharsets.UTF_8;

public class WriteTable<T> extends RequestBase<WriteTable.Builder<T>, WriteTable<T>> {
    @Nullable
    private final YPath path;
    @Nullable
    private final String stringPath;
    private final SerializationContext<T> serializationContext;
    @Nullable
    private final TableSchema tableSchema;

    @Nullable
    private final YTreeNode config;
    @Nullable
    private final TransactionalOptions transactionalOptions;

    private final long windowSize;
    private final long packetSize;

    private final boolean needRetries;
    private final int maxWritesInFlight;
    private final int chunkSize;

    public WriteTable(YPath path, SerializationContext<T> serializationContext) {
        this(new Builder<T>().setPath(path).setSerializationContext(serializationContext));
    }

    public WriteTable(YPath path, Class<T> objectClass) {
        this(new Builder<T>()
                .setPath(path)
                .setSerializationContext(new SerializationContext<>(objectClass))
        );
    }

    public WriteTable(YPath path, SerializationContext<T> serializationContext, TableSchema tableSchema) {
        this(new Builder<T>()
                .setPath(path)
                .setSerializationContext(serializationContext)
                .setTableSchema(tableSchema)
        );
    }

    public WriteTable(BuilderBase<T, ?> builder) {
        super(builder);
        this.serializationContext = Objects.requireNonNull(builder.serializationContext);
        this.tableSchema = builder.tableSchema;
        this.path = builder.path;
        this.stringPath = builder.stringPath;
        this.config = builder.config;
        this.transactionalOptions = builder.transactionalOptions;
        this.windowSize = builder.windowSize;
        this.packetSize = builder.packetSize;
        this.needRetries = builder.needRetries;
        this.maxWritesInFlight = builder.maxWritesInFlight;
        this.chunkSize = builder.chunkSize;
    }

    /**
     * Use {@link #builder(Class)} instead if you don't need specific SerializationContext.
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static <T> Builder<T> builder(Class<T> rowClass) {
        return new Builder<T>().setSerializationContext(new SerializationContext<>(rowClass));
    }

    public SerializationContext<T> getSerializationContext() {
        return serializationContext;
    }

    public long getWindowSize() {
        return windowSize;
    }

    public long getPacketSize() {
        return packetSize;
    }

    /**
     * @see BuilderBase#setNeedRetries(boolean)
     */
    public boolean getNeedRetries() {
        return needRetries;
    }

    /**
     * @see BuilderBase#setMaxWritesInFlight(int)
     */
    public int getMaxWritesInFlight() {
        return maxWritesInFlight;
    }

    /**
     * @see BuilderBase#setChunkSize(int)
     */
    public int getChunkSize() {
        return chunkSize;
    }

    public Optional<TableSchema> getTableSchema() {
        return Optional.ofNullable(tableSchema);
    }

    public Optional<GUID> getTransactionId() {
        if (this.transactionalOptions == null) {
            return Optional.empty();
        }
        return this.transactionalOptions.getTransactionId();
    }

    public String getPath() {
        return path != null ? path.toString() : Objects.requireNonNull(stringPath);
    }

    public YPath getYPath() {
        return Objects.requireNonNull(path);
    }

    public TReqWriteTable.Builder writeTo(TReqWriteTable.Builder builder) {
        builder.setPath(ByteString.copyFromUtf8(getPath()));
        if (config != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            YTreeBinarySerializer.serialize(config, baos);
            byte[] data = baos.toByteArray();
            builder.setConfig(ByteString.copyFrom(data));
        } else {
            // TODO: remove this HACK
            builder.setConfig(ByteString.copyFrom("{}", UTF_8));
        }
        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
        Optional<Format> format = serializationContext.getFormat();
        if (format.isPresent()) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            YTreeBinarySerializer.serialize(format.get().toTree(), baos);
            byte[] data = baos.toByteArray();
            builder.setFormat(ByteString.copyFrom(data));
        }
        return builder;
    }

    @Override
    public Builder<T> toBuilder() {
        return new Builder<T>()
                .setPath(path)
                .setPath(stringPath)
                .setSerializationContext(serializationContext)
                .setTableSchema(tableSchema)
                .setNeedRetries(needRetries)
                .setMaxWritesInFlight(maxWritesInFlight)
                .setChunkSize(chunkSize)
                .setWindowSize(windowSize)
                .setPacketSize(packetSize)
                .setConfig(config)
                .setTransactionalOptions(transactionalOptions)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder<T> extends BuilderBase<T, Builder<T>> {
        private Builder() {
        }

        @Override
        protected Builder<T> self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            T,
            TBuilder extends BuilderBase<T, TBuilder>>
            extends RequestBase.Builder<TBuilder, WriteTable<T>> {

        @Nullable
        private YPath path;
        @Nullable
        private String stringPath;

        @Nullable
        private SerializationContext<T> serializationContext;
        @Nullable
        private TableSchema tableSchema;

        @Nullable
        private YTreeNode config = null;
        @Nullable
        private TransactionalOptions transactionalOptions = null;

        private long windowSize = 16000000L;
        private long packetSize = windowSize / 2;

        private boolean needRetries = true;
        private int maxWritesInFlight = 1;
        private int chunkSize = 524288000;

        public TBuilder setPath(@Nullable YPath path) {
            this.path = path;
            return self();
        }

        /**
         * @deprecated prefer to use {@link #setPath(YPath)}
         */
        @Deprecated
        public TBuilder setPath(@Nullable String path) {
            this.stringPath = path;
            return self();
        }

        public TBuilder setSerializationContext(SerializationContext<T> serializationContext) {
            if (serializationContext instanceof ReadSerializationContext) {
                throw new IllegalArgumentException("ReadSerializationContext do not allowed here");
            }
            this.serializationContext = serializationContext;
            return self();
        }

        public TBuilder setTableSchema(@Nullable TableSchema tableSchema) {
            this.tableSchema = tableSchema;
            return self();
        }

        /**
         * If you don't need a writer with retries, set needRetries=false.
         * RetryPolicy should be set in RpcOptions
         *
         * @return self
         */
        public TBuilder setNeedRetries(boolean needRetries) {
            this.needRetries = needRetries;
            return self();
        }

        /**
         * If a rows ordering doesn't matter, you can set maxWritesInFlight more than 1.
         * This will make writing faster.
         *
         * @return self
         */
        public TBuilder setMaxWritesInFlight(int maxWritesInFlight) {
            this.maxWritesInFlight = maxWritesInFlight;
            return self();
        }

        /**
         * Specifies the minimum data size for a {@code write_table} request
         * (one {@code write_table} request creates at least one chunk).
         * <p>
         * If you want to specify the desired chunk size in the output table,
         * set {@code desired_chunk_size} in {@link #config}.
         * <p>
         * This parameter will be ignored if {@link #needRetries}=false.
         *
         * @return self
         * @see BuilderBase#setNeedRetries(boolean)
         * @see BuilderBase#setConfig(YTreeNode)
         * @see <a href=https://ytsaurus.tech/docs/en/user-guide/storage/io-configuration#desired_chunk_size>
         * desired_chunk_size
         * </a>
         */
        public TBuilder setChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return self();
        }

        public TBuilder setWindowSize(long windowSize) {
            this.windowSize = windowSize;
            return self();
        }

        public TBuilder setPacketSize(long packetSize) {
            this.packetSize = packetSize;
            return self();
        }

        public TBuilder setConfig(@Nullable YTreeNode config) {
            this.config = config;
            return self();
        }

        public TBuilder setTransactionalOptions(@Nullable TransactionalOptions transactionalOptions) {
            this.transactionalOptions = transactionalOptions;
            return self();
        }

        public String getPath() {
            return path != null ? path.toString() : Objects.requireNonNull(stringPath);
        }

        public TReqWriteTable.Builder writeTo(TReqWriteTable.Builder builder) {
            builder.setPath(ByteString.copyFromUtf8(getPath()));
            if (config != null) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                YTreeBinarySerializer.serialize(config, baos);
                byte[] data = baos.toByteArray();
                builder.setConfig(ByteString.copyFrom(data));
            } else {
                // TODO: remove this HACK
                builder.setConfig(ByteString.copyFrom("{}", UTF_8));
            }
            if (transactionalOptions != null) {
                builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
            }
            if (additionalData != null) {
                builder.mergeFrom(additionalData);
            }
            Optional<Format> format = Objects.requireNonNull(serializationContext).getFormat();
            if (format.isPresent()) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                YTreeBinarySerializer.serialize(format.get().toTree(), baos);
                byte[] data = baos.toByteArray();
                builder.setFormat(ByteString.copyFrom(data));
            }
            return builder;
        }

        @Override
        public WriteTable<T> build() {
            return new WriteTable<>(this);
        }
    }
}
