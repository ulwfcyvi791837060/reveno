package org.reveno.atp.core.serialization;

import io.protostuff.Input;
import io.protostuff.LowCopyProtostuffOutput;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.reveno.atp.api.domain.RepositoryData;
import org.reveno.atp.api.exceptions.BufferOutOfBoundsException;
import org.reveno.atp.commons.ByteArrayObjectMap;
import org.reveno.atp.core.api.TransactionCommitInfo;
import org.reveno.atp.core.api.TransactionCommitInfo.Builder;
import org.reveno.atp.core.api.channel.Buffer;
import org.reveno.atp.core.api.serialization.RepositoryDataSerializer;
import org.reveno.atp.core.api.serialization.TransactionInfoSerializer;
import org.reveno.atp.core.serialization.protostuff.ZeroCopyBufferInput;
import org.reveno.atp.core.serialization.protostuff.ZeroCopyLinkBuffer;
import org.reveno.atp.utils.BinaryUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.reveno.atp.utils.BinaryUtils.crc32;
import static org.reveno.atp.utils.BinaryUtils.sha1;

public class ProtostuffSerializer implements RepositoryDataSerializer, TransactionInfoSerializer {
    protected static final int PROTO_TYPE = 0x222;
    protected static final int SHA1_DIGEST_SIZE = 20;
    protected static final byte SHA1_TYPE = 1;
    protected static final byte CRC32_TYPE = 2;
    protected final Schema<RepositoryData> repoSchema = RuntimeSchema.createFrom(RepositoryData.class);
    protected ThreadLocal<ZeroCopyLinkBuffer> linkedBuff = new ThreadLocal<ZeroCopyLinkBuffer>() {
        protected ZeroCopyLinkBuffer initialValue() {
            return new ZeroCopyLinkBuffer();
        }
    };
    protected ThreadLocal<LowCopyProtostuffOutput> output = new ThreadLocal<LowCopyProtostuffOutput>() {
        protected LowCopyProtostuffOutput initialValue() {
            return new LowCopyProtostuffOutput();
        }
    };
    protected ClassLoader classLoader;
    protected ByteArrayObjectMap<ProtoTransactionTypeHolder> registeredSha1 = new ByteArrayObjectMap<>();
    protected Long2ObjectMap<ProtoTransactionTypeHolder> registeredCrc = new Long2ObjectOpenHashMap<>(64);
    protected Map<Class<?>, byte[]> sha1Names = new HashMap<>(64);
    protected Object2LongMap<Class<?>> crcNames = new Object2LongOpenHashMap<>(64);

    public ProtostuffSerializer() {
        this(Thread.currentThread().getContextClassLoader());
    }

    public ProtostuffSerializer(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public int getSerializerType() {
        return PROTO_TYPE;
    }

    @Override
    public boolean isRegistered(Class<?> type) {
        return sha1Names.containsKey(type);
    }

    @Override
    public void registerTransactionType(Class<?> txDataType) {
        byte[] shaKey = sha1(txDataType.getName());
        long crc = crc32(txDataType.getName());
        ProtoTransactionTypeHolder ptth = new ProtoTransactionTypeHolder(txDataType,
                RuntimeSchema.getSchema(txDataType), registeredCrc.containsKey(crc));
        registeredSha1.put(shaKey, ptth);
        if (!ptth.crcCollision) {
            registeredCrc.put(crc, ptth);
        }
        sha1Names.put(txDataType, shaKey);
        if (!ptth.crcCollision) {
            crcNames.put(txDataType, crc);
        }
    }

    @Override
    public void serialize(TransactionCommitInfo info, Buffer buffer) {
        buffer.writeLong(info.transactionId());
        buffer.writeLong(info.time());
        BinaryUtils.writeNullable(info.flag(), buffer);
        BinaryUtils.writeNullable(info.tag(), buffer);
        buffer.writeInt(info.transactionCommits().size());

        serializeObjects(buffer, info.transactionCommits());
    }

    @Override
    public TransactionCommitInfo deserialize(Builder builder, Buffer buffer) {
        changeClassLoaderIfRequired();

        long transactionId = buffer.readLong();
        long time = buffer.readLong();
        long flag = BinaryUtils.readNullable(buffer);
        long tag = BinaryUtils.readNullable(buffer);
        if (transactionId == 0 && time == 0) {
            throw new BufferOutOfBoundsException();
        }
        //反序列化对象
        List<Object> commits = deserializeObjects(buffer);

        return builder.create().transactionId(transactionId)
                .time(time).transactionCommits(commits)
                .flag(flag).tag(tag);
    }

    @Override
    public void serialize(RepositoryData repository, Buffer buffer) {
        changeClassLoaderIfRequired();

        ZeroCopyLinkBuffer zeroCopyLinkBuffer = linkedBuff.get();
        LowCopyProtostuffOutput lowCopyProtostuffOutput = output.get();

        zeroCopyLinkBuffer.withBuffer(buffer);
        lowCopyProtostuffOutput.buffer = zeroCopyLinkBuffer;

        try {
            repoSchema.writeTo(lowCopyProtostuffOutput, repository);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RepositoryData deserialize(Buffer buffer) {
        changeClassLoaderIfRequired();

        // TODO unnecessary allocation
        Input input = new ZeroCopyBufferInput(buffer, true);
        RepositoryData repoData = repoSchema.newMessage();
        try {
            repoSchema.mergeFrom(input, repoData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return repoData;
    }

    @Override
    public void serializeCommands(List<Object> commands, Buffer buffer) {
        changeClassLoaderIfRequired();

        buffer.writeInt(commands.size());
        serializeObjects(buffer, commands);
    }

    @Override
    public List<Object> deserializeCommands(Buffer buffer) {
        changeClassLoaderIfRequired();

        return deserializeObjects(buffer);
    }

    protected void serializeObjects(Buffer buffer, List<Object> objs) {
        for (int i = 0; i < objs.size(); i++) {
            serializeObject(buffer, objs.get(i));
        }
    }

    @SuppressWarnings("unchecked")
    public void serializeObject(Buffer buffer, Object tc) {
        ZeroCopyLinkBuffer zeroCopyLinkBuffer = linkedBuff.get();
        LowCopyProtostuffOutput lowCopyProtostuffOutput = output.get();
        zeroCopyLinkBuffer.withBuffer(buffer);
        lowCopyProtostuffOutput.buffer = zeroCopyLinkBuffer;

        long crc = crcNames.getLong(tc.getClass());
        ProtoTransactionTypeHolder ptth = registeredCrc.get(crc);
        if (ptth.crcCollision) {
            byte[] key = sha1Names.get(tc.getClass());
            ptth = registeredSha1.get(key);
            buffer.writeByte(SHA1_TYPE);
            buffer.writeBytes(key, 0, SHA1_DIGEST_SIZE);
        } else {
            buffer.writeByte(CRC32_TYPE);
            buffer.writeLong(crc);
        }

        buffer.markSize();
        Schema<Object> schema = (Schema<Object>) ptth.schema;
        try {
            schema.writeTo(lowCopyProtostuffOutput, tc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        buffer.writeSize();
    }

    protected List<Object> deserializeObjects(Buffer buffer) {
        int len = buffer.readInt();
        List<Object> commits = new ArrayList<>(len);

        for (int i = 0; i < len; i++) {
            //反序列化对象
            commits.add(i, deserializeObject(buffer));
        }
        return commits;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object deserializeObject(Buffer buffer) {
        Input input = new ZeroCopyBufferInput(buffer, true);
        byte type = buffer.readByte();
        Schema<Object> schema;
        if (type == CRC32_TYPE) {
            long crc = buffer.readLong();
            schema = (Schema<Object>) registeredCrc.get(crc).schema;
        } else {
            schema = (Schema<Object>) registeredSha1.get(buffer, SHA1_DIGEST_SIZE).schema;
        }
        int size = buffer.readInt();
        Object message = schema.newMessage();
        try {
            buffer.limitNext(size);
            schema.mergeFrom(input, message);
            buffer.resetNextLimit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return message;
    }

    protected void changeClassLoaderIfRequired() {
        if (Thread.currentThread().getContextClassLoader() != classLoader) {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
    }

    protected static class ProtoTransactionTypeHolder {
        public final Class<?> transactionType;
        public final Schema<?> schema;
        public final boolean crcCollision;

        public ProtoTransactionTypeHolder(Class<?> transactionType, Schema<?> schema, boolean crcCollision) {
            this.transactionType = transactionType;
            this.schema = schema;
            this.crcCollision = crcCollision;
        }
    }

}
