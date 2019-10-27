package org.reveno.atp.core.disruptor;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import org.reveno.atp.api.Configuration.CpuConsumption;
import org.reveno.atp.core.api.Destroyable;
import org.reveno.atp.core.engine.processor.PipeProcessor;
import org.reveno.atp.core.engine.processor.ProcessorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public abstract class DisruptorPipeProcessor<T extends Destroyable> implements PipeProcessor<T> {
    protected static final Logger log = LoggerFactory.getLogger(DisruptorPipeProcessor.class);
    protected volatile boolean isStarted = false;
    protected Disruptor<T> disruptor;
    protected List<ProcessorHandler<T>[]> handlers = new ArrayList<>();

    abstract CpuConsumption cpuConsumption();

    abstract int bufferSize();

    abstract boolean singleProducer();

    abstract EventFactory<T> eventFactory();

    abstract ThreadFactory threadFactory();

    abstract void startupInterceptor();

    @Override
    public void start() {
        if (isStarted) throw new IllegalStateException("The Pipe Processor is already started.");
        //new Disruptor<
        disruptor = new Disruptor(eventFactory(), bufferSize(), threadFactory(),
                singleProducer() ? ProducerType.SINGLE : ProducerType.MULTI,
                createWaitStrategy());

        //附加处理程序
        attachHandlers(disruptor);
        //启动拦截器
        startupInterceptor();

        disruptor.start();

        log.info("Started.");
        isStarted = true;
    }

    @Override
    public void stop() {
        if (!isStarted) throw new IllegalStateException("The Pipe Processor is already stopped.");

        isStarted = false;
        disruptor.shutdown();
        log.info("Stopped.");
    }

    @Override
    public void shutdown() {
        stop();
        disruptor.shutdown();

        for (int i = 0; i < bufferSize(); i++)
            disruptor.getRingBuffer().get(i).destroy();
    }

    @Override
    public boolean isStarted() {
        return isStarted;
    }

    /**
     * handler 添加到管子里排队
     * @param handler
     * @return
     */
    @Override
    public PipeProcessor<T> pipe(ProcessorHandler<T>... handler) {
        //如果没有启动
        if (!isStarted) {
            handlers.add(handler);
        }
        return this;
    }

    @Override
    public <R> CompletableFuture<R> process(BiConsumer<T, CompletableFuture<R>> consumer) {
        if (!isStarted)
            throw new RuntimeException("Pipe Processor must be started!");

        final CompletableFuture<R> f = new CompletableFuture<R>();
        //发布事件
        disruptor.publishEvent((e, s) -> consumer.accept(e, f));
        return f;
    }

    protected WaitStrategy createWaitStrategy() {
        switch (cpuConsumption()) {
            case LOW:
                return new BlockingWaitStrategy();
            case NORMAL:
                return new SleepingWaitStrategy();
            case HIGH:
                return new YieldingWaitStrategy();
            case PHASED:
                return PhasedBackoffWaitStrategy.withLiteLock((int) 2.5e5, (int) 8.5e5,
                        TimeUnit.NANOSECONDS);
        }
        return null;
    }

    /**
     * 附加处理程序
     * @param disruptor
     */
    protected void attachHandlers(Disruptor<T> disruptor) {
        List<EventHandler<T>[]> disruptorHandlers = handlers.stream()
                .<EventHandler<T>[]>map(this::convert)
                .collect(Collectors.toList());

        EventHandlerGroup<T> h = disruptor.handleEventsWith(disruptorHandlers.get(0));
        for (int i = 1; i < disruptorHandlers.size(); i++) {
            h = h.then(disruptorHandlers.get(i));
        }
    }

    protected EventHandler<T>[] convert(ProcessorHandler<T>[] h) {
        EventHandler<T>[] acs = new EventHandler[h.length];
        for (int i = 0; i < h.length; i++) {
            final ProcessorHandler<T> hh = h[i];
            acs[i] = (e, c, eob) -> hh.handle(e, eob);
        }
        return acs;
    }

}
