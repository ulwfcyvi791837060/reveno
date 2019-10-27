package org.reveno.atp.api;

import org.reveno.atp.api.commands.EmptyResult;
import org.reveno.atp.api.commands.Result;
import org.reveno.atp.api.dynamic.DynamicCommand;
import org.reveno.atp.api.query.QueryManager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Core engine interface. It provides with all starting points for working
 * with framework: startup/shutdown engine, domains, query, events sides of
 * engine, and configuration part as well. Naturally, the whole design of the
 * system is inspired by Command-Query segregation principle (CQRS).
 *
 * @author Artem Dmitriev <art.dm.ser@gmail.com>
 */
public interface Reveno {

    /**
     * Checks whether the engine was started or now.
     *
     * @return flag indicating wether engine is already started or now.
     */
    boolean isStarted();

    /**
     * Performs startup of the engine. It includes startup of all
     * workflow handlers, restoring latest state of system through recorded
     * events and snapshots done previously, setting all views in place.
     * <p>
     * Please note, that this method is not idempotent: you can call it only once.
     */
    void startup();

    /**
     * Shuts down an engine, gently stopping all processing, closing
     * all channels.
     * <p>
     * Please note, that after shutdown you can't startup an engine again.
     */
    void shutdown();

    /**
     * Contains all required operations for managing of domain space of engine.
     * Basically it includes registration of commands and transaction actions along
     * with their handlers. You can put here your own View mappers as well, which
     * naturally are the bridge between Query side and your whole domain model.
     * <p>
     * Also you can provide your own serializers chain or domain snapshotters as well.
     *
     * @return manager for working with domain space.
     */
    RevenoManager domain();

    /**
     * The query side of system, which is managed by view model. QueryManager
     * allows you to do all basic retrieval operations on your view model, which
     * is in an nutshell your whole domain reflected by a set of view mappers.
     * <p>
     * From CQRS pattern it can be interpreted as Query side.
     *
     * @return
     */
    QueryManager query();

    /**
     * Allows to register custom event handlers for different event types. An event can be
     * any Pojo object, and is fired during transaction execution.
     *
     * @return events manager
     */
    EventsManager events();

    /**
     * Contains all configuration settings regarding usage of engine in cluster.
     *
     * @return cluster manager.
     */
    ClusterManager cluster();

    /**
     * Regular configuration of an engine, things like model type, snapshotting
     * frequency, CPU consumption and etc.
     *
     * @return engine configuration manager.
     */
    Configuration config();


    /**
     * This is one the core methods of engine. The whole workflow of system starts from issuing
     * a command, which naturally can be seen as some internal state mutator.
     * <p>
     * Command handler is responsible for execution a number (or single) of transaction actions. This
     * actions either fails or succeed together, cooperating into one atomic operation. If Reveno used in
     * cluster environment, then given command is dispatched to slaves as well, either in sync or async manner
     * (which is configurable thing for each command as well)
     * <p>
     * The commands itself with their appropriate handlers can be registered by calling {@link #domain()} method.
     *
     * @param command to be executed.
     * @return Future representing the state of command execution. Can contain some result of an operation, as well as
     * indicator of success and optional throwable object in case if failure.
     */
    <R> CompletableFuture<Result<R>> executeCommand(Object command);

    /**
     * 执行同步
     * @param command
     * @param <R>
     * @return
     */
    <R> R executeSync(Object command);

    /**
     * Same as {@link #executeCommand(Object)}, but allows to execute a batch of
     * commands as a single atomic transaction.
     *
     * @param commands to be executed.
     * @return Future representing the state of commands execution. Has empty result with only
     * indicator of success of operation, and optional throwable object in case of failure.
     */
    CompletableFuture<EmptyResult> performCommands(List<Object> commands);

    <R> CompletableFuture<Result<R>> execute(DynamicCommand command, Map<String, Object> args);

    <R> R executeSync(DynamicCommand command, Map<String, Object> args);

    <R> R executeSync(String command, Map<String, Object> args);

    <R> R executeSync(String command);

    <R> CompletableFuture<Result<R>> execute(String command, Map<String, Object> args);

    <R> CompletableFuture<Result<R>> execute(String command);

}
