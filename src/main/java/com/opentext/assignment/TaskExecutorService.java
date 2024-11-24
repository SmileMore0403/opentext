package com.opentext.assignment;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public record TaskExecutorService(
        ExecutorService executor,
        int maxConcurrency,
        Queue<Main.Task<?>> globalQueue,
        Map<UUID, Queue<Main.Task<?>>> taskGroupQueues,
        Map<UUID, Boolean> taskGroupExecutionStatus
) implements Main.TaskExecutor {

    public TaskExecutorService(int maxConcurrency) {
        this(
                Executors.newFixedThreadPool(maxConcurrency),
                maxConcurrency,
                new LinkedList<>(),
                new HashMap<>(),
                new HashMap<>()
        );

        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("Max concurrency must be greater than 0.");
        }
    }

    @Override
    public <T> Future<T> submitTask(Main.Task<T> task) {
        synchronized (this) {
            // Add task to global queue and task group queue
            globalQueue.add(task);
            taskGroupQueues.computeIfAbsent(task.taskGroup().groupUUID(), k -> new LinkedList<>()).add(task);
            taskGroupExecutionStatus.putIfAbsent(task.taskGroup().groupUUID(), false);
        }
        executeNext();
        return createFutureForTask(task);
    }

    // Handles execution of the next task in the global queue
    private void executeNext() {
        executor.submit(() -> {
            Main.Task<?> nextTask;
            synchronized (this) {
                nextTask = globalQueue.peek(); // Peek without removing to preserve order
                if (nextTask == null || taskGroupExecutionStatus.get(nextTask.taskGroup().groupUUID())) {
                    return; // No task to execute or TaskGroup is already running
                }
                // Set TaskGroup as running
                taskGroupExecutionStatus.put(nextTask.taskGroup().groupUUID(), true);
                // Remove task from queues
                globalQueue.poll();
                taskGroupQueues.get(nextTask.taskGroup().groupUUID()).poll();
            }
            try {
                nextTask.taskAction().call(); // Execute the task
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                // Mark TaskGroup as not running and trigger next execution
                synchronized (this) {
                    taskGroupExecutionStatus.put(nextTask.taskGroup().groupUUID(), false);
                }
                executeNext();
            }
        });
    }

    private <T> Future<T> createFutureForTask(Main.Task<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();
        executor.submit(() -> {
            try {
                T result = task.taskAction().call();
                future.complete(result);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    public void shutdown() {
        executor.shutdown();
    }

}
