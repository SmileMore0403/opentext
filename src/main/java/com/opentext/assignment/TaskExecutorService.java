package com.opentext.assignment;

import java.util.*;
import java.util.concurrent.*;

public class TaskExecutorService<T> {
    private List<Thread> threadPool;
    private int maxConcurrency;
    private Map<UUID, Queue<Main.Task<?>>> taskGroupQueues;
    private Map<UUID, String> taskGroupExecutionStatus;
    private Map<UUID, CompletableFuture<?>> taskFutures;
    private int activeThreads = 0; // Track active threads

    public TaskExecutorService(int maxConcurrency) {
        this.threadPool = new ArrayList<>();
        this.maxConcurrency = maxConcurrency;
        this.taskGroupQueues = new HashMap<>();
        this.taskGroupExecutionStatus = new HashMap<>();
        this.taskFutures = new HashMap<>();

        // Create the thread pool with maxConcurrency threads
        for (int i = 0; i < maxConcurrency; i++) {
            threadPool.add(new Thread(this::processTasks));
        }
    }

    public enum TaskGroupExecutionType {
        READY,
        RUNNING
    }

    // Start all threads
    public void start() {
        for (Thread thread : threadPool) {
            thread.start();
        }
    }

    // Submit a task and return a future
    public <T> Future<T> submitTask(Main.Task<T> task) {

        CompletableFuture<T> future = new CompletableFuture<>();

        // Add the task to the task group queue
        synchronized (this) {
            taskGroupQueues.computeIfAbsent(task.taskGroup().groupUUID(), k -> new LinkedList<>()).add(task);
            taskGroupExecutionStatus.putIfAbsent(task.taskGroup().groupUUID(), String.valueOf(TaskGroupExecutionType.READY));
            taskFutures.put(task.taskUUID(), future);
        }



        // Store the future associated with the task
        return future;
    }

    // Method to process tasks in the task group queues
    private synchronized void processTasks() {
        if (activeThreads < maxConcurrency) {
            for (UUID taskGroupUUID : taskGroupQueues.keySet()) {
                Queue<Main.Task<?>> taskQueue = taskGroupQueues.get(taskGroupUUID);

                if (taskQueue == null || taskQueue.isEmpty()) {
                    continue; // Skip empty queues
                }

                Main.Task<?> task = taskQueue.peek(); // Peek the task from the queue

                // Skip if the task group is already running
                if (String.valueOf(TaskGroupExecutionType.RUNNING).equals(taskGroupExecutionStatus.get(taskGroupUUID))) {
                    continue;
                }

                taskGroupExecutionStatus.put(taskGroupUUID, String.valueOf(TaskGroupExecutionType.RUNNING));

                Thread workerThread = new Thread(() -> {
                    try {
                        synchronized (this) {
                            activeThreads++; // Increment active thread count
                        }

                        System.out.println("Executing task: " + task.taskUUID());

                        T result = (T) task.taskAction().call();

                        CompletableFuture<T> taskFuture = (CompletableFuture<T>) this.taskFutures.get(task.taskUUID());
                        if (taskFuture != null) {
                            taskFuture.complete(result);
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                        CompletableFuture<?> taskFuture = this.taskFutures.get(task.taskUUID());
                        if (taskFuture != null) {
                            taskFuture.completeExceptionally(e);
                        }
                    } finally {
                        synchronized (this) {
                            activeThreads--; // Decrement active thread count after task completion

                            // Remove task from task group queue
                            taskQueue.poll();

                            // If task group queue is empty, mark it as ready
                            if (taskQueue.isEmpty()) {
                                taskGroupExecutionStatus.put(taskGroupUUID, String.valueOf(TaskGroupExecutionType.READY));
                            }

                                processTasks(); // Trigger next task processing only if tasks remain

                        }
                    }
                });

                // Start the worker thread to execute the task
                workerThread.start();
                break; // Exit the loop after starting one task execution thread
            }

            // Sleep to prevent busy-waiting
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // Shutdown the executor service
    public void shutdown() {
        for (Thread thread : threadPool) {
            thread.interrupt();
        }
    }

    // Main method to test the functionality
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TaskExecutorService taskExecutor = new TaskExecutorService(2); // 2 threads for concurrency
        taskExecutor.start(); // Start the threads

        // Example tasks
        Main.Task<String> task1 = new Main.Task<>(
                UUID.randomUUID(),
                new Main.TaskGroup(UUID.randomUUID()), // Task Group 1
                Main.TaskType.WRITE,
                () -> {
                    System.out.println("Task 1 completed");
                    return "Task 1 completed";
                }
        );

        Main.Task<String> task2 = new Main.Task<>(
                UUID.randomUUID(),
                new Main.TaskGroup(UUID.randomUUID()), // Task Group 2
                Main.TaskType.READ,
                () -> {
                    System.out.println("Task 2 completed");
                    return "Task 2 completed";
                }
        );

        System.out.println("Task 1 created with UUID : " + task1.taskUUID());
        System.out.println("Task 2 created with UUID : " + task2.taskUUID());

        // Submit tasks
        Future<String> result1 = taskExecutor.submitTask(task1); // Submit Task 1
        Future<String> result2 = taskExecutor.submitTask(task2); // Submit Task 2

        // Get and print the results of tasks
        System.out.println(result1.get()); // This will print "Task 1 completed"
        System.out.println(result2.get()); // This will print "Task 2 completed"

        taskExecutor.shutdown(); // Shutdown after tasks complete
    }
}
