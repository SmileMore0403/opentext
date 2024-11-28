package com.opentext.assignment;


import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class TaskExecutorService {
    private final int maxConcurrency;
    private final Map<UUID, Queue<Main.Task<?>>> taskGroupQueues;
    private final Map<UUID, Boolean> taskGroupRunningStatus; // Tracks if a group is running
    private int activeThreads = 0; // Tracks total active threads

    public TaskExecutorService(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
        this.taskGroupQueues = new HashMap<>();
        this.taskGroupRunningStatus = new HashMap<>();
    }

    public <T> Future<T> submitTask(Main.Task<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();

        synchronized (this) {
            // Add task to the appropriate task group queue
            taskGroupQueues.computeIfAbsent(task.taskGroup().groupUUID(), k -> new LinkedList<>()).add(task);

            // If the group is not running, mark it as ready to run
            taskGroupRunningStatus.putIfAbsent(task.taskGroup().groupUUID(), false);

            // Start a dedicated thread for this task group if not already running
            if (!taskGroupRunningStatus.get(task.taskGroup().groupUUID())) {
                startTaskGroupThread(task.taskGroup().groupUUID());
            }
        }

        return future;
    }

    private void startTaskGroupThread(UUID taskGroupUUID) {
        // Mark the group as running
        taskGroupRunningStatus.put(taskGroupUUID, true);

        // Create a thread to process tasks for this group
        new Thread(() -> processTaskGroup(taskGroupUUID)).start();
    }

    private void processTaskGroup(UUID taskGroupUUID) {
        while (true) {
            Main.Task<?> task;

            synchronized (this) {
                // Get the task queue for this group
                Queue<Main.Task<?>> taskQueue = taskGroupQueues.get(taskGroupUUID);

                if (taskQueue == null || taskQueue.isEmpty()) {
                    // Mark the group as not running and exit if no tasks remain
                    taskGroupRunningStatus.put(taskGroupUUID, false);
                    return;
                }

                // If max concurrency is reached, wait
                if (activeThreads >= maxConcurrency) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    continue;
                }

                // Dequeue the next task and mark the group as running
                task = taskQueue.poll();
                activeThreads++;
            }

            try {
                System.out.println("Executing task: " + task.taskUUID() + " from group: " + task.taskGroup().groupUUID());

                // Execute the task
                Object result = task.taskAction().call();

                // Complete the task's future (if applicable)
                CompletableFuture<Object> future = new CompletableFuture<>();
                if (future != null) {
                    future.complete(result);
                }

                System.out.println("Completed task: " + task.taskUUID() + " from group: " + task.taskGroup().groupUUID());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                synchronized (this) {
                    activeThreads--; // Decrement active thread count after task completion
                    notifyAll(); // Notify other threads waiting for concurrency slots
                }
            }
        }
    }

    public void shutdown() {
        synchronized (this) {
            taskGroupQueues.clear();
            activeThreads = 0;
        }
        System.out.println("TaskExecutorService has been shut down.");
    }

    // Test the implementation
    public static void main(String[] args) throws Exception {
        TaskExecutorService taskExecutor = new TaskExecutorService(2); // Max 2 threads at a time

        // Example Task Group 1
        Main.Task<String> task1 = new Main.Task<>(
                UUID.randomUUID(),
                new Main.TaskGroup(UUID.randomUUID()), // Group 1
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(1000); // Simulate task execution time
                    System.out.println("Task 1 completed");
                    return "Task 1 completed";
                }
        );

        Main.Task<String> task2 = new Main.Task<>(
                UUID.randomUUID(),
                task1.taskGroup(), // Group 1
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(500); // Simulate task execution time
                    System.out.println("Task 2 completed");
                    return "Task 2 completed";
                }
        );

        // Example Task Group 2
        Main.Task<String> task3 = new Main.Task<>(
                UUID.randomUUID(),
                new Main.TaskGroup(UUID.randomUUID()), // Group 2
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(700); // Simulate task execution time
                    System.out.println("Task 3 completed");
                    return "Task 3 completed";
                }
        );

        Main.Task<String> task4 = new Main.Task<>(
                UUID.randomUUID(),
                task3.taskGroup(), // Group 2
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(400); // Simulate task execution time
                    System.out.println("Task 4 completed");
                    return "Task 4 completed";
                }
        );

        // Submit tasks
        taskExecutor.submitTask(task1);
        taskExecutor.submitTask(task2);
        taskExecutor.submitTask(task3);
        taskExecutor.submitTask(task4);

        // No explicit start method is required as task group threads are created on demand
    }
}
