package com.opentext.assignment;


import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskExecutorService implements Main.TaskExecutor {
    private final int maxConcurrency;
    private final Map<UUID, Queue<Main.Task<?>>> taskGroupQueues;
    private final Map<UUID, TaskGroupExecutionType> taskGroupRunningStatus;

    private final Map<UUID, Future<?>> taskFuture;
    private int activeThreads = 0;

    private final AtomicBoolean isShutdown;

    public enum TaskGroupExecutionType {
        READY,
        RUNNING
    }

    public TaskExecutorService(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
        this.taskGroupQueues = new HashMap<>();
        this.taskGroupRunningStatus = new HashMap<>();
        this.taskFuture = new HashMap<>();
        this.isShutdown = new AtomicBoolean(false);
    }

    @Override
    public <T> Future<T> submitTask(Main.Task<T> task) {

        CompletableFuture<T> future = new CompletableFuture<>();

        synchronized (this) {

            taskGroupQueues.computeIfAbsent(task.taskGroup().groupUUID(), k -> new LinkedList<>()).add(task);

            taskGroupRunningStatus.putIfAbsent(task.taskGroup().groupUUID(), TaskGroupExecutionType.READY);

            taskFuture.put(task.taskUUID(), future);

            if (TaskGroupExecutionType.READY.equals(taskGroupRunningStatus.get(task.taskGroup().groupUUID()))) {
                startTaskGroupThread(task.taskGroup().groupUUID());
            }
        }

        return (Future<T>) taskFuture.get(task.taskUUID());
    }

    private void startTaskGroupThread(UUID taskGroupUUID) {

        taskGroupRunningStatus.put(taskGroupUUID, TaskGroupExecutionType.RUNNING);

        new Thread(() -> processTaskGroup(taskGroupUUID)).start();
    }

    private void processTaskGroup(UUID taskGroupUUID) {
        while (true) { // Iteratively process tasks in the group

            if (isShutdown.get()) {
                throw new RejectedExecutionException("TaskExecutorService is shut down");
            }

            Main.Task<?> task;

            synchronized (this) {
                Queue<Main.Task<?>> taskQueue = taskGroupQueues.get(taskGroupUUID);

                if (taskQueue == null || taskQueue.isEmpty() || isShutdown.get()) {
                    // Mark the group as ready and exit if no tasks remain
                    taskGroupRunningStatus.put(taskGroupUUID, TaskGroupExecutionType.READY);
                    return;
                }

                // If max concurrency is reached, wait
                while (activeThreads >= maxConcurrency) {
                    try {
                        System.out.println("Waiting tasks : " + taskQueue.peek());
                        wait(); // Wait until a thread becomes available
                        if (isShutdown.get()) {
                            throw new RejectedExecutionException("TaskExecutorService is shut down");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        taskGroupRunningStatus.put(taskGroupUUID, TaskGroupExecutionType.READY);
                        return;
                    }
                }

                // Dequeue the next task and increment active threads
                task = taskQueue.poll();
                activeThreads++;
            }

            try {
                System.out.println("Executing task: " + task.taskUUID() + " from group: " + task.taskGroup().groupUUID()
                        + " on Thread : " + Thread.currentThread().getName());
                Object result = task.taskAction().call();

                synchronized (this) {
                    // Complete the associated CompletableFuture
                    CompletableFuture<Object> future = (CompletableFuture<Object>) taskFuture.get(task.taskUUID());
                    if (future != null) {
                        future.complete(result);
                    }


                }
            } catch (Exception e) {
                synchronized (this) {
                    CompletableFuture<Object> future = (CompletableFuture<Object>) taskFuture.get(task.taskUUID());
                    if (future != null) {
                        future.completeExceptionally(e);
                    }
                }
                e.printStackTrace();
            } finally {
                synchronized (this) {
                    activeThreads--;
                    notifyAll(); // Notify waiting threads that a slot is available
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

    public void shutDownNow(List<Future<?>> futures) {
        if (isShutdown.compareAndSet(false, true)) {
            shutdown();
            System.out.println("TaskExecutorService has been shut down now.");
            for (Future<?> runningTask : futures) {
                runningTask.cancel(true); // Interrupt actively running tasks
            }
        }

    }

    public List<Future<?>> invokeAll(Collection<Main.Task<?>> tasks) {
        List<Future<?>> futures = new ArrayList<>();
        for (Main.Task<?> task : tasks) {
            futures.add(submitTask(task));
        }
        return futures;
    }

    // Test the implementation
    public static <T> void main(String[] args) throws Exception {

//        testScenario1(); //Submit One task
//        testScenario2(); //Executing tasks concurrently with two task Groups and maxConcurrency as 2
//        testScenario3(); //Executing tasks concurrently with three task Groups and maxConcurrency as 2
//        testScenario4(); //Verify task execution order for one group and tasks in same group are not running concurrently
//        testScenario5(); //Submitting tasks in one go with two task Groups and maxConcurrency as 2
//        testScenario6(); //Verifying tasks delay with two groups
//        testScenario7(); //verify task creation with null task group
//        testScenario8(); //verify 2 task creation with null task group
//        testScenario9(); //verify 2 task creation with null task group
        testScenario10(); //Executing tasks concurrently with four task Groups and maxConcurrency as 2

    }

    private static void testScenario1() throws ExecutionException, InterruptedException {

        System.out.println("Test Scenario 1 - Executing one task");
        TaskExecutorService taskExecutor = new TaskExecutorService(2);

        Main.TaskGroup taskGroup1 = new Main.TaskGroup(UUID.randomUUID());

        Main.Task<String> task1 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup1,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(1000);
                    return "Task 1 completed";
                }
        );

        System.out.println("Task 1 : " + task1.taskUUID());

        Future<String> future1 = taskExecutor.submitTask(task1);

        System.out.println(future1.get());

        // Allow tasks to complete
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        taskExecutor.shutdown();

    }

    private static void testScenario2() throws ExecutionException, InterruptedException {

        System.out.println("Test Scenario 2 - Executing tasks concurrently with two task Groups and maxConcurrency as 2");
        TaskExecutorService taskExecutor = new TaskExecutorService(2);

        Main.TaskGroup taskGroup1 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup taskGroup2 = new Main.TaskGroup(UUID.randomUUID());

        Main.Task<String> task1 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup1,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(1000);
                    return "Task 1 completed";
                }
        );

        Main.Task<String> task2 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup2,
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(200);
                    return "Task 2 completed";
                }
        );


        Main.Task<String> task3 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup2,
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(500);
                    return "Task 3 completed";
                }
        );

        Main.Task<String> task4 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup1,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(800);
                    return "Task 4 completed";
                }
        );

        System.out.println("Task 1 : " + task1.taskUUID());
        System.out.println("Task 2 : " + task2.taskUUID());
        System.out.println("Task 3 : " + task3.taskUUID());
        System.out.println("Task 4 : " + task4.taskUUID());

        Future<String> future1 = taskExecutor.submitTask(task1);
        Future<String> future2 = taskExecutor.submitTask(task2);
        Future<String> future3 = taskExecutor.submitTask(task3);
        Future<String> future4 = taskExecutor.submitTask(task4);

        System.out.println(future1.get());
        System.out.println(future2.get());
        System.out.println(future3.get());
        System.out.println(future4.get());

        // Allow tasks to complete
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        taskExecutor.shutdown();

    }

    private static void testScenario3() throws ExecutionException, InterruptedException {

        System.out.println("Test Scenario 3 - Executing tasks concurrently with three task Groups " +
                "and maxConcurrency as 2");
        TaskExecutorService taskExecutor = new TaskExecutorService(2);

        Main.TaskGroup taskGroup1 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup taskGroup2 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup taskGroup3 = new Main.TaskGroup(UUID.randomUUID());

        Main.Task<String> task1 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup1,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(1000);
                    return "Task 1 completed";
                }
        );

        Main.Task<String> task2 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup2,
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(200);
                    return "Task 2 completed";
                }
        );


        Main.Task<String> task3 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup2,
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(500);
                    return "Task 3 completed";
                }
        );

        Main.Task<String> task4 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup3,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(800);
                    return "Task 4 completed";
                }
        );

        System.out.println("Task 1 : " + task1.taskUUID());
        System.out.println("Task 2 : " + task2.taskUUID());
        System.out.println("Task 3 : " + task3.taskUUID());
        System.out.println("Task 4 : " + task4.taskUUID());

        Future<String> future1 = taskExecutor.submitTask(task1);
        Future<String> future2 = taskExecutor.submitTask(task2);
        Future<String> future3 = taskExecutor.submitTask(task3);
        Future<String> future4 = taskExecutor.submitTask(task4);

        System.out.println(future1.get());
        System.out.println(future2.get());
        System.out.println(future3.get());
        System.out.println(future4.get());

        // Allow tasks to complete
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        taskExecutor.shutdown();

    }

    private static void testScenario4() throws ExecutionException, InterruptedException {

        System.out.println("Test Scenario 4 - Verufying execution order in task group");
        TaskExecutorService taskExecutor = new TaskExecutorService(2);

        Main.TaskGroup taskGroup1 = new Main.TaskGroup(UUID.randomUUID());

        Main.Task<String> task1 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup1,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(1000);
                    return "Task 1 completed";
                }
        );

        Main.Task<String> task2 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup1,
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(1000);
                    return "Task 2 completed";
                }
        );


        Main.Task<String> task3 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup1,
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(1500);
                    return "Task 3 completed";
                }
        );

        Main.Task<String> task4 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup1,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(800);
                    return "Task 4 completed";
                }
        );

        System.out.println("Task 1 : " + task1.taskUUID());
        System.out.println("Task 2 : " + task2.taskUUID());
        System.out.println("Task 3 : " + task3.taskUUID());
        System.out.println("Task 4 : " + task4.taskUUID());

        Future<String> future1 = taskExecutor.submitTask(task1);
        Future<String> future2 = taskExecutor.submitTask(task2);
        Future<String> future3 = taskExecutor.submitTask(task3);
        Future<String> future4 = taskExecutor.submitTask(task4);

        System.out.println(future1.get());
        System.out.println(future2.get());
        System.out.println(future3.get());
        System.out.println(future4.get());

        // Allow tasks to complete
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        taskExecutor.shutdown();

    }

    private static void testScenario5() throws ExecutionException, InterruptedException {

        System.out.println("Test Scenario  - Verify tasks in one go with two task Groups and maxConcurrency as 2 ");
        TaskExecutorService taskExecutor = new TaskExecutorService(2);

        Main.TaskGroup taskGroup1 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup taskGroup2 = new Main.TaskGroup(UUID.randomUUID());

        Main.Task<String> task1 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup1,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(1000);
                    return "Task 1 completed";
                }
        );

        Main.Task<String> task2 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup2,
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(200);
                    return "Task 2 completed";
                }
        );


        Main.Task<String> task3 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup2,
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(500);
                    return "Task 3 completed";
                }
        );

        Main.Task<String> task4 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup1,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(800);
                    return "Task 4 completed";
                }
        );

        System.out.println("Task 1 : " + task1.taskUUID());
        System.out.println("Task 2 : " + task2.taskUUID());
        System.out.println("Task 3 : " + task3.taskUUID());
        System.out.println("Task 4 : " + task4.taskUUID());

        // Using invokeAll to submit all tasks at once
        List<Future<?>> futures = taskExecutor.invokeAll(List.of(task1, task2, task3, task4));

        // Wait for each future to complete and print results
        for (Future<?> future : futures) {
            System.out.println(future.get());
        }

        // Allow tasks to complete
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        taskExecutor.shutdown();

    }

    private static void testScenario6() throws ExecutionException, InterruptedException {

        System.out.println("Test Scenario  - Verify task Delay with two groups");
        TaskExecutorService taskExecutor = new TaskExecutorService(2);

        Main.TaskGroup taskGroup1 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup taskGroup2 = new Main.TaskGroup(UUID.randomUUID());

        Main.Task<String> task1 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup1,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(3000);
                    return "Task 1 completed";
                }
        );

        Main.Task<String> task2 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup2,
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(200);
                    return "Task 2 completed";
                }
        );


        Main.Task<String> task3 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup2,
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(500);
                    return "Task 3 completed";
                }
        );

        Main.Task<String> task4 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup1,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(100);
                    return "Task 4 completed";
                }
        );

        System.out.println("Task 1 : " + task1.taskUUID());
        System.out.println("Task 2 : " + task2.taskUUID());
        System.out.println("Task 3 : " + task3.taskUUID());
        System.out.println("Task 4 : " + task4.taskUUID());

        Future<String> future1 = taskExecutor.submitTask(task1);
        Future<String> future2 = taskExecutor.submitTask(task2);
        Future<String> future3 = taskExecutor.submitTask(task3);
        Future<String> future4 = taskExecutor.submitTask(task4);

        System.out.println(future1.get());
        System.out.println(future2.get());
        System.out.println(future3.get());
        System.out.println(future4.get());


        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        taskExecutor.shutdown();

    }

    private static void testScenario7() throws ExecutionException, InterruptedException {

        System.out.println("Test Scenario 7 - Executing task with null task group");
        TaskExecutorService taskExecutor = new TaskExecutorService(2);

        Main.Task<String> task1 = new Main.Task<>(
                UUID.randomUUID(),
                null,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(1000);
                    return "Task 1 completed";
                }
        );

        System.out.println("Task 1 : " + task1.taskUUID());

        Future<String> future1 = taskExecutor.submitTask(task1);

        System.out.println(future1.get());

        // Allow tasks to complete
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        taskExecutor.shutdown();

    }

    private static void testScenario8() throws ExecutionException, InterruptedException {

        System.out.println("Test Scenario 8 - Executing 2 tasks with null task group");
        TaskExecutorService taskExecutor = new TaskExecutorService(2);

        Main.Task<String> task1 = new Main.Task<>(
                UUID.randomUUID(),
                null,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(1000);
                    return "Task 1 completed";
                }
        );

        Main.Task<String> task2 = new Main.Task<>(
                UUID.randomUUID(),
                null,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(1000);
                    return "Task 2 completed";
                }
        );

        System.out.println("Task 1 : " + task1.taskUUID());
        System.out.println("Task 2 : " + task2.taskUUID());

        Future<String> future1 = taskExecutor.submitTask(task1);
        Future<String> future2 = taskExecutor.submitTask(task2);

        System.out.println(future1.get());
        System.out.println(future2.get());

        // Allow tasks to complete
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        taskExecutor.shutdown();

    }

    private static void testScenario9() throws ExecutionException, InterruptedException {

        System.out.println("Test Scenario 9 - Executing tasks task executor " +
                "getting shutdown before completion of task execution");
        TaskExecutorService taskExecutor = new TaskExecutorService(2);
        Main.TaskGroup taskGroup1 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup taskGroup2 = new Main.TaskGroup(UUID.randomUUID());

        Main.Task<String> task1 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup1,
                Main.TaskType.WRITE,
                () -> {
                    return "Task 1 completed";
                }
        );

        Main.Task<String> task2 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup2,
                Main.TaskType.WRITE,
                () -> {
                    return "Task 2 completed";
                }
        );

        Main.Task<String> task3 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup2,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(1000);
                    return "Task 3 completed";
                }
        );

        System.out.println("Task 1 : " + task1.taskUUID());
        System.out.println("Task 2 : " + task2.taskUUID());
        System.out.println("Task 3 : " + task3.taskUUID());

        // Using invokeAll to submit all tasks at once
        List<Future<?>> futures = taskExecutor.invokeAll(List.of(task1, task2, task3));

        taskExecutor.shutDownNow(futures);

        // Wait for each future to complete and print results
        try {
            for (Future<?> future : futures) {
                System.out.println(future.get());
            }
        } catch(CancellationException e) {
            System.out.println("Tasks have been cancelled" + futures);
        }

    }

    private static void testScenario10() throws ExecutionException, InterruptedException {

        System.out.println("Test Scenario 10 - Executing tasks concurrently with four task Groups " +
                "and maxConcurrency as 2");
        TaskExecutorService taskExecutor = new TaskExecutorService(2);

        Main.TaskGroup taskGroup1 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup taskGroup2 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup taskGroup3 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup taskGroup4 = new Main.TaskGroup(UUID.randomUUID());

        Main.Task<String> task1 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup1,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(1000);
                    return "Task 1 completed";
                }
        );

        Main.Task<String> task2 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup2,
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(200);
                    return "Task 2 completed";
                }
        );


        Main.Task<String> task3 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup4,
                Main.TaskType.READ,
                () -> {
                    Thread.sleep(500);
                    return "Task 3 completed";
                }
        );

        Main.Task<String> task4 = new Main.Task<>(
                UUID.randomUUID(),
                taskGroup3,
                Main.TaskType.WRITE,
                () -> {
                    Thread.sleep(800);
                    return "Task 4 completed";
                }
        );

        System.out.println("Task 1 : " + task1.taskUUID());
        System.out.println("Task 2 : " + task2.taskUUID());
        System.out.println("Task 3 : " + task3.taskUUID());
        System.out.println("Task 4 : " + task4.taskUUID());

        Future<String> future1 = taskExecutor.submitTask(task1);
        Future<String> future2 = taskExecutor.submitTask(task2);
        Future<String> future3 = taskExecutor.submitTask(task3);
        Future<String> future4 = taskExecutor.submitTask(task4);

        System.out.println(future1.get());
        System.out.println(future2.get());
        System.out.println(future3.get());
        System.out.println(future4.get());

        // Allow tasks to complete
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        taskExecutor.shutdown();

    }

}
