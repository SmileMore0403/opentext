package com.opentext.assignment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@SpringBootApplication
public class AssignmentApplication {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		TaskExecutorService executorService = new TaskExecutorService(3);

		Main.TaskGroup group1 = new Main.TaskGroup(UUID.randomUUID());
		Main.TaskGroup group2 = new Main.TaskGroup(UUID.randomUUID());

		Main.Task<String> task1 = new Main.Task<>(
				UUID.randomUUID(),
				group1,
				Main.TaskType.READ,
				() -> {
					Thread.sleep(1000);
					return "Task 1";
				}
		);

		Main.Task<String> task2 = new Main.Task<>(
				UUID.randomUUID(),
				group1,
				Main.TaskType.READ,
				() -> "Task 2"
		);

		Main.Task<String> task3 = new Main.Task<>(
				UUID.randomUUID(),
				group2,
				Main.TaskType.WRITE,
				() -> "Task 3"
		);

		Future<String> result1 = executorService.submitTask(task1);
		Future<String> result2 = executorService.submitTask(task2);
		Future<String> result3 = executorService.submitTask(task3);

		System.out.println(result1.get()); // Task 1
		System.out.println(result2.get()); // Task 2
		System.out.println(result3.get()); // Task 3

		executorService.shutdown();
	}

}
