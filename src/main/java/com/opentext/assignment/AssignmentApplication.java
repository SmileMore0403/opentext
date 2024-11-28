//package com.opentext.assignment;
//
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.UUID;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.Future;
//
//@SpringBootApplication
//public class AssignmentApplication {
//
//	public static void main(String[] args) throws ExecutionException, InterruptedException {
//		TaskExecutorService executorService = new TaskExecutorService(3);
//		executorService.start();
//
//		Main.TaskGroup group1 = new Main.TaskGroup(UUID.randomUUID());
//		Main.TaskGroup group2 = new Main.TaskGroup(UUID.randomUUID());
//
//		Main.Task<String> task1 = new Main.Task<>(
//				UUID.randomUUID(),
//				group1,
//				Main.TaskType.READ,
//				() -> {
//					List<String> arrayList = new ArrayList<>(List.of("apple", "banana", "lichi"));
//					return "Task 1 Completed Successfully - "  + Collections.max(arrayList);
//				}
//		);
//
//		Main.Task<String> task2 = new Main.Task<>(
//				UUID.randomUUID(),
//				group1,
//				Main.TaskType.READ,
//				() -> {
//					int[] arr = {4,2,15,7,8};
//					for(int i = 0; i < arr.length; i++) {
//						System.out.println(arr[i]);
//					}
//					return "Task 2 Completed Successfully";
//				}
//		);
//
//		Main.Task<String> task3 = new Main.Task<>(
//				UUID.randomUUID(),
//				group2,
//				Main.TaskType.WRITE,
//				() -> {
//					int a = 6;
//					int b = 8;
//					a = a + b;
//					b = a - b;
//					a = a - b;
//					System.out.println(a + " , " + b);
//					return "Task 3 : Swap operation completed successfully";
//				}
//		);
//
//		System.out.println("Task 1 created with UUID : " + task1.taskUUID());
//		System.out.println("Task 2 created with UUID : " + task2.taskUUID());
//		System.out.println("Task 3 created with UUID : " + task3.taskUUID());
//
//		//Test 1 : Submittimg tasks sequentiallu
//		Future<String> result1 = executorService.submitTask(task1);
//		System.out.println(result1.get()); // Task 1
//		Future<String> result2 = executorService.submitTask(task2);
//		System.out.println(result2.get()); // Task 2
//		Future<String> result3 = executorService.submitTask(task3);
//		System.out.println(result3.get()); // Task 3
//
//
////		System.out.println("------------------------------- Submitting tasks in one go -------------------------------");
////		//Test 2 : Submittimg tasks in one go
////		List<Future<String>> futures = new ArrayList<>();
////		futures.add(executorService.submitTask(task1));
////		futures.add(executorService.submitTask(task2));
////		futures.add(executorService.submitTask(task3));
////
////		// Loop through the futures and print results as they complete
////		for (Future<String> future : futures) {
////			try {
////				System.out.println(future.get()); // This will print the result when the task completes
////			} catch (InterruptedException | ExecutionException e) {
////				e.printStackTrace();
////			}
////		}
//
//
//		executorService.shutdown();
//	}
//
//}
