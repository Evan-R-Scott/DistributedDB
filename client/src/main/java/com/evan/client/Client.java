package com.evan.client;

import java.util.Scanner;
import java.util.Set;

public class Client {
    Router router;
    Set<String> commands_with_args;
    Set<String> valid_commands;

    public Client() {
        this.router = new Router();
        this.commands_with_args = Set.of("get", "put", "delete", "remove");
        this.valid_commands = Set.of("get", "put", "delete", "nodes", "remove", "help", "quit");
    }

    public static void main(String[] args) {
        Client client = new Client();
        client.listenForCommands();
    }

    private void listCommandOptions() {
        System.out.println("Available commands:");

        System.out.println("\nQuery commands:");
        System.out.println("  get <key> - Get value for a key");
        System.out.println("  put <key> <value> - Put a key-value pair");
        System.out.println("  delete <key> - Delete a key-value pair");

        System.out.println("\nManagement commands:");
        System.out.println("  nodes - Show all active nodes in the distributed system");
        System.out.println("  remove <node_id> - Remove a node from the distributed system");
        System.out.println("  help - Show this help message");
        System.out.println("  quit - Exit the client\n");
    }

    private void listenForCommands() {
        Scanner scanner = new Scanner(System.in);
        listCommandOptions();
        while (true) {
            System.out.print("Enter command: ");
            String input = scanner.nextLine();
            String[] parts = input.trim().split("\\s+");
            parts[0] = parts[0].toLowerCase();

            if (parts.length == 0) {
                System.out.println("No command entered");
                continue;
            }
            if (!valid_commands.contains(parts[0])) {
                System.out.println("Invalid command: " + parts[0]);
                continue;
            }
            if (parts[0].equals("quit")) {
                break;
            }
            if (parts[0].equals("help")) {
                listCommandOptions();
                continue;
            }
            if (commands_with_args.contains(parts[0])) {
                if (!validateArgumentCount(parts)) {
                    System.out.println("Invalid number of arguments for command: " + parts[0]);
                    continue;
                }
            }
            String response = router.handle(parts);
            System.out.println(response);
        }
        scanner.close();
    }

    private boolean validateArgumentCount(String[] parts) {
        // make sure get/delete/remove have 1 extra part/argument for use
        if (parts[0].equals("get") || parts[0].equals("delete") || parts[0].equals("remove")) {
            if (parts.length != 2) {
                return false;
            }
        }
        // make sure put has 2 extr args to use
        if (parts[0].equals("put")) {
            if (parts.length != 3) {
                return false;
            }
            try {
                Double.parseDouble(parts[2]);
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }
}
