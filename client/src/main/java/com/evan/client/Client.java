package com.evan.client;

import java.util.Scanner;
import java.util.Set;

public class Client {
    Router router;
    Set<String> commands_with_args;
    Set<String> valid_commands;

    public Client() {
        this.router = new Router();
        this.commands_with_args = Set.of("get", "history", "getversion", "put", "delete", "remove", "workload");
        this.valid_commands = Set.of("get", "history", "getversion", "put", "delete", "nodes", "health", "workload",
                "help", "quit");
    }

    public static void main(String[] args) {
        new Client().listenForCommands();
    }

    private void listCommandOptions() {
        System.out.println("Available commands:");

        System.out.println("\nQuery commands:");
        System.out.println("  get <key> - Get latest value for a key");
        System.out.println("  history <key> - Show MVCC version history for a key");
        System.out.println("  getversion <key> <version> - Get an exact MVCC version for a key");
        System.out.println("  put <key> <value> - Put a key-value pair");
        System.out.println("  delete <key> - Delete a key-value pair");

        System.out.println("\nLoad test commands:");
        System.out.println("  workload <users> <opsPerUser> <readPercent> <keyspace>");
        System.out.println("    Example: workload 100 500 70 1000");

        System.out.println("\nManagement commands:");
        System.out.println("  nodes - Show all active nodes in the distributed system");
        System.out.println("  health - Show health status of the cluster");
        System.out.println("  help - Show this help message");
        System.out.println("  quit - Exit the client\n");
    }

    private void listenForCommands() {
        Scanner scanner = new Scanner(System.in);
        listCommandOptions();

        while (true) {
            System.out.print("Enter command: ");
            System.out.flush();

            if (!scanner.hasNextLine()) {
                System.out.println("\nInput stream closed. Exiting client.");
                router.shutdown();
                break;
            }

            String input = scanner.nextLine().trim();

            if (input.isEmpty()) {
                System.out.println("No command entered");
                continue;
            }

            String[] parts = input.split("\\s+");
            parts[0] = parts[0].toLowerCase();

            if (!valid_commands.contains(parts[0])) {
                System.out.println("Invalid command: " + parts[0]);
                continue;
            }

            if (parts[0].equals("quit")) {
                System.out.println("Shutting down client...");
                router.shutdown();
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

            if (parts[0].equals("workload")) {
                if (!validateWorkloadArgs(parts)) {
                    continue;
                }
            }

            if (parts[0].equals("getversion")) {
                if (!validateVersionArg(parts[2])) {
                    continue;
                }
            }

            router.handle(parts);
        }

        scanner.close();
        System.out.println("Client exited.");
        System.exit(0);
    }

    private boolean validateArgumentCount(String[] parts) {
        if (parts[0].equals("get") || parts[0].equals("history") || parts[0].equals("delete")
                || parts[0].equals("remove")) {
            return parts.length == 2;
        }

        if (parts[0].equals("put")) {
            return parts.length == 3;
        }

        if (parts[0].equals("getversion")) {
            return parts.length == 3;
        }

        if (parts[0].equals("workload")) {
            return parts.length == 5;
        }

        return true;
    }

    private boolean validateVersionArg(String version) {
        try {
            int parsed = Integer.parseInt(version);
            if (parsed <= 0) {
                System.out.println("version must be > 0");
                return false;
            }
            return true;
        } catch (NumberFormatException e) {
            System.out.println("version must be an integer");
            return false;
        }
    }

    private boolean validateWorkloadArgs(String[] parts) {
        try {
            int users = Integer.parseInt(parts[1]);
            int opsPerUser = Integer.parseInt(parts[2]);
            int readPercent = Integer.parseInt(parts[3]);
            int keyspace = Integer.parseInt(parts[4]);

            if (users <= 0) {
                System.out.println("users must be > 0");
                return false;
            }
            if (opsPerUser <= 0) {
                System.out.println("opsPerUser must be > 0");
                return false;
            }
            if (readPercent < 0 || readPercent > 100) {
                System.out.println("readPercent must be between 0 and 100");
                return false;
            }
            if (keyspace <= 0) {
                System.out.println("keyspace must be > 0");
                return false;
            }

            return true;
        } catch (NumberFormatException e) {
            System.out.println("workload arguments must all be integers");
            return false;
        }
    }
}