import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import * as rolesModule from "../roles";
import type { TreeConfig, TreeNode } from "../roles";
import { initRoles, getSelectedRoles } from "../roles";

/**
 * Test the isNumeric utility function
 * This is a private function in roles.ts, so we'll test it through its usage
 * in other functions rather than directly
 */
describe("roles.ts", () => {
    // Create a mock DOM environment before each test
    let treeContainer: HTMLElement;

    beforeEach(() => {
        // Create a container for the tree
        treeContainer = document.createElement("div");
        treeContainer.id = "jstree_div";
        document.body.appendChild(treeContainer);

        // Mock console methods
        vi.spyOn(console, "warn");
        vi.spyOn(console, "log");
    });

    afterEach(() => {
        // Clean up the DOM
        document.body.innerHTML = "";
        vi.resetAllMocks();

        // Reset any module state by calling initRoles with undefined
        // This ensures treeInstance is set to null
        try {
            initRoles(undefined as unknown as TreeConfig);
        } catch (e) {
            // Ignore error from throwing on undefined config
        }
    });

    describe("initRoles", () => {
        it("should initialize the role selection tree with provided container", () => {
            // Mock the config
            const config: TreeConfig = {
                core: {
                    data: [
                        { id: "1", text: "Role 1" },
                        { id: "2", text: "Role 2", parent: "1" },
                    ],
                },
                plugins: ["checkbox"],
            };

            // Initialize the roles with the existing container
            const container = initRoles(config, treeContainer);

            // Check if the tree container has been populated
            expect(container).toBe(treeContainer);
            expect(treeContainer.innerHTML).not.toBe("");
            expect(treeContainer.querySelector(".tree-root")).not.toBeNull();
            expect(treeContainer.querySelectorAll(".tree-node").length).toBe(3);
        });

        it("should create a new container if none provided", () => {
            // Mock the config
            const config: TreeConfig = {
                core: {
                    data: [
                        { id: "1", text: "Role 1" },
                        { id: "2", text: "Role 2", parent: "1" },
                    ],
                },
                plugins: ["checkbox"],
            };

            // Remove existing container
            document.body.removeChild(treeContainer);

            // Initialize the roles without a container
            const container = initRoles(config);

            // Check if a new container was created
            expect(container).not.toBe(treeContainer);
            expect(container.id).toBe("jstree_container");
            expect(container.className).toBe("jstree-container");
            expect(container.innerHTML).not.toBe("");
            expect(container.querySelector(".tree-root")).not.toBeNull();
        });

        it("should throw error on undefined config", () => {
            // Expect an error when config is undefined
            expect(() => {
                initRoles(undefined as unknown as TreeConfig);
            }).toThrow("Tree configuration is required");
        });

        it("should handle container specified by selector", () => {
            // Create a uniquely identifiable container
            const uniqueContainer = document.createElement("div");
            uniqueContainer.id = "unique-container";
            document.body.appendChild(uniqueContainer);

            // Mock the config
            const config: TreeConfig = {
                core: {
                    data: [{ id: "1", text: "Role 1" }],
                },
            };

            // Initialize with selector
            const container = initRoles(config, "#unique-container");

            // Should find and use our unique container
            expect(container).toBe(uniqueContainer);
            expect(uniqueContainer.innerHTML).not.toBe("");
        });

        it("should create a new container if selector doesn't match", () => {
            // Mock the config
            const config: TreeConfig = {
                core: {
                    data: [{ id: "1", text: "Role 1" }],
                },
            };

            // Initialize with non-existent selector
            const container = initRoles(config, "#non-existent");

            // Should create a new container
            expect(container.id).toBe("jstree_container");
            expect(console.warn).toHaveBeenCalled();
        });
    });

    describe("getSelectedRoles", () => {
        it("should return an empty array if tree is not initialized", () => {
            // Make sure tree instance is null
            try {
                initRoles(undefined as unknown as TreeConfig);
            } catch (e) {
                // Ignore error from throwing on undefined config
            }

            // Get selected roles without initializing tree
            const roles = getSelectedRoles();

            // Check result - should be empty array
            expect(roles).toEqual([]);

            // Verify warning was logged
            expect(console.warn).toHaveBeenCalledWith(
                "Roles tree not initialized",
            );
        });

        it("should return selected roles", () => {
            // Mock the config with pre-selected nodes
            const config: TreeConfig = {
                core: {
                    data: [
                        {
                            id: "parent1",
                            text: "Parent 1",
                            state: { selected: true },
                        },
                        { id: "child1", text: "Child 1", parent: "parent1" },
                        { id: "parent2", text: "Parent 2" },
                        {
                            id: "child2",
                            text: "Child 2",
                            parent: "parent2",
                            state: { selected: true },
                        },
                    ],
                },
                plugins: ["checkbox"],
            };

            // Initialize the roles with the test container
            const container = initRoles(config, treeContainer);

            // Simulate clicking on checkboxes
            const checkboxes = container.querySelectorAll<HTMLInputElement>(
                "input[type='checkbox']",
            );
            checkboxes.forEach((checkbox) => {
                if (checkbox.checked) {
                    // Trigger change event to register selection
                    checkbox.dispatchEvent(new Event("change"));
                }
            });

            // Get selected roles
            const roles = getSelectedRoles();

            // Should include Parent 1 but not Child 2 (pruned)
            expect(roles).toContain("Parent 1");
            expect(roles).not.toContain("Child 1"); // Child is pruned because parent is selected
            expect(roles).toContain("Child 2");
        });

        it("should prune child roles when parent is selected", () => {
            // Create a more complex tree structure
            const config: TreeConfig = {
                core: {
                    data: [
                        { id: "group1", text: "Group 1" },
                        { id: "role1", text: "Role 1", parent: "group1" },
                        { id: "role2", text: "Role 2", parent: "group1" },
                        { id: "group2", text: "Group 2" },
                        { id: "role3", text: "Role 3", parent: "group2" },
                    ],
                },
                plugins: ["checkbox"],
            };

            // Initialize the roles with the test container
            const container = initRoles(config, treeContainer);

            // Simulate selecting group1 and role3
            const nodes = container.querySelectorAll(".tree-node");

            // Find the checkboxes for group1 and role3
            const group1Node = Array.from(nodes).find(
                (node) => (node as HTMLElement).dataset.id === "group1",
            );
            const role3Node = Array.from(nodes).find(
                (node) => (node as HTMLElement).dataset.id === "role3",
            );

            if (group1Node && role3Node) {
                const group1Checkbox =
                    group1Node.querySelector<HTMLInputElement>(
                        "input[type='checkbox']",
                    );
                const role3Checkbox = role3Node.querySelector<HTMLInputElement>(
                    "input[type='checkbox']",
                );

                if (group1Checkbox && role3Checkbox) {
                    // Check the boxes
                    group1Checkbox.checked = true;
                    role3Checkbox.checked = true;

                    // Trigger change events
                    group1Checkbox.dispatchEvent(new Event("change"));
                    role3Checkbox.dispatchEvent(new Event("change"));

                    // Get selected roles
                    const roles = getSelectedRoles();

                    // Should include Group 1 and Role 3, but not Role 1 or Role 2
                    expect(roles).toContain("Group 1");
                    expect(roles).not.toContain("Role 1"); // Pruned
                    expect(roles).not.toContain("Role 2"); // Pruned
                    expect(roles).toContain("Role 3");
                }
            }
        });

        it("should handle numeric IDs properly", () => {
            // Create a tree with numeric IDs
            const config: TreeConfig = {
                core: {
                    data: [
                        { id: 1, text: "Role 1" },
                        { id: 2, text: "Role 2", parent: 1 },
                        { id: "3", text: "Role 3" }, // String ID
                    ],
                },
                plugins: ["checkbox"],
            };

            // Initialize the roles with the test container
            const container = initRoles(config, treeContainer);

            // Simulate selecting role 1 and role 3
            const nodes = container.querySelectorAll(".tree-node");

            // Find checkboxes for roles 1 and 3
            const role1Node = Array.from(nodes).find(
                (node) => (node as HTMLElement).dataset.id === "1",
            );
            const role3Node = Array.from(nodes).find(
                (node) => (node as HTMLElement).dataset.id === "3",
            );

            if (role1Node && role3Node) {
                const role1Checkbox = role1Node.querySelector<HTMLInputElement>(
                    "input[type='checkbox']",
                );
                const role3Checkbox = role3Node.querySelector<HTMLInputElement>(
                    "input[type='checkbox']",
                );

                if (role1Checkbox && role3Checkbox) {
                    // Check the boxes
                    role1Checkbox.checked = true;
                    role3Checkbox.checked = true;

                    // Trigger change events
                    role1Checkbox.dispatchEvent(new Event("change"));
                    role3Checkbox.dispatchEvent(new Event("change"));

                    // Get selected roles
                    const roles = getSelectedRoles();

                    // Should include both Role 1 and Role 3, but not Role 2
                    expect(roles).toContain("Role 1");
                    expect(roles).not.toContain("Role 2"); // Pruned
                    expect(roles).toContain("Role 3");
                }
            }
        });
    });

    // Additional test for edge cases
    describe("Edge Cases", () => {
        it("should handle empty tree data", () => {
            // Initialize with empty data
            initRoles({
                core: {
                    data: [],
                },
            });

            // Tree should be initialized but empty
            expect(treeContainer.innerHTML).not.toBe("");
            expect(treeContainer.querySelector(".tree-root")).not.toBeNull();
            expect(treeContainer.querySelectorAll(".tree-node").length).toBe(0);

            // Get selected roles should return empty array
            expect(getSelectedRoles()).toEqual([]);
        });

        it("should handle tree with icons", () => {
            // Initialize with nodes that have icons
            initRoles({
                core: {
                    data: [{ id: "1", text: "Role 1", icon: "✅" }],
                },
            });

            // Check if icon is rendered
            const iconElement = treeContainer.querySelector(".tree-icon");
            expect(iconElement).not.toBeNull();
            expect(iconElement?.textContent).toBe("✅");
        });

        it("should handle nodes without parent", () => {
            // All nodes at root level
            initRoles({
                core: {
                    data: [
                        { id: "1", text: "Role 1" },
                        { id: "2", text: "Role 2" },
                        { id: "3", text: "Role 3" },
                    ],
                },
                plugins: ["checkbox"],
            });

            // Should render all nodes at root level
            const rootChildren =
                treeContainer.querySelector(".tree-root")?.children;
            expect(rootChildren?.length).toBe(3);
        });
    });
});
