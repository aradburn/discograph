/**
 * Role management functionality for Discograph
 * This file handles the initialization and management of role selection using vanilla JavaScript
 */

import { TREE } from "./constants";

/**
 * Interface for tree node state
 */
export interface TreeNodeState {
    selected?: boolean;
}

/**
 * Interface for tree node data
 */
export interface TreeNode {
    id: string | number;
    text: string;
    icon?: string;
    parent?: string | number;
    state?: TreeNodeState;
}

/**
 * Interface for tree configuration
 */
export interface TreeConfig {
    core: {
        data: TreeNode[];
    };
    plugins?: string[];
}

/**
 * Interface for selected node details
 */
interface SelectedNodeDetails {
    id: string | number;
    text: string;
    children: (string | number)[];
}

/**
 * Utility function to check if a value is numeric
 * @param {unknown} obj - The value to check
 * @returns {boolean} - True if the value is numeric, false otherwise
 */
const isNumeric = (obj: unknown): boolean => {
    if (typeof obj === "number") return true;
    if (typeof obj !== "string") return false;
    return (
        !Array.isArray(obj) && !isNaN(Number(obj)) && !isNaN(parseFloat(obj))
    );
};

class TreeComponent {
    private container: HTMLElement;
    private config: TreeConfig;
    private selectedNodes: Set<string | number>;

    constructor(container: HTMLElement, config: TreeConfig) {
        this.container = container;
        this.config = config;
        this.selectedNodes = new Set();
        this.init();
    }

    private init(): void {
        // Clear existing content
        this.container.innerHTML = "";

        // Create tree structure
        const treeRoot = document.createElement("ul");
        treeRoot.className = TREE.CLASS_NAMES.ROOT;

        // Add nodes from config
        if (this.config?.core?.data) {
            this.buildTreeNodes(treeRoot, this.config.core.data);
        }

        this.container.appendChild(treeRoot);

        // Add styles if not already present
        if (!document.getElementById(TREE.TREE_STYLES)) {
            const styles = document.createElement("style");
            styles.id = TREE.TREE_STYLES;
            styles.textContent = `
                .${TREE.CLASS_NAMES.ROOT} {
                    list-style: none;
                    padding-left: ${TREE.PADDING_LEFT}px;
                }
                .${TREE.CLASS_NAMES.NODE} {
                    margin: ${TREE.MARGIN}px 0;
                }
                .${TREE.CLASS_NAMES.CONTENT} {
                    display: flex;
                    align-items: center;
                    gap: ${TREE.MARGIN}px;
                }
                .${TREE.CLASS_NAMES.CHECKBOX} {
                    margin: 0;
                }
                .${TREE.CLASS_NAMES.ICON} {
                    width: ${TREE.ICON_SIZE}px;
                    height: ${TREE.ICON_SIZE}px;
                }
                .${TREE.CLASS_NAMES.CHILDREN} {
                    list-style: none;
                    padding-left: ${TREE.PADDING_LEFT}px;
                }
            `;
            document.head.appendChild(styles);
        }
    }

    private buildTreeNodes(
        parentElement: HTMLElement,
        nodes: TreeNode[],
    ): void {
        nodes.forEach((node) => {
            const li = document.createElement("li");
            li.className = TREE.CLASS_NAMES.NODE;
            li.dataset.id = String(node.id);

            const content = document.createElement("div");
            content.className = TREE.CLASS_NAMES.CONTENT;

            // Add checkbox if enabled in config
            if (this.config.plugins?.includes("checkbox")) {
                const checkbox = document.createElement("input");
                checkbox.type = "checkbox";
                checkbox.className = TREE.CLASS_NAMES.CHECKBOX;
                checkbox.checked = node.state?.selected || false;
                checkbox.addEventListener("change", () =>
                    this.handleNodeSelection(node.id, checkbox.checked),
                );
                content.appendChild(checkbox);
            }

            // Add icon if present
            if (node.icon) {
                const icon = document.createElement("span");
                icon.className = TREE.CLASS_NAMES.ICON;
                icon.textContent = node.icon;
                content.appendChild(icon);
            }

            // Add text
            const text = document.createElement("span");
            text.className = TREE.CLASS_NAMES.TEXT;
            text.textContent = node.text;
            content.appendChild(text);

            li.appendChild(content);

            // Add child nodes if any exist
            const childNodes = nodes.filter((n) => n.parent === node.id);
            if (childNodes.length > 0) {
                const childrenContainer = document.createElement("ul");
                childrenContainer.className = TREE.CLASS_NAMES.CHILDREN;
                this.buildTreeNodes(childrenContainer, childNodes);
                li.appendChild(childrenContainer);
            }

            parentElement.appendChild(li);
        });
    }

    private handleNodeSelection(
        nodeId: string | number,
        selected: boolean,
    ): void {
        if (selected) {
            this.selectedNodes.add(nodeId);
        } else {
            this.selectedNodes.delete(nodeId);
        }
    }

    get_selected(
        withDetails = false,
    ): (string | number)[] | SelectedNodeDetails[] {
        if (!withDetails) {
            return Array.from(this.selectedNodes);
        }

        return Array.from(this.selectedNodes).map((id) => {
            const node = this.findNodeById(id);
            if (!node) {
                throw new Error(`Node with id ${id} not found`);
            }
            return {
                id: node.id,
                text: node.text,
                children: this.getChildrenIds(node.id),
            };
        });
    }

    private findNodeById(id: string | number): TreeNode | undefined {
        return this.config.core.data.find((node) => node.id === id);
    }

    private getChildrenIds(parentId: string | number): (string | number)[] {
        return this.config.core.data
            .filter((node) => node.parent === parentId)
            .map((node) => node.id);
    }
}

let treeInstance: TreeComponent | null = null;

/**
 * Initializes the role selection tree
 * @param {TreeConfig} config - The tree configuration object
 */
export const initRoles = (config: TreeConfig): void => {
    if (!config) return;

    const treeContainer = document.getElementById("jstree_div");
    if (!treeContainer) {
        console.warn("Tree container not found");
        return;
    }

    treeInstance = new TreeComponent(treeContainer, config);
};

/**
 * Gets the currently selected roles from the tree
 * This function:
 * 1. Retrieves all selected nodes from the tree
 * 2. Removes child roles when a parent role is selected (pruning)
 * 3. Returns an array of role names (text) after pruning
 *
 * @returns {string[]} Array of selected role names after pruning parent/child relationships
 */
export const getSelectedRoles = (): string[] => {
    if (!treeInstance) {
        console.warn("Roles tree not initialized");
        return [];
    }

    const selectedRoles = treeInstance.get_selected(
        true,
    ) as SelectedNodeDetails[];
    console.log("Selected roles:", selectedRoles);

    // Identify child roles that need to be removed when their parent is selected
    const keysToRemove = selectedRoles.reduce<(string | number)[]>(
        (acc, roleEntry) => {
            if (!isNumeric(roleEntry.id)) {
                acc.push(...roleEntry.children);
            }
            return acc;
        },
        [],
    );

    console.log("keysToRemove:", keysToRemove);

    // Create final list of roles, excluding children of selected parents
    const prunedRoles = selectedRoles
        .filter((roleEntry) => !keysToRemove.includes(roleEntry.id))
        .map((roleEntry) => roleEntry.text);

    console.log("Pruned roles:", prunedRoles);
    return prunedRoles;
};
