/**
 * Role management functionality for Discograph
 * This file handles the initialization and management of role selection using vanilla JavaScript
 */

/**
 * Utility function to check if a value is numeric
 * @param {*} obj - The value to check
 * @returns {boolean} - True if the value is numeric, false otherwise
 */
const isNumeric = (obj) => {
    return !Array.isArray(obj) && (obj - parseFloat(obj) + 1) >= 0;
};

class TreeComponent {
    constructor(container, config) {
        this.container = container;
        this.config = config;
        this.selectedNodes = new Set();
        this.init();
    }

    init() {
        // Clear existing content
        this.container.innerHTML = '';
        
        // Create tree structure
        const treeRoot = document.createElement('ul');
        treeRoot.className = 'tree-root';
        
        // Add nodes from config
        if (this.config?.core?.data) {
            this.buildTreeNodes(treeRoot, this.config.core.data);
        }
        
        this.container.appendChild(treeRoot);
        
        // Add styles if not already present
        if (!document.getElementById('tree-styles')) {
            const styles = document.createElement('style');
            styles.id = 'tree-styles';
            styles.textContent = `
                .tree-root {
                    list-style: none;
                    padding-left: 20px;
                }
                .tree-node {
                    margin: 5px 0;
                }
                .tree-content {
                    display: flex;
                    align-items: center;
                    gap: 5px;
                }
                .tree-checkbox {
                    margin: 0;
                }
                .tree-icon {
                    width: 16px;
                    height: 16px;
                }
                .tree-children {
                    list-style: none;
                    padding-left: 20px;
                }
            `;
            document.head.appendChild(styles);
        }
    }

    buildTreeNodes(parentElement, nodes) {
        nodes.forEach(node => {
            const li = document.createElement('li');
            li.className = 'tree-node';
            li.dataset.id = node.id;

            const content = document.createElement('div');
            content.className = 'tree-content';

            // Add checkbox if enabled in config
            if (this.config.plugins?.includes('checkbox')) {
                const checkbox = document.createElement('input');
                checkbox.type = 'checkbox';
                checkbox.className = 'tree-checkbox';
                checkbox.checked = node.state?.selected || false;
                checkbox.addEventListener('change', () => this.handleNodeSelection(node.id, checkbox.checked));
                content.appendChild(checkbox);
            }

            // Add icon if present
            if (node.icon) {
                const icon = document.createElement('span');
                icon.className = 'tree-icon';
                icon.textContent = node.icon;
                content.appendChild(icon);
            }

            // Add text
            const text = document.createElement('span');
            text.className = 'tree-text';
            text.textContent = node.text;
            content.appendChild(text);

            li.appendChild(content);

            // Add child nodes if any exist
            const childNodes = nodes.filter(n => n.parent === node.id);
            if (childNodes.length > 0) {
                const childrenContainer = document.createElement('ul');
                childrenContainer.className = 'tree-children';
                this.buildTreeNodes(childrenContainer, childNodes);
                li.appendChild(childrenContainer);
            }

            parentElement.appendChild(li);
        });
    }

    handleNodeSelection(nodeId, selected) {
        if (selected) {
            this.selectedNodes.add(nodeId);
        } else {
            this.selectedNodes.delete(nodeId);
        }
    }

    get_selected(withDetails = false) {
        if (!withDetails) {
            return Array.from(this.selectedNodes);
        }

        return Array.from(this.selectedNodes).map(id => {
            const node = this.findNodeById(id);
            return {
                id: node.id,
                text: node.text,
                children: this.getChildrenIds(node.id)
            };
        });
    }

    findNodeById(id) {
        return this.config.core.data.find(node => node.id === id);
    }

    getChildrenIds(parentId) {
        return this.config.core.data
            .filter(node => node.parent === parentId)
            .map(node => node.id);
    }
}

let treeInstance = null;

/**
 * Initializes the role selection tree
 * @param {Object} config - The tree configuration object
 */
export const initRoles = (config) => {
    if (!config) return;
    
    const treeContainer = document.getElementById('jstree_div');
    if (!treeContainer) {
        console.warn('Tree container not found');
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
export const getSelectedRoles = () => {
    if (!treeInstance) {
        console.warn("Roles tree not initialized");
        return [];
    }

    const selectedRoles = treeInstance.get_selected(true);
    console.log("Selected roles:", selectedRoles);

    // Identify child roles that need to be removed when their parent is selected
    const keysToRemove = selectedRoles.reduce((acc, roleEntry) => {
        if (!isNumeric(roleEntry.id)) {
            acc.push(...roleEntry.children);
        }
        return acc;
    }, []);
    
    console.log("keysToRemove:", keysToRemove);

    // Create final list of roles, excluding children of selected parents
    const prunedRoles = selectedRoles
        .filter(roleEntry => !keysToRemove.includes(roleEntry.id))
        .map(roleEntry => roleEntry.text);

    console.log("Pruned roles:", prunedRoles);
    return prunedRoles;
};