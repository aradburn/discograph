import { initRelations } from "./relations";
import { initRoles } from "./roles";
import type { TreeConfig } from "./roles";
import { initFSM } from "./fsm/index";
import { DOM_IDS } from "./constants";
import { resetNetworkForces } from "./network/forceLayout";

declare global {
    interface Window {
        dgRoles: TreeConfig;
    }
}

/**
 * Initialize the application
 * Sets up event handlers for UI controls and initializes various components
 */
export const initApp = (): void => {
    // Check if React app is mounted and if SVG container exists
    const checkForContainer = (): void => {
        const svgContainer = document.getElementById(DOM_IDS.SVG_CONTAINER);

        if (!svgContainer) {
            console.log("SVG container not found yet, retrying in 100ms...");
            // Wait for the React components to render and try again
            setTimeout(checkForContainer, 100);
            return;
        }

        console.log("SVG container found, initializing application...");

        // Initialize all required components
        initRelations();
        if (window.dgRoles) {
            initRoles(window.dgRoles);
        }

        // const svgDimensions: [number, number] = [
        //     discographManager.svgDimensions[0],
        //     discographManager.svgDimensions[1],
        // ];
        // loading.init(svgDimensions);

        // Initialize the Discograph Finite State Machine
        initFSM();

        resetNetworkForces();

        console.log("discograph initialized.");
    };

    // Start the container check process
    checkForContainer();
};
