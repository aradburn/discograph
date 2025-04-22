import { Tooltip } from "bootstrap";
import { loading } from "./loading";
import { initRelations } from "./relations";
import { initRoles } from "./roles";
import { discographManager } from "./core";
import type { TreeConfig } from "./roles";
import { initFSM } from "./fsm/index";
import { DOM_IDS, INIT } from "./constants";

declare global {
    interface Window {
        dgRoles: TreeConfig;
    }
}

/**
 * Initializes window dimensions and event handlers
 */
// export const initWindow = (): void => {
//     // Handle window resize events
//     const handleResize = debounce(() => {
//         try {
//             console.log("handleResize()");
//             initWindow();
//             initSvg();
//             resetNetworkTransform();
//             window.dispatchEvent(new ResizeEvent());
//         } catch (error: unknown) {
//             const errorMessage =
//                 error instanceof Error
//                     ? error.message
//                     : typeof error === "string"
//                       ? error
//                       : "Unknown error";
//             console.error("Error during window resize:", errorMessage);
//             showMessage("Error during window resize: " + errorMessage, "error");
//             clearMessages(INIT.MESSAGE_CLEAR_DELAY);
//         }
//     }, INIT.DEBOUNCE_DELAY);
//
//     window.addEventListener("resize", handleResize);
// };

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
        //         initWindow();
        //         initSvg();
        //         initNetwork(DOM_IDS.SVG_ID);
        initRelations();
        if (window.dgRoles) {
            initRoles(window.dgRoles);
        }

        const svgDimensions: [number, number] = [
            discographManager.svgDimensions[0],
            discographManager.svgDimensions[1],
        ];
        loading.init(svgDimensions);

        // Layout control button handlers and Print button handlers have been migrated to React components
        // and are no longer needed here

        // Tooltip initialization has been migrated to React Bootstrap components
        // Bootstrap 5 tooltips for any remaining non-React elements
        const tooltipTriggerList = document.querySelectorAll<HTMLElement>(
            '[data-bs-toggle="tooltip"]',
        );
        if (tooltipTriggerList.length > 0) {
            [...tooltipTriggerList].map(
                (tooltipTriggerEl) =>
                    new Tooltip(tooltipTriggerEl, {
                        trigger: INIT.TOOLTIP_TRIGGER,
                    }),
            );
        }

        // Initialize the Discograph Finite State Machine
        initFSM();

        // Modals start off hidden to prevent them showing on startup before CSS gets loaded.
        const navTop = document.querySelector<HTMLDivElement>(
            `#${DOM_IDS.NAV_TOP}`,
        );
        if (navTop) {
            navTop.style.opacity = "1";
        }
        const modalHelp = document.querySelector<HTMLDivElement>(
            `#${DOM_IDS.MODAL_HELP}`,
        );
        if (modalHelp) {
            modalHelp.style.opacity = "1";
        }
        const sideMenuContent = document.querySelector<HTMLDivElement>(
            `#${DOM_IDS.SIDE_MENU_CONTENT}`,
        );
        if (sideMenuContent) {
            sideMenuContent.style.opacity = "1";
        }

        console.log("discograph initialized.");
    };

    // Start the container check process
    checkForContainer();
};
