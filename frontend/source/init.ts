import { Tooltip } from "bootstrap";
import { loading } from "./loading";
import { initRelations } from "./relations";
import { initNetwork, resetNetworkTransform } from "./network/init";
import { restartForceLayout, stopForceLayout } from "./network/forceLayout";
import { initRoles } from "./roles";
import { initSvg, printSvg } from "./svg";
import { initTypeahead } from "./typeahead";
import { discographManager } from "./core";
import type { TreeConfig } from "./roles";
import { debounce } from "./utils";
import { showMessage, clearMessages } from "./messages";
import { ResizeEvent } from "./network/events";
import { initFSM } from "./fsm";
import { SVG, DOM_IDS, INIT, FSM } from "./constants";

declare global {
    interface Window {
        dgRoles: TreeConfig;
    }
}

/**
 * Initializes window dimensions and event handlers
 */
export const initWindow = (): void => {
    const dpr = window.devicePixelRatio || 1;
    console.log("window devicePixelRatio: ", dpr);

    const svgContainer = document.getElementById(DOM_IDS.SVG_CONTAINER);
    const width = svgContainer.clientWidth;
    const height = svgContainer.clientHeight;
    const svgContainerDimensions: [number, number] = [width, height];
    const svgCanvasDimensions: [number, number] = [
        svgContainerDimensions[0] * SVG.VIEWPORT_SIZE_MULTIPLIER * dpr,
        svgContainerDimensions[1] * SVG.VIEWPORT_SIZE_MULTIPLIER * dpr,
    ];
    console.log("svgContainerDimensions: ", svgContainerDimensions);
    console.log("svgCanvasDimensions: ", svgCanvasDimensions);

    discographManager.dpr = dpr;
    discographManager.dimensions = svgContainerDimensions;
    discographManager.svgDimensions = svgCanvasDimensions;

    // Handle window resize events
    const handleResize = debounce(() => {
        try {
            initWindow();
            initSvg();
            resetNetworkTransform();
            window.dispatchEvent(new ResizeEvent());
        } catch (error: unknown) {
            const errorMessage =
                error instanceof Error
                    ? error.message
                    : typeof error === "string"
                      ? error
                      : "Unknown error";
            console.error("Error during window resize:", errorMessage);
            showMessage("Error during window resize: " + errorMessage, "error");
            clearMessages(INIT.MESSAGE_CLEAR_DELAY);
        }
    }, INIT.DEBOUNCE_DELAY);

    window.addEventListener("resize", handleResize);
};

/**
 * Initialize the application
 * Sets up event handlers for UI controls and initializes various components
 */
export const initApp = (): void => {
    // Initialize all required components
    initWindow();
    initSvg();
    initNetwork(DOM_IDS.SVG_ID);
    initRelations();
    if (window.dgRoles) {
        initRoles(window.dgRoles);
    }
    initTypeahead();

    const svgDimensions: [number, number] = [
        discographManager.svgDimensions[0],
        discographManager.svgDimensions[1],
    ];
    loading.init(svgDimensions);

    // Random request button handler
    const requestRandomButton = document.querySelector(
        `#${DOM_IDS.REQUEST_RANDOM}`,
    );
    const handleRandomRequest = (event: Event): void => {
        event.preventDefault();
        if (requestRandomButton) {
            requestRandomButton.dispatchEvent(
                new CustomEvent(FSM.EVENTS.REQUEST_RANDOM, { bubbles: true }),
            );
        }
    };
    if (requestRandomButton) {
        requestRandomButton.addEventListener("click", handleRandomRequest);
        requestRandomButton.addEventListener("touchstart", handleRandomRequest);
    }

    // Layout control button handlers
    const startLayoutButton = document.querySelector(
        `#${DOM_IDS.START_LAYOUT}`,
    );
    const handleStartLayout = (event: Event): void => {
        event.preventDefault();
        restartForceLayout(INIT.DEFAULT_ALPHA);
    };
    if (startLayoutButton) {
        startLayoutButton.addEventListener("click", handleStartLayout);
        startLayoutButton.addEventListener("touchstart", handleStartLayout);
    }

    const stopLayoutButton = document.querySelector(`#${DOM_IDS.STOP_LAYOUT}`);
    const handleStopLayout = (event: Event): void => {
        event.preventDefault();
        stopForceLayout();
    };
    if (stopLayoutButton) {
        stopLayoutButton.addEventListener("click", handleStopLayout);
        stopLayoutButton.addEventListener("touchstart", handleStopLayout);
    }

    // Print button handler
    const printButton = document.querySelector(`#${DOM_IDS.PRINT}`);
    const handlePrint = (event: Event): void => {
        event.preventDefault();
        printSvg(
            discographManager.svgDimensions[0],
            discographManager.svgDimensions[1],
        );
    };
    if (printButton) {
        printButton.addEventListener("click", handlePrint);
        printButton.addEventListener("touchstart", handlePrint);
    }

    // Initialize tooltips using Bootstrap 5
    const tooltipTriggerList = document.querySelectorAll<HTMLElement>(
        '[data-bs-toggle="tooltip"]',
    );
    [...tooltipTriggerList].map(
        (tooltipTriggerEl) =>
            new Tooltip(tooltipTriggerEl, {
                trigger: INIT.TOOLTIP_TRIGGER,
            }),
    );

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
