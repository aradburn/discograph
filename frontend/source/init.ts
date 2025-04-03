import { Tooltip } from "bootstrap";
import { loading } from "./loading";
import { initRelations } from "./relations";
import { initNetwork } from "./network/init";
import { restartForceLayout, stopForceLayout } from "./network/forceLayout";
import { initRoles } from "./roles";
import { initSvg, printSvg } from "./svg";
import { initTypeahead } from "./typeahead";
import { dg } from "./dg";
import { DiscographFsm } from "./fsm";
import type { TreeConfig } from "./roles";
import { debounce } from "./utils";
import { showMessage, clearMessages } from "./messages";

declare global {
    interface Window {
        dgRoles: TreeConfig;
    }
}

// Constants for viewport and SVG scaling
export const VIEWPORT_SIZE_MULTIPLIER = 3.0;
export const SVG_SCALING_MULTIPLIER = 0.8;

/**
 * Initializes window dimensions and event handlers
 */
export const initWindow = (): void => {
    const dpr = window.devicePixelRatio || 1;
    console.log("window devicePixelRatio: ", dpr);

    const svgContainer = document.getElementById("svg-container-fluid");
    const width = svgContainer.clientWidth;
    const height = svgContainer.clientHeight;
    const svgContainerDimensions: [number, number] = [width, height];
    const svgCanvasDimensions: [number, number] = [
        svgContainerDimensions[0] *
            VIEWPORT_SIZE_MULTIPLIER *
            SVG_SCALING_MULTIPLIER *
            dpr,
        svgContainerDimensions[1] *
            VIEWPORT_SIZE_MULTIPLIER *
            SVG_SCALING_MULTIPLIER *
            dpr,
    ];
    console.log("svgContainerDimensions: ", svgContainerDimensions);
    console.log("svgCanvasDimensions: ", svgCanvasDimensions);

    dg.dpr = dpr;
    dg.dimensions = svgContainerDimensions;
    dg.svg_dimensions = svgCanvasDimensions;

    // All nodes start at center of the screen
    dg.network.newNodeCoords = [
        dg.svg_dimensions[0] / 2,
        dg.svg_dimensions[1] / 2,
    ];
    console.log("svg newNodeCoords: ", dg.network.newNodeCoords);

    // Handle window resize events
    const handleResize = debounce(() => {
        try {
            initWindow();
            initSvg();
        } catch (error: unknown) {
            const errorMessage =
                error instanceof Error
                    ? error.message
                    : typeof error === "string"
                      ? error
                      : "Unknown error";
            console.error("Error during window resize:", errorMessage);
            showMessage("Error during window resize: " + errorMessage, "error");
            clearMessages(5000); // Clear error message after 5 seconds
        }
    }, 250);

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
    initNetwork();
    initRelations();
    if (window.dgRoles) {
        initRoles(window.dgRoles);
    }
    initTypeahead();

    const svgDimensions: [number, number] = [
        dg.svg_dimensions[0],
        dg.svg_dimensions[1],
    ];
    loading.init(svgDimensions);

    // Random request button handler
    const requestRandomButton = document.querySelector("#request-random");
    const handleRandomRequest = (event: Event): void => {
        event.preventDefault();
        if (requestRandomButton) {
            requestRandomButton.dispatchEvent(
                new CustomEvent("discograph:request-random", { bubbles: true }),
            );
        }
    };
    if (requestRandomButton) {
        requestRandomButton.addEventListener("click", handleRandomRequest);
        requestRandomButton.addEventListener("touchstart", handleRandomRequest);
    }

    // Layout control button handlers
    const startLayoutButton = document.querySelector("#start-layout");
    const handleStartLayout = (event: Event): void => {
        event.preventDefault();
        restartForceLayout(0.1); // Default alpha value
    };
    if (startLayoutButton) {
        startLayoutButton.addEventListener("click", handleStartLayout);
        startLayoutButton.addEventListener("touchstart", handleStartLayout);
    }

    const stopLayoutButton = document.querySelector("#stop-layout");
    const handleStopLayout = (event: Event): void => {
        event.preventDefault();
        stopForceLayout();
    };
    if (stopLayoutButton) {
        stopLayoutButton.addEventListener("click", handleStopLayout);
        stopLayoutButton.addEventListener("touchstart", handleStopLayout);
    }

    // Print button handler
    const printButton = document.querySelector("#print");
    const handlePrint = (event: Event): void => {
        event.preventDefault();
        printSvg(dg.svg_dimensions[0], dg.svg_dimensions[1]);
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
                trigger: "hover",
            }),
    );

    // Initialize the Discograph Finite State Machine
    dg.fsm = new DiscographFsm();
    console.log("discograph initialized.");

    // Modals start off hidden to prevent them showing on startup before CSS gets loaded.
    const modalHelp = document.querySelector("#modal-help");
    modalHelp.style.opacity = "1";
};
