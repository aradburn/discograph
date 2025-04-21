/**
 * @fileoverview Main entry point for the Discograph application.
 * This module initializes the application and sets up module exports.
 * @module discograph
 * @version 0.2
 */

import "~bootstrap/dist/css/bootstrap.min.css";
import "jquery";
import jQuery from "jquery";

// Declare jQuery globals
declare global {
    interface Window {
        $: typeof jQuery;
        jQuery: typeof jQuery;
    }
}

// Inject jQuery into the global scope
Object.assign(window, { $: jQuery, jQuery });

// Import our custom CSS
import "./css/discograph.scss";

// Import all of Bootstrap's JS
// eslint-disable-next-line @typescript-eslint/no-unused-vars
import * as bootstrap from "bootstrap";

import { initApp } from "./init";

// Initialize the application when the DOM is loaded
document.addEventListener("DOMContentLoaded", (): void => {
    // Initialize the original app
    initApp();

    // Initialize React app using dynamic import
    import("./components/index.tsx")
        .then((module) => {
            if (typeof module.initReactApp === "function") {
                try {
                    module.initReactApp();
                    console.log("React app initialized successfully");
                } catch (error) {
                    console.error("Error initializing React app:", error);
                }
            } else {
                console.error(
                    "initReactApp function not found in module:",
                    module,
                );
            }
        })
        .catch((error) => {
            console.error("Failed to load React initialization module:", error);
        });

    // Add a toggle button for the React container (for development)
    const toggleButton = document.createElement("button");
    toggleButton.textContent = "Toggle React UI";
    toggleButton.style.position = "fixed";
    toggleButton.style.bottom = "10px";
    toggleButton.style.right = "10px";
    toggleButton.style.zIndex = "2000";
    toggleButton.onclick = (): void => {
        const container = document.getElementById("react-app-container");
        if (container) {
            container.style.display =
                container.style.display === "none" ? "block" : "none";
        }
    };
    document.body.appendChild(toggleButton);
});
