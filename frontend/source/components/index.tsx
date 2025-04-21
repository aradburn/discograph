/** @jsxImportSource react */
import React from "react";
import { createRoot } from "react-dom/client";
import App from "./App";

/**
 * Initializes the React application by mounting it to a specific DOM element.
 * This allows the React app to coexist with the existing jQuery-based UI during migration.
 */
export const initReactApp = (): void => {
    console.log("React app initialization function called");

    // Create a container for the React app if it doesn't exist
    let reactContainer = document.getElementById("react-app-container");

    if (!reactContainer) {
        reactContainer = document.createElement("div");
        reactContainer.id = "react-app-container";
        reactContainer.style.position = "absolute";
        reactContainer.style.top = "0";
        reactContainer.style.left = "0";
        reactContainer.style.width = "100%";
        reactContainer.style.height = "100%";
        reactContainer.style.zIndex = "1000";
        reactContainer.style.backgroundColor = "rgba(255, 255, 255, 0.9)";
        //         reactContainer.style.padding = "10px";
        //         reactContainer.style.borderRadius = "0 0 0 10px";
        //         reactContainer.style.boxShadow = "0 0 10px rgba(0, 0, 0, 0.2)";
        reactContainer.style.display = "none"; // Hidden initially, can be toggled for testing
        document.body.appendChild(reactContainer);
    }

    console.log("React container created");

    try {
        // Mount the React app
        const root = createRoot(reactContainer);
        root.render(
            <React.StrictMode>
                <App />
            </React.StrictMode>,
        );
        console.log("React app rendered successfully");
    } catch (error) {
        console.error("Error rendering React app:", error);
    }
};

// Also export as default just in case
export default { initReactApp };
