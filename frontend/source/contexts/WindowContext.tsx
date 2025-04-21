/** @jsxImportSource react */
import React, { createContext, useContext, useState, useEffect } from "react";
import type { ReactNode } from "react";
import { debounce } from "../utils";
import { INIT, SVG } from "../constants";
import { discographManager } from "../core";
import { ResizeEvent } from "../network/events";
import { initSvg } from "../svg";
import { resetNetworkTransform } from "../network/init";

// Define the state interface
interface WindowState {
    width: number;
    height: number;
    dpr: number;
    dimensions: [number, number];
    svgDimensions: [number, number];
}

// Context interface
interface WindowContextProps {
    state: WindowState;
    handleResize: () => void;
}

// Initial state (with placeholder values that will be updated in useEffect)
const initialState: WindowState = {
    width: 0,
    height: 0,
    dpr: 1,
    dimensions: [0, 0],
    svgDimensions: [0, 0],
};

// Create the context
const WindowContext = createContext<WindowContextProps | undefined>(undefined);

// Provider component
interface WindowProviderProps {
    children: ReactNode;
}

export const WindowProvider: React.FC<WindowProviderProps> = ({ children }) => {
    const [state, setState] = useState<WindowState>(initialState);

    // Calculate window and SVG dimensions
    const calculateDimensions = (): WindowState => {
        const dpr = window.devicePixelRatio || 1;
        const svgContainer = document.getElementById("svg-container-fluid");

        if (!svgContainer) {
            return {
                ...state,
                dpr,
            };
        }

        const width = svgContainer.clientWidth;
        const height = svgContainer.clientHeight;
        const dimensions: [number, number] = [width, height];
        const svgDimensions: [number, number] = [
            dimensions[0] * SVG.VIEWPORT_SIZE_MULTIPLIER * dpr,
            dimensions[1] * SVG.VIEWPORT_SIZE_MULTIPLIER * dpr,
        ];

        // Update the discographManager with the new dimensions
        discographManager.dpr = dpr;
        discographManager.dimensions = dimensions;
        discographManager.svgDimensions = svgDimensions;

        return {
            width,
            height,
            dpr,
            dimensions,
            svgDimensions,
        };
    };

    // Handle window resize
    const handleResize = debounce((): void => {
        try {
            // Update dimensions state
            setState(calculateDimensions());

            // Reset SVG and network visualization
            initSvg();
            resetNetworkTransform();

            // Dispatch resize event
            window.dispatchEvent(new ResizeEvent());
        } catch (error) {
            console.error("Error during window resize:", error);
        }
    }, INIT.DEBOUNCE_DELAY);

    // Initialize dimensions on mount and set up resize listener
    useEffect(() => {
        // Calculate initial dimensions
        setState(calculateDimensions());

        // Add resize event listener
        window.addEventListener("resize", handleResize);

        // Clean up event listener on unmount
        return (): void => {
            window.removeEventListener("resize", handleResize);
        };
    }, []);

    return (
        <WindowContext.Provider value={{ state, handleResize }}>
            {children}
        </WindowContext.Provider>
    );
};

// Custom hook to use the window context
export const useWindow = (): WindowContextProps => {
    const context = useContext(WindowContext);
    if (context === undefined) {
        throw new Error("useWindow must be used within a WindowProvider");
    }
    return context;
};
