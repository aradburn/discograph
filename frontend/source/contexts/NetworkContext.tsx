/** @jsxImportSource react */
import React, { createContext, useContext, useReducer, useEffect } from "react";
import type { ReactNode } from "react";
import { networkManager, discographManager } from "../core";
import type { SimNode, SimLink } from "../network/data";
import { FORCE } from "../constants";
import * as d3 from "d3";

// Define the state interface
interface NetworkState {
    isSimulationRunning: boolean;
    nodeStrength: number;
    linkStrength: number;
    gravityStrength: number;
    selectedNode: string | null;
    isInitialized: boolean;
}

// Define the actions that can be dispatched
type NetworkAction =
    | { type: "START_SIMULATION" }
    | { type: "STOP_SIMULATION" }
    | { type: "SET_NODE_STRENGTH"; value: number }
    | { type: "SET_LINK_STRENGTH"; value: number }
    | { type: "SET_GRAVITY_STRENGTH"; value: number }
    | { type: "SELECT_NODE"; nodeId: string | null }
    | { type: "SET_INITIALIZED"; value: boolean };

// Context interface
interface NetworkContextProps {
    state: NetworkState;
    dispatch: React.Dispatch<NetworkAction>;
    setupChargeForce: (nodeStrength: number) => void;
    setupLinkForce: (linkStrength: number) => void;
    setupGravityForce: (gravityStrength: number) => void;
    restartForceLayout: (alpha: number) => void;
}

// Initial state
const initialState: NetworkState = {
    isSimulationRunning: false,
    nodeStrength: 30,
    linkStrength: 30,
    gravityStrength: 30,
    selectedNode: null,
    isInitialized: false,
};

// Create the context
const NetworkContext = createContext<NetworkContextProps | undefined>(
    undefined,
);

// Reducer function
function networkReducer(
    state: NetworkState,
    action: NetworkAction,
): NetworkState {
    switch (action.type) {
        case "START_SIMULATION":
            return { ...state, isSimulationRunning: true };
        case "STOP_SIMULATION":
            return { ...state, isSimulationRunning: false };
        case "SET_NODE_STRENGTH":
            return { ...state, nodeStrength: action.value };
        case "SET_LINK_STRENGTH":
            return { ...state, linkStrength: action.value };
        case "SET_GRAVITY_STRENGTH":
            return { ...state, gravityStrength: action.value };
        case "SELECT_NODE":
            return { ...state, selectedNode: action.nodeId };
        case "SET_INITIALIZED":
            return { ...state, isInitialized: action.value };
        default:
            return state;
    }
}

// Provider component
interface NetworkProviderProps {
    children: ReactNode;
}

export const NetworkProvider: React.FC<NetworkProviderProps> = ({
    children,
}) => {
    const [state, dispatch] = useReducer(networkReducer, initialState);

    // Synchronize React state with NetworkManager when needed
    useEffect(() => {
        if (state.isSimulationRunning !== networkManager.isRunningLayout) {
            networkManager.isRunningLayout = state.isSimulationRunning;

            if (state.isSimulationRunning && networkManager.forceLayout) {
                networkManager.forceLayout
                    .alpha(FORCE.SIMULATION.ALPHA)
                    .alphaDecay(FORCE.SIMULATION.ALPHA_DECAY)
                    .restart();
            } else if (networkManager.forceLayout) {
                networkManager.forceLayout.stop();
            }
        }
    }, [state.isSimulationRunning]);

    // Helper functions for force layout manipulation
    const setupChargeForce = (nodeStrength: number): void => {
        if (!networkManager.forceLayout) return;

        const nodeStrengthMultiplier =
            nodeStrength / FORCE.MULTIPLIER.NODE_STRENGTH_SCALE +
            FORCE.MULTIPLIER.NODE_STRENGTH_BASE;

        networkManager.forceLayout.force(
            "charge",
            d3
                .forceManyBody<SimNode>()
                .strength(calculateNodeStrength)
                .distanceMax(FORCE.DISTANCE.MAX)
                .theta(FORCE.SIMULATION.THETA),
        );

        // Helper function for node strength calculation
        function calculateNodeStrength(d: SimNode): number {
            const baseStrength = d.isIntermediate
                ? FORCE.NODE.STRENGTH_INTERMEDIATE
                : d.cluster
                  ? FORCE.NODE.STRENGTH_CLUSTER
                  : FORCE.NODE.STRENGTH;

            return baseStrength * nodeStrengthMultiplier;
        }
    };

    const setupLinkForce = (linkStrength: number): void => {
        if (!networkManager.forceLayout) return;

        const linkStrengthMultiplier =
            linkStrength / FORCE.MULTIPLIER.LINK_STRENGTH_SCALE;

        networkManager.forceLayout.force(
            "link",
            d3
                .forceLink<SimNode, SimLink>()
                .id((d) => d.key || "")
                .links(Array.from(networkManager.data.linkMap.values()))
                .distance(calculateLinkDistance)
                .iterations(FORCE.LINK.ITERATIONS),
        );

        // Helper function for link distance calculation
        function calculateLinkDistance(d: SimLink): number {
            let distance = FORCE.DISTANCE.LINK;

            if (d.role === FORCE.LINK.ROLES.ALIAS) {
                distance = FORCE.DISTANCE.LINK_ALIAS;
            } else if (d.role === FORCE.LINK.ROLES.RELEASED_ON) {
                distance = FORCE.DISTANCE.LINK_RELEASED_ON;
            } else if (d.isSpline) {
                distance =
                    d.distance < 1
                        ? FORCE.DISTANCE.LINK / 5
                        : FORCE.DISTANCE.LINK / 10;
            }

            return distance * linkStrengthMultiplier;
        }
    };

    const setupGravityForce = (gravityStrength: number): void => {
        if (!networkManager.forceLayout) return;

        const gravStrengthMultiplier =
            gravityStrength / FORCE.MULTIPLIER.GRAVITY_STRENGTH_SCALE;

        networkManager.forceLayout
            .force(
                "x",
                d3
                    .forceX<SimNode>(discographManager.svgDimensions[0] / 2)
                    .strength(calculateGravityStrength),
            )
            .force(
                "y",
                d3
                    .forceY<SimNode>(discographManager.svgDimensions[1] / 2)
                    .strength(calculateGravityStrength),
            );

        // Helper function for gravity strength calculation
        function calculateGravityStrength(_d: SimNode): number {
            return gravStrengthMultiplier * 0.02;
        }
    };

    const restartForceLayout = (alpha: number): void => {
        if (networkManager.forceLayout && state.isSimulationRunning) {
            networkManager.forceLayout.alpha(alpha).restart();
        }
    };

    return (
        <NetworkContext.Provider
            value={{
                state,
                dispatch,
                setupChargeForce,
                setupLinkForce,
                setupGravityForce,
                restartForceLayout,
            }}
        >
            {children}
        </NetworkContext.Provider>
    );
};

// Custom hook to use the network context
export const useNetwork = (): NetworkContextProps => {
    const context = useContext(NetworkContext);
    if (context === undefined) {
        throw new Error("useNetwork must be used within a NetworkProvider");
    }
    return context;
};
