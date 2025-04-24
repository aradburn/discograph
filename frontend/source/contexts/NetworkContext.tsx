/** @jsxImportSource react */
import React, {
    createContext,
    useContext,
    useReducer,
    useEffect,
    useCallback,
    useMemo,
    useRef,
} from "react";
import type { ReactNode } from "react";
import { networkManager, discographManager } from "../core";
import type { SimNode, SimLink } from "../network/data";
import { FORCE } from "../constants";
import * as d3 from "d3";
import { clamp } from "../utils";

// Define the state interface
interface NetworkState {
    nodeStrength: number;
    linkStrength: number;
    gravityStrength: number;
    selectedNode: string | null;
}

// Define the actions that can be dispatched
type NetworkAction =
    | { type: "SET_NODE_STRENGTH"; value: number }
    | { type: "SET_LINK_STRENGTH"; value: number }
    | { type: "SET_GRAVITY_STRENGTH"; value: number }
    | { type: "SELECT_NODE"; nodeId: string | null }
    | { type: "SET_FORCES" }
    | { type: "RESET_FORCES" };

// Context interface
interface NetworkContextProps {
    state: NetworkState;
    dispatch: React.Dispatch<NetworkAction>;
    setupChargeForce: (nodeStrength: number) => void;
    setupLinkForce: (linkStrength: number) => void;
    setupGravityForce: (gravityStrength: number) => void;
    setForces: () => void;
    resetForces: () => void;
}

// Initial state
const initialState: NetworkState = {
    nodeStrength: 12,
    linkStrength: 40,
    gravityStrength: 10,
    selectedNode: null,
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
        case "SET_NODE_STRENGTH":
            return { ...state, nodeStrength: action.value };
        case "SET_LINK_STRENGTH":
            return { ...state, linkStrength: action.value };
        case "SET_GRAVITY_STRENGTH":
            return { ...state, gravityStrength: action.value };
        case "SELECT_NODE":
            return { ...state, selectedNode: action.nodeId };
        case "SET_FORCES":
            return { ...state };
        case "RESET_FORCES":
            return { ...initialState };
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

    // Create a ref to always track the latest state
    const stateRef = useRef(state);

    // Update the ref whenever state changes
    useEffect(() => {
        stateRef.current = state;
    }, [state]);

    // Helper functions for force layout manipulation, memoized with useCallback
    const setupChargeForce = useCallback((nodeStrength: number): void => {
        if (!networkManager.forceLayout) {
            console.error("forceLayout not setup yet");
            return;
        }
        console.log("setupChargeForce:", nodeStrength);

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
    }, []);

    const setupLinkForce = useCallback((linkStrength: number): void => {
        if (!networkManager.forceLayout) return;
        console.log("setupLinkForce:", linkStrength);

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
    }, []);

    const setupGravityForce = useCallback((gravityStrength: number): void => {
        if (!networkManager.forceLayout) return;
        console.log("setupGravityForce:", gravityStrength);

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
        function calculateGravityStrength(d: SimNode): number {
            var dist = d.distance ? 4 - clamp(d.distance, 0, 3) : 1.0;
            var maxDimension = Math.max(
                discographManager.svgDimensions[0],
                discographManager.svgDimensions[1],
            );
            var scaling = dist / 10.0;
            var radialDistance =
                (maxDimension -
                    Math.max(
                        d.x - discographManager.svgDimensions[0] / 2,
                        d.y - discographManager.svgDimensions[1] / 2,
                    )) /
                maxDimension;
            return radialDistance * scaling * gravStrengthMultiplier;
        }
    }, []);

    // Initialize forces when component mounts or when the force layout changes
    useEffect(() => {
        // Check if forceLayout exists before trying to set up forces
        if (networkManager.forceLayout) {
            console.log("Initializing network forces from React context");

            // Set up initial force values from current state
            const currentState = stateRef.current;
            setupChargeForce(currentState.nodeStrength);
            setupLinkForce(currentState.linkStrength);
            setupGravityForce(currentState.gravityStrength);
        } else {
            console.error("Force layout not initialized yet in useEffect() #1");
        }
    }, [setupChargeForce, setupLinkForce, setupGravityForce]);

    // Setup event listener for reset forces event
    useEffect(() => {
        const handleSetForces = (): void => {
            console.log("Received set forces event");
            dispatch({ type: "SET_FORCES" });
            // Use the ref to get current state values
            const currentState = stateRef.current;
            setupChargeForce(currentState.nodeStrength);
            setupLinkForce(currentState.linkStrength);
            setupGravityForce(currentState.gravityStrength);
        };

        window.addEventListener("discograph:set-forces", handleSetForces);

        const handleResetForces = (): void => {
            console.log("Received reset forces event");
            dispatch({ type: "RESET_FORCES" });
            setupChargeForce(initialState.nodeStrength);
            setupLinkForce(initialState.linkStrength);
            setupGravityForce(initialState.gravityStrength);
        };

        window.addEventListener("discograph:reset-forces", handleResetForces);

        return (): void => {
            window.removeEventListener(
                "discograph:set-forces",
                handleSetForces,
            );
            window.removeEventListener(
                "discograph:reset-forces",
                handleResetForces,
            );
        };
    }, [setupChargeForce, setupLinkForce, setupGravityForce]);

    // Define setForces and resetForces as separate callbacks with proper dependencies
    const setForces = useCallback((): void => {
        dispatch({ type: "SET_FORCES" });
        // Use the ref to get current state values
        const currentState = stateRef.current;
        setupChargeForce(currentState.nodeStrength);
        setupLinkForce(currentState.linkStrength);
        setupGravityForce(currentState.gravityStrength);
    }, [dispatch, setupChargeForce, setupLinkForce, setupGravityForce]);

    const resetForces = useCallback((): void => {
        dispatch({ type: "RESET_FORCES" });
        setupChargeForce(initialState.nodeStrength);
        setupLinkForce(initialState.linkStrength);
        setupGravityForce(initialState.gravityStrength);
    }, [dispatch, setupChargeForce, setupLinkForce, setupGravityForce]);

    // Memoize the context value to prevent unnecessary re-renders of consumers
    const contextValue = useMemo(
        () => ({
            state,
            dispatch,
            setupChargeForce,
            setupLinkForce,
            setupGravityForce,
            setForces,
            resetForces,
        }),
        [
            state,
            dispatch,
            setupChargeForce,
            setupLinkForce,
            setupGravityForce,
            setForces,
            resetForces,
        ],
    );

    return (
        <NetworkContext.Provider value={contextValue}>
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
