import { createContext, type Dispatch } from "react";

// Define the state interface
export interface NetworkState {
    nodeStrength: number;
    linkStrength: number;
    gravityStrength: number;
    selectedNode: string | null;
}

// Define the actions that can be dispatched
export type NetworkAction =
    | { type: "SET_NODE_STRENGTH"; value: number }
    | { type: "SET_LINK_STRENGTH"; value: number }
    | { type: "SET_GRAVITY_STRENGTH"; value: number }
    | { type: "SELECT_NODE"; nodeId: string | null }
    | { type: "SET_FORCES" }
    | { type: "RESET_FORCES" };

// Context interface
export interface NetworkContextProps {
    state: NetworkState;
    dispatch: Dispatch<NetworkAction>;
    setupChargeForce: (nodeStrength: number) => void;
    setupLinkForce: (linkStrength: number) => void;
    setupGravityForce: (gravityStrength: number) => void;
    setForces: () => void;
    resetForces: () => void;
}

// Create the context
export const NetworkContext = createContext<NetworkContextProps | undefined>(
    undefined,
);
