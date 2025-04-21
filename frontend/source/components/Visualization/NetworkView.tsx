/** @jsxImportSource react */
import React, { useRef, useEffect } from "react";
import { NetworkControls } from "./NetworkControls";
import { initNetwork } from "../../network/init";
import { networkManager } from "../../core";
import { useNetwork } from "../../contexts/NetworkContext";

interface NetworkViewProps {
    width?: string;
    height?: string;
    showControls?: boolean;
}

/**
 * NetworkView component that serves as a wrapper for the D3.js visualization.
 * This component initializes and integrates with the existing D3.js visualization.
 * It uses NetworkContext to manage the visualization state.
 */
export const NetworkView: React.FC<NetworkViewProps> = ({
    width = "100%",
    height = "100%",
    showControls = false,
}): React.ReactElement => {
    const svgRef = useRef<SVGSVGElement>(null);
    const networkContainerId = "react-network-container";
    const initializedRef = useRef<boolean>(false);
    const { dispatch } = useNetwork();

    useEffect(() => {
        // Initialize the D3.js visualization when the component mounts
        if (svgRef.current && !initializedRef.current) {
            console.log("Initializing D3.js network visualization");

            // Clear any existing network elements first
            if (networkManager.layers.root) {
                networkManager.layers.root.remove();
            }

            // Initialize the network with our SVG element
            initNetwork(`#${networkContainerId}`);

            // Mark the network as initialized
            initializedRef.current = true;
            dispatch({ type: "SET_INITIALIZED", value: true });
        }

        // Cleanup function to handle component unmount
        return (): void => {
            console.log("Cleaning up D3.js network visualization");
            // Stop the force simulation if it's running
            if (networkManager.forceLayout) {
                networkManager.forceLayout.stop();
                dispatch({ type: "STOP_SIMULATION" });
            }

            // Remove the visualization layers
            if (networkManager.layers.root) {
                networkManager.layers.root.remove();
            }

            // Mark the network as uninitialized
            initializedRef.current = false;
            dispatch({ type: "SET_INITIALIZED", value: false });
        };
    }, [dispatch]);

    return (
        <div style={{ width, height, position: "relative" }}>
            <svg
                ref={svgRef}
                id={networkContainerId}
                width="100%"
                height={showControls ? "calc(100% - 200px)" : "100%"}
                style={{
                    backgroundColor: "#f8f9fa",
                    border: "1px solid #dee2e6",
                    borderRadius: "4px",
                }}
            >
                {/* D3.js will populate this SVG with network visualization */}
            </svg>

            {showControls && (
                <div className="network-controls-container mt-3 p-3 border rounded">
                    <h5>Visualization Controls</h5>
                    <NetworkControls className="mt-3" />
                </div>
            )}
        </div>
    );
};

export default NetworkView;
