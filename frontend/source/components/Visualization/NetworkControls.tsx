/** @jsxImportSource react */
import React, { useCallback, memo } from "react";
import { Form, Button } from "react-bootstrap";
import { FORCE } from "../../constants";
import { useNetwork } from "../../contexts/NetworkContext";

interface NetworkControlsProps {
    className?: string;
}

/**
 * NetworkControls component that provides controls for the D3.js force layout visualization.
 * This component uses the NetworkContext to interact with the D3.js force layout.
 */
const NetworkControls: React.FC<NetworkControlsProps> = ({
    className = "",
}) => {
    const {
        state,
        dispatch,
        setupChargeForce,
        setupLinkForce,
        setupGravityForce,
        restartForceLayout,
    } = useNetwork();

    // Memoize event handlers to prevent unnecessary re-renders
    const handleNodeStrengthChange = useCallback(
        (e: React.ChangeEvent<HTMLInputElement>): void => {
            const value = parseInt(e.target.value, 10);
            dispatch({ type: "SET_NODE_STRENGTH", value });

            // Update the node strength in the force layout
            setupChargeForce(value);
            // Restart the force layout with a reduced alpha
            restartForceLayout(FORCE.SIMULATION.ALPHA / 10.0);
        },
        [dispatch, setupChargeForce, restartForceLayout],
    );

    const handleLinkStrengthChange = useCallback(
        (e: React.ChangeEvent<HTMLInputElement>): void => {
            const value = parseInt(e.target.value, 10);
            dispatch({ type: "SET_LINK_STRENGTH", value });

            // Update the link strength in the force layout
            setupLinkForce(value);
            // Restart the force layout with a reduced alpha
            restartForceLayout(FORCE.SIMULATION.ALPHA / 5.0);
        },
        [dispatch, setupLinkForce, restartForceLayout],
    );

    const handleGravityStrengthChange = useCallback(
        (e: React.ChangeEvent<HTMLInputElement>): void => {
            const value = parseInt(e.target.value, 10);
            dispatch({ type: "SET_GRAVITY_STRENGTH", value });

            // Update the gravity strength in the force layout
            setupGravityForce(value);
            // Restart the force layout with a reduced alpha
            restartForceLayout(FORCE.SIMULATION.ALPHA / 10.0);
        },
        [dispatch, setupGravityForce, restartForceLayout],
    );

    const handleStartLayout = useCallback((): void => {
        if (!state.isSimulationRunning) {
            dispatch({ type: "START_SIMULATION" });
        }
    }, [state.isSimulationRunning, dispatch]);

    const handleStopLayout = useCallback((): void => {
        if (state.isSimulationRunning) {
            dispatch({ type: "STOP_SIMULATION" });
        }
    }, [state.isSimulationRunning, dispatch]);

    return (
        <div className={`network-controls ${className}`}>
            {/* Node strength slider */}
            <Form.Label htmlFor="nodeRangeReact">Node Strength</Form.Label>
            <Form.Range
                id="nodeRangeReact"
                value={state.nodeStrength}
                onChange={handleNodeStrengthChange}
                min={0}
                max={100}
            />

            {/* Link strength slider */}
            <Form.Label htmlFor="linkRangeReact">Link Strength</Form.Label>
            <Form.Range
                id="linkRangeReact"
                value={state.linkStrength}
                onChange={handleLinkStrengthChange}
                min={0}
                max={100}
            />

            {/* Gravity strength slider */}
            <Form.Label htmlFor="gravRangeReact">Gravity Strength</Form.Label>
            <Form.Range
                id="gravRangeReact"
                value={state.gravityStrength}
                onChange={handleGravityStrengthChange}
                min={0}
                max={100}
            />

            <div className="d-flex justify-content-between mt-3">
                <Button
                    variant="primary"
                    onClick={handleStartLayout}
                    disabled={state.isSimulationRunning}
                >
                    <i className="bi bi-lightning"></i>{" "}
                    <span className="d-none d-sm-inline">Start Layout</span>
                </Button>
                <Button
                    variant="secondary"
                    onClick={handleStopLayout}
                    disabled={!state.isSimulationRunning}
                >
                    <i className="bi bi-sign-stop"></i>{" "}
                    <span className="d-none d-sm-inline">Stop Layout</span>
                </Button>
            </div>
        </div>
    );
};

// Wrap with memo to prevent unnecessary re-renders
export default memo(NetworkControls);

// Also export the non-memoized version for cases where that might be needed
export { NetworkControls };
