/** @jsxImportSource react */
import React from "react";
import { Form } from "react-bootstrap";
import { useNetwork } from "../../contexts/NetworkContext";
import { FORCE } from "../../constants";
import { printSvg } from "../../svg";
import { discographManager } from "../../core";

/**
 * Sidebar component that will replace the side navigation.
 * This component matches the structure of the original nav-side.html template.
 */
export const Sidebar: React.FC = () => {
    const {
        state,
        dispatch,
        setupChargeForce,
        setupLinkForce,
        setupGravityForce,
        restartForceLayout,
    } = useNetwork();

    const handleNodeStrengthChange = (
        e: React.ChangeEvent<HTMLInputElement>,
    ): void => {
        const value = parseInt(e.target.value, 10);
        dispatch({ type: "SET_NODE_STRENGTH", value });

        // Update the node strength in the force layout
        setupChargeForce(value);
        // Restart the force layout with a reduced alpha
        restartForceLayout(FORCE.SIMULATION.ALPHA / 10.0);
    };

    const handleLinkStrengthChange = (
        e: React.ChangeEvent<HTMLInputElement>,
    ): void => {
        const value = parseInt(e.target.value, 10);
        dispatch({ type: "SET_LINK_STRENGTH", value });

        // Update the link strength in the force layout
        setupLinkForce(value);
        // Restart the force layout with a reduced alpha
        restartForceLayout(FORCE.SIMULATION.ALPHA / 5.0);
    };

    const handleGravityStrengthChange = (
        e: React.ChangeEvent<HTMLInputElement>,
    ): void => {
        const value = parseInt(e.target.value, 10);
        dispatch({ type: "SET_GRAVITY_STRENGTH", value });

        // Update the gravity strength in the force layout
        setupGravityForce(value);
        // Restart the force layout with a reduced alpha
        restartForceLayout(FORCE.SIMULATION.ALPHA / 10.0);
    };

    const handleShowDetails = (): void => {
        console.log("Show entity details");
        // TODO: Implement details functionality
    };

    const handleShowRoles = (): void => {
        console.log("Show roles");
        // TODO: Implement roles functionality
    };

    const handlePrint = (): void => {
        printSvg(
            discographManager.svgDimensions[0],
            discographManager.svgDimensions[1],
        );
    };

    const handleStartLayout = (): void => {
        if (!state.isSimulationRunning) {
            dispatch({ type: "START_SIMULATION" });
        }
    };

    const handleStopLayout = (): void => {
        if (state.isSimulationRunning) {
            dispatch({ type: "STOP_SIMULATION" });
        }
    };

    return (
        <div className="sidebar d-flex h-100 flex-sm-column flex-xl-column justify-content-evenly justify-content-sm-start justify-content-xl-start align-items-start align-items-sm-start align-items-xl-start bg-secondary-subtle p-2 text-light">
            {/* Details button */}
            <div
                className="navbar-text px-sm-0 px-2"
                role="button"
                onClick={handleShowDetails}
            >
                <i className="fs-5 bi-eye"></i>
                <span className="ms-1 d-none d-sm-inline">DETAILS</span>
            </div>

            {/* Roles button */}
            <div
                className="navbar-text px-sm-0 px-2"
                role="button"
                onClick={handleShowRoles}
            >
                <i className="fs-5 bi-person"></i>
                <span className="ms-1 d-none d-sm-inline">ROLES</span>
            </div>

            {/* Print button */}
            <div
                className="navbar-text px-sm-0 px-2 mb-sm-auto"
                role="button"
                onClick={handlePrint}
            >
                <i className="fs-5 bi-printer"></i>
                <span className="ms-1 d-none d-sm-inline">PRINT</span>
            </div>

            {/* Node strength slider */}
            <Form.Label htmlFor="nodeRange">Node Strength</Form.Label>
            <Form.Range
                id="nodeRange"
                value={state.nodeStrength}
                onChange={handleNodeStrengthChange}
                min={0}
                max={100}
            />

            {/* Link strength slider */}
            <Form.Label htmlFor="linkRange">Link Strength</Form.Label>
            <Form.Range
                id="linkRange"
                value={state.linkStrength}
                onChange={handleLinkStrengthChange}
                min={0}
                max={100}
            />

            {/* Gravity strength slider */}
            <Form.Label htmlFor="gravRange">Gravity Strength</Form.Label>
            <Form.Range
                id="gravRange"
                value={state.gravityStrength}
                onChange={handleGravityStrengthChange}
                min={0}
                max={100}
            />

            {/* Start layout button */}
            <div
                className="navbar-text px-sm-0 px-2 justify-content-end"
                role="button"
                onClick={handleStartLayout}
                style={{ opacity: state.isSimulationRunning ? 0.5 : 1 }}
            >
                <i className="fs-5 bi-lightning"></i>
                <span className="ms-1 d-none d-sm-inline">LAYOUT</span>
            </div>

            {/* Stop layout button */}
            <div
                className="navbar-text px-sm-0 px-2 justify-content-end"
                role="button"
                onClick={handleStopLayout}
                style={{ opacity: state.isSimulationRunning ? 1 : 0.5 }}
            >
                <i className="fs-5 bi-sign-stop"></i>
                <span className="ms-1 d-none d-sm-inline">LAYOUT</span>
            </div>
        </div>
    );
};

export default Sidebar;
