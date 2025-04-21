/** @jsxImportSource react */
import React, { useState } from "react";
import { Form } from "react-bootstrap";

/**
 * Sidebar component that will replace the side navigation.
 * This component matches the structure of the original nav-side.html template.
 */
export const Sidebar: React.FC = () => {
    const [nodeStrength, setNodeStrength] = useState<number>(30);
    const [linkStrength, setLinkStrength] = useState<number>(30);
    const [gravityStrength, setGravityStrength] = useState<number>(30);

    const handleNodeStrengthChange = (
        e: React.ChangeEvent<HTMLInputElement>,
    ): void => {
        setNodeStrength(parseInt(e.target.value, 10));
    };

    const handleLinkStrengthChange = (
        e: React.ChangeEvent<HTMLInputElement>,
    ): void => {
        setLinkStrength(parseInt(e.target.value, 10));
    };

    const handleGravityStrengthChange = (
        e: React.ChangeEvent<HTMLInputElement>,
    ): void => {
        setGravityStrength(parseInt(e.target.value, 10));
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
        console.log("Print requested");
        // TODO: Implement print functionality
    };

    const handleStartLayout = (): void => {
        console.log("Start layout");
        // TODO: Implement start layout functionality
    };

    const handleStopLayout = (): void => {
        console.log("Stop layout");
        // TODO: Implement stop layout functionality
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
                value={nodeStrength}
                onChange={handleNodeStrengthChange}
            />

            {/* Link strength slider */}
            <Form.Label htmlFor="linkRange">Link Strength</Form.Label>
            <Form.Range
                id="linkRange"
                value={linkStrength}
                onChange={handleLinkStrengthChange}
            />

            {/* Gravity strength slider */}
            <Form.Label htmlFor="gravRange">Gravity Strength</Form.Label>
            <Form.Range
                id="gravRange"
                value={gravityStrength}
                onChange={handleGravityStrengthChange}
            />

            {/* Start layout button */}
            <div
                className="navbar-text px-sm-0 px-2 justify-content-end"
                role="button"
                onClick={handleStartLayout}
            >
                <i className="fs-5 bi-lightning"></i>
                <span className="ms-1 d-none d-sm-inline">LAYOUT</span>
            </div>

            {/* Stop layout button */}
            <div
                className="navbar-text px-sm-0 px-2 justify-content-end"
                role="button"
                onClick={handleStopLayout}
            >
                <i className="fs-5 bi-sign-stop"></i>
                <span className="ms-1 d-none d-sm-inline">LAYOUT</span>
            </div>
        </div>
    );
};

export default Sidebar;
