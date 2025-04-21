/** @jsxImportSource react */
import React, { useState } from "react";
import { Modal, Button } from "react-bootstrap";

interface HelpModalProps {
    show?: boolean;
    onHide?: () => void;
}

/**
 * Help modal component that displays application usage instructions.
 * This is a basic implementation that will be expanded during migration.
 */
export const HelpModal: React.FC<HelpModalProps> = ({
    show = false,
    onHide = (): void => {
        return;
    },
}): React.ReactElement => {
    const [isVisible, setIsVisible] = useState(show);

    const handleClose = (): void => {
        setIsVisible(false);
        onHide();
    };

    return (
        <Modal show={isVisible} onHide={handleClose} size="lg">
            <Modal.Header closeButton>
                <Modal.Title>Help</Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <h5>Welcome to Discograph!</h5>
                <p>
                    Discograph provides an interactive visualization of the
                    relationships between artists, bands, and labels using
                    information derived from the Discogs.com database.
                </p>

                <h6>Basic Usage</h6>
                <ul>
                    <li>
                        Search for an artist, band, or label using the search
                        box
                    </li>
                    <li>
                        Click on nodes to expand the graph and see relationships
                    </li>
                    <li>Use the controls to adjust the layout and view</li>
                </ul>

                <h6>Navigation</h6>
                <ul>
                    <li>Click and drag to move the graph</li>
                    <li>Scroll to zoom in and out</li>
                    <li>Double-click a node to center on it</li>
                </ul>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="secondary" onClick={handleClose}>
                    Close
                </Button>
            </Modal.Footer>
        </Modal>
    );
};

export default HelpModal;
