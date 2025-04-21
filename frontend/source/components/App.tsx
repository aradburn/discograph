/** @jsxImportSource react */
import React, { useState } from "react";
import { Container, Row, Col, Button } from "react-bootstrap";
import "bootstrap/dist/css/bootstrap.min.css";

import { Header } from "./Layout/Header.tsx";
import { Sidebar } from "./Layout/Sidebar";
import { HelpModal } from "./Modals/HelpModal";
import { NetworkView } from "./Visualization/NetworkView";

/**
 * Main App component that serves as the container for the React application.
 * During migration, this will gradually replace the existing jQuery-based UI.
 */
const App: React.FC = (): React.ReactElement => {
    const [showHelpModal, setShowHelpModal] = useState<boolean>(false);

    const handleShowHelp = (): void => {
        setShowHelpModal(true);
    };

    const handleHideHelp = (): void => {
        setShowHelpModal(false);
    };

    return (
        <Container fluid className="h-100 d-flex flex-column">
            <Row>
                <Header onShowHelp={handleShowHelp} />
            </Row>

            <Row className="flex-grow-1" style={{ minHeight: 0 }}>
                <Col xs={12} sm={2} xl={1} className="h-100 p-0">
                    <Sidebar />
                </Col>
                <Col
                    xs={12}
                    sm={10}
                    xl={11}
                    className="h-100 px-0 d-flex flex-column flex-grow-1 flex-shrink-1"
                >
                    <div className="h-100 d-flex flex-column">
                        <div className="p-3 bg-light">
                            <h4>React Implementation</h4>
                            <p>
                                This is a React-based UI that will gradually
                                replace the existing jQuery-based UI.
                            </p>
                            <Button variant="primary" onClick={handleShowHelp}>
                                Open Help Modal
                            </Button>
                        </div>
                        <div
                            className="flex-grow-1"
                            style={{ overflow: "hidden" }}
                        >
                            <NetworkView height="100%" />
                        </div>
                    </div>
                </Col>
            </Row>

            <HelpModal show={showHelpModal} onHide={handleHideHelp} />
        </Container>
    );
};

export default App;
