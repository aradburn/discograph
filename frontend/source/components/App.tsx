/** @jsxImportSource react */
import React, { useState, useEffect } from "react";
import { Container, Row, Col, Button } from "react-bootstrap";
import "bootstrap/dist/css/bootstrap.min.css";

import { Header } from "./Layout/Header.tsx";
import { Sidebar } from "./Layout/Sidebar";
import { HelpModal, WelcomeModal, WhoModal } from "./Modals";
import { NetworkView } from "./Visualization/NetworkView";
import { NetworkProvider } from "../contexts/NetworkContext";

/**
 * Main App component that serves as the container for the React application.
 * During migration, this will gradually replace the existing jQuery-based UI.
 */
const App: React.FC = (): React.ReactElement => {
    const [showHelpModal, setShowHelpModal] = useState<boolean>(false);
    const [showWelcomeModal, setShowWelcomeModal] = useState<boolean>(false);
    const [showWhoModal, setShowWhoModal] = useState<boolean>(false);
    const [isReturnVisitor, setIsReturnVisitor] = useState<boolean>(false);

    // Check if this is a return visitor
    useEffect(() => {
        const hasVisitedBefore = localStorage.getItem("hasVisitedBefore");
        if (!hasVisitedBefore) {
            // First time visitor - show welcome modal
            setShowWelcomeModal(true);
            localStorage.setItem("hasVisitedBefore", "true");
        } else {
            setIsReturnVisitor(true);
        }
    }, []);

    const handleShowHelp = (): void => {
        setShowHelpModal(true);
    };

    const handleHideHelp = (): void => {
        setShowHelpModal(false);
    };

    const handleShowWelcome = (): void => {
        setShowWelcomeModal(true);
    };

    const handleHideWelcome = (): void => {
        setShowWelcomeModal(false);
    };

    const handleShowWho = (): void => {
        setShowWhoModal(true);
    };

    const handleHideWho = (): void => {
        setShowWhoModal(false);
    };

    return (
        <NetworkProvider>
            <Container fluid className="h-100 d-flex flex-column">
                <Row>
                    <Header
                        onShowHelp={handleShowHelp}
                        onShowWho={handleShowWho}
                    />
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


                            </div>
                            <div
                                className="flex-grow-1"
                                style={{ overflow: "hidden" }}
                            >
                                <NetworkView
                                    height="100%"
                                    showControls={true}
                                />
                            </div>
                        </div>
                    </Col>
                </Row>

                <HelpModal show={showHelpModal} onHide={handleHideHelp} />
                <WhoModal show={showWhoModal} onHide={handleHideWho} />
                <WelcomeModal show={showWelcomeModal}
                                  onHide={handleHideWelcome}
                                    isReturnVisitor={isReturnVisitor}
                                />
            </Container>
        </NetworkProvider>
    );
};

export default App;
