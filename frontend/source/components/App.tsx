/** @jsxImportSource react */
import React, { useState, useEffect } from "react";
import { Container, Row, Col } from "react-bootstrap";
import "bootstrap/dist/css/bootstrap.min.css";

import { Header } from "./Layout/Header.tsx";
import { Sidebar } from "./Layout/Sidebar";
import { HelpModal, WelcomeModal, WhoModal } from "./Modals";
import { NetworkView } from "./Visualization/NetworkView";
import { LoadingAnimation } from "./Visualization";
import { NetworkProvider } from "../contexts/NetworkContext";
import { WindowProvider } from "../contexts/WindowContext";
import { LoadingProvider } from "../contexts/LoadingContext";

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
        <WindowProvider>
            <NetworkProvider>
                <LoadingProvider>
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
                                <NetworkView />
                                <LoadingAnimation />
                            </Col>
                        </Row>

                        <HelpModal
                            show={showHelpModal}
                            onHide={handleHideHelp}
                        />
                        <WhoModal show={showWhoModal} onHide={handleHideWho} />
                        <WelcomeModal
                            show={showWelcomeModal}
                            onHide={handleHideWelcome}
                            isReturnVisitor={isReturnVisitor}
                        />
                    </Container>
                </LoadingProvider>
            </NetworkProvider>
        </WindowProvider>
    );
};

export default App;
