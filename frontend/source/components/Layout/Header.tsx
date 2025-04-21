/* @jsxImportSource react */

import React from "react";
import { Navbar, Container } from "react-bootstrap";
import { SearchInput } from "../Search";

interface HeaderProps {
    onShowHelp?: () => void;
    onShowWho?: () => void;
}

/**
 * Header component that will replace the top navigation.
 * This component matches the structure of the original nav-top.html template.
 */
export const Header: React.FC<HeaderProps> = ({ onShowHelp, onShowWho }) => {
    const handleRandom = (): void => {
        // Placeholder for random artist functionality
        console.log("Random artist requested");
        // TODO: Implement random artist functionality
    };

    return (
        <Navbar bg="body-tertiary" expand="lg" className="text-body p-0">
            <Container fluid>
                {/* Brand section */}
                <div className="px-2 py-0 col-lg-2 col-md-2 col-sm-1 col-1 order-1 order-md-1">
                    <div
                        className="d-flex flex-row navbar-brand px-0 py-0"
                        role="button"
                        onClick={onShowWho}
                    >
                        <span className="text-body px-2 py-0">
                            <i
                                className="bi bi-snow3"
                                style={{ fontSize: "2rem" }}
                            ></i>
                        </span>
                        <h3
                            className="text-body flex-grow-0 px-1 py-0 mb-0 collapse navbar-collapse"
                            title="Discograph"
                            data-bs-toggle="tooltip"
                            data-placement="bottom"
                        >
                            DISCOGRAPH
                        </h3>
                        <h6 className="text-body mb-0 collapse navbar-collapse">
                            &nbsp;v2 React
                        </h6>
                    </div>
                </div>

                {/* Navbar title section */}
                <div className="navbar-title flex-grow-1 d-flex justify-content-center px-2 py-0 h-100 d-inline-block col-lg-5 col-md-5 col-sm-10 col-10 order-2 order-md-2">
                    <span id="navbar-title"></span>
                </div>

                {/* Search section */}
                <div className="justify-content-center px-2 py-2 flex-grow-1 col-lg-3 col-md-3 col-sm-11 col-11 order-4 order-md-3">
                    <SearchInput
                        placeholder="Search for artists, labels, etc."
                        className="w-100"
                    />
                </div>

                {/* Random button section */}
                <div className="navbar-text navbar-right px-2 py-0 fs-5 d-flex justify-content-center col-lg-1 col-md-1 col-sm-1 col-1 order-3 order-md-4">
                    <div
                        className="d-flex flex-row"
                        title="Choose a random artist"
                        role="button"
                        onClick={handleRandom}
                    >
                        <i className="bi bi-shuffle px-1 py-0"></i>
                        <div className="collapse navbar-collapse px-1 py-0">
                            RANDOM
                        </div>
                    </div>
                </div>

                {/* Help button section */}
                <div className="navbar-text navbar-right px-2 py-0 fs-5 d-flex justify-content-center col-lg-1 col-md-1 col-sm-1 col-1 order-5 order-md-5">
                    <div
                        className="d-flex flex-row"
                        title="Help"
                        role="button"
                        onClick={onShowHelp}
                    >
                        <i className="bi bi-question-circle px-1 py-0"></i>
                        <div className="collapse navbar-collapse px-1 py-0">
                            HELP
                        </div>
                    </div>
                </div>
            </Container>
        </Navbar>
    );
};

export default Header;
