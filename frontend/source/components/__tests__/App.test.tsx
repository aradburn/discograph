// This file should be executed first to ensure mocks are defined before imports
import { vi } from "vitest";

// Mock all components before importing App
vi.mock("../Layout/Header.tsx", () => ({
    Header: vi.fn(() => null),
}));

vi.mock("../Layout/Sidebar", () => ({
    Sidebar: vi.fn(() => null),
}));

vi.mock("../Visualization/NetworkView", () => ({
    NetworkView: vi.fn(() => null),
}));

vi.mock("../Visualization", () => ({
    LoadingAnimation: vi.fn(() => null),
}));

vi.mock("../Modals", () => ({
    HelpModal: vi.fn(() => null),
    WelcomeModal: vi.fn(() => null),
    WhoModal: vi.fn(() => null),
}));

// Mock the context providers
vi.mock("../../contexts/NetworkContext", () => ({
    NetworkProvider: vi.fn(({ children }) => children),
}));

vi.mock("../../contexts/WindowContext", () => ({
    WindowProvider: vi.fn(({ children }) => children),
}));

vi.mock("../../contexts/LoadingContext", () => ({
    LoadingProvider: vi.fn(({ children }) => children),
}));

// Mock localStorage
vi.mock("../../utils", () => ({
    debounce: vi.fn((fn) => fn),
}));

// Now import the component and testing utilities
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { render, screen, act } from "@testing-library/react";
import "@testing-library/jest-dom";
import App from "../App";
import { Header } from "../Layout/Header.tsx";
import { Sidebar } from "../Layout/Sidebar";
import { NetworkView } from "../Visualization/NetworkView";
import { LoadingAnimation } from "../Visualization";
import { HelpModal, WelcomeModal, WhoModal } from "../Modals";

// Type the mocked components for TypeScript
type MockedComponent = ReturnType<typeof vi.fn> & {
    mock: { calls: any[][] };
};

// Cast mocked components to include mock property
const MockedHeader = Header as MockedComponent;
const MockedHelpModal = HelpModal as MockedComponent;
const MockedWhoModal = WhoModal as MockedComponent;
const MockedWelcomeModal = WelcomeModal as MockedComponent;

// Create localStorage mock
const localStorageMock = {
    getItem: vi.fn(),
    setItem: vi.fn(),
    clear: vi.fn(),
};
Object.defineProperty(window, "localStorage", { value: localStorageMock });

describe("App Component", () => {
    beforeEach(() => {
        vi.clearAllMocks();
        localStorageMock.getItem.mockReturnValue(null);
    });

    afterEach(() => {
        vi.resetAllMocks();
    });

    it("renders without crashing", () => {
        expect(() => render(<App />)).not.toThrow();
    });

    it("renders all required components", () => {
        render(<App />);

        // Check that all mocked components were called/rendered
        expect(Header).toHaveBeenCalled();
        expect(Sidebar).toHaveBeenCalled();
        expect(NetworkView).toHaveBeenCalled();
        expect(LoadingAnimation).toHaveBeenCalled();
        expect(HelpModal).toHaveBeenCalled();
        expect(WhoModal).toHaveBeenCalled();
        expect(WelcomeModal).toHaveBeenCalled();
    });

    it("checks localStorage for returning visitors", () => {
        render(<App />);
        expect(localStorageMock.getItem).toHaveBeenCalledWith(
            "hasVisitedBefore",
        );
    });

    it("sets localStorage for first-time visitors", () => {
        render(<App />);
        expect(localStorageMock.setItem).toHaveBeenCalledWith(
            "hasVisitedBefore",
            "true",
        );
    });

    it("doesn't show welcome modal for return visitors", () => {
        // Setup localStorage mock for return visitor
        localStorageMock.getItem.mockReturnValue("true");

        render(<App />);

        // Check last call to WelcomeModal
        const lastCallProps =
            MockedWelcomeModal.mock.calls[
                MockedWelcomeModal.mock.calls.length - 1
            ][0];
        expect(lastCallProps.show).toBe(false);
        expect(lastCallProps.isReturnVisitor).toBe(true);
    });

    it("shows welcome modal for first-time visitors", () => {
        // Setup localStorage mock for first-time visitor
        localStorageMock.getItem.mockReturnValue(null);

        render(<App />);

        // Check last call to WelcomeModal
        const lastCallProps =
            MockedWelcomeModal.mock.calls[
                MockedWelcomeModal.mock.calls.length - 1
            ][0];
        expect(lastCallProps.show).toBe(true);
        expect(lastCallProps.isReturnVisitor).toBe(false);
    });

    it("passes correct initial props to modals", () => {
        render(<App />);

        // Check props passed to HelpModal
        const helpModalProps =
            MockedHelpModal.mock.calls[
                MockedHelpModal.mock.calls.length - 1
            ][0];
        expect(helpModalProps.show).toBe(false);

        // Check props passed to WhoModal
        const whoModalProps =
            MockedWhoModal.mock.calls[MockedWhoModal.mock.calls.length - 1][0];
        expect(whoModalProps.show).toBe(false);

        // WelcomeModal is tested separately
    });

    it("passes the correct handlers to Header", () => {
        render(<App />);

        const headerProps =
            MockedHeader.mock.calls[MockedHeader.mock.calls.length - 1][0];
        expect(typeof headerProps.onShowHelp).toBe("function");
        expect(typeof headerProps.onShowWho).toBe("function");
    });

    describe("Modal handlers", () => {
        it("handleShowHelp sets showHelpModal to true", async () => {
            const { rerender } = render(<App />);

            // Extract the onShowHelp handler from Header props
            const { onShowHelp } = MockedHeader.mock.calls[0][0];

            // Check initial state
            let helpModalProps =
                MockedHelpModal.mock.calls[
                    MockedHelpModal.mock.calls.length - 1
                ][0];
            expect(helpModalProps.show).toBe(false);

            // Call the handler within act
            await act(async () => {
                onShowHelp();
            });

            // Re-render to update state
            rerender(<App />);

            // Check updated state
            helpModalProps =
                MockedHelpModal.mock.calls[
                    MockedHelpModal.mock.calls.length - 1
                ][0];
            expect(helpModalProps.show).toBe(true);
        });

        it("handleHideHelp sets showHelpModal to false", async () => {
            const { rerender } = render(<App />);

            // Extract the onShowHelp handler from Header props
            const { onShowHelp } = MockedHeader.mock.calls[0][0];

            // Call the handler to show the modal
            await act(async () => {
                onShowHelp();
            });
            rerender(<App />);

            // Verify HelpModal now has show=true
            let helpModalProps =
                MockedHelpModal.mock.calls[
                    MockedHelpModal.mock.calls.length - 1
                ][0];
            expect(helpModalProps.show).toBe(true);

            // Extract the onHide handler from HelpModal props
            const { onHide } = helpModalProps;

            // Call the handler to hide the modal
            await act(async () => {
                onHide();
            });
            rerender(<App />);

            // Verify HelpModal now has show=false
            helpModalProps =
                MockedHelpModal.mock.calls[
                    MockedHelpModal.mock.calls.length - 1
                ][0];
            expect(helpModalProps.show).toBe(false);
        });

        it("handleShowWho sets showWhoModal to true", async () => {
            const { rerender } = render(<App />);

            // Extract the onShowWho handler from Header props
            const { onShowWho } = MockedHeader.mock.calls[0][0];

            // Check initial state
            let whoModalProps =
                MockedWhoModal.mock.calls[
                    MockedWhoModal.mock.calls.length - 1
                ][0];
            expect(whoModalProps.show).toBe(false);

            // Call the handler
            await act(async () => {
                onShowWho();
            });

            // Re-render to update state
            rerender(<App />);

            // Check updated state
            whoModalProps =
                MockedWhoModal.mock.calls[
                    MockedWhoModal.mock.calls.length - 1
                ][0];
            expect(whoModalProps.show).toBe(true);
        });

        it("handleHideWho sets showWhoModal to false", async () => {
            const { rerender } = render(<App />);

            // Extract the onShowWho handler from Header props
            const { onShowWho } = MockedHeader.mock.calls[0][0];

            // Call the handler to show the modal
            await act(async () => {
                onShowWho();
            });
            rerender(<App />);

            // Verify WhoModal now has show=true
            let whoModalProps =
                MockedWhoModal.mock.calls[
                    MockedWhoModal.mock.calls.length - 1
                ][0];
            expect(whoModalProps.show).toBe(true);

            // Extract the onHide handler from WhoModal props
            const { onHide } = whoModalProps;

            // Call the handler to hide the modal
            await act(async () => {
                onHide();
            });
            rerender(<App />);

            // Verify WhoModal now has show=false
            whoModalProps =
                MockedWhoModal.mock.calls[
                    MockedWhoModal.mock.calls.length - 1
                ][0];
            expect(whoModalProps.show).toBe(false);
        });

        it("handleHideWelcome sets showWelcomeModal to false", async () => {
            const { rerender } = render(<App />);

            // Ensure welcome modal is shown for first-time visitor
            localStorageMock.getItem.mockReturnValue(null);
            rerender(<App />);

            // Verify WelcomeModal has show=true
            let welcomeModalProps =
                MockedWelcomeModal.mock.calls[
                    MockedWelcomeModal.mock.calls.length - 1
                ][0];
            expect(welcomeModalProps.show).toBe(true);

            // Extract the onHide handler from WelcomeModal props
            const { onHide } = welcomeModalProps;

            // Call the handler to hide the modal
            await act(async () => {
                onHide();
            });
            rerender(<App />);

            // Verify WelcomeModal now has show=false
            welcomeModalProps =
                MockedWelcomeModal.mock.calls[
                    MockedWelcomeModal.mock.calls.length - 1
                ][0];
            expect(welcomeModalProps.show).toBe(false);
        });
    });
});
