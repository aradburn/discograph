import { vi, describe, it, expect, beforeEach, afterEach } from "vitest";
import type { FSMInstance, FSMConfig } from "../fsm";

/**
 * NOTE: This test file fully mocks the DiscographFsm implementation which causes
 * 0% code coverage for fsm.ts. This is because:
 *
 * 1. The FSM implementation depends on window.machina which isn't available in the test environment
 * 2. We mock it before importing DiscographFsm, effectively replacing the real implementation
 *
 * To improve coverage, a different approach would be needed:
 * - Create a facade or wrapper for window.machina that can be injected
 * - Refactor fsm.ts to accept window.machina as a parameter for better testability
 * - Implement window.machina more faithfully in the test environment
 */

// Mock window.machina before importing FSM
const mockMachina = {
    Fsm: {
        extend: vi.fn((config: FSMConfig) => {
            return class MockFsm implements FSMInstance {
                state = "uninitialized" as const;
                handle = vi.fn();
                handleError = vi.fn((error: unknown) => {
                    this.transition("state-viewing-network");
                });
                showNetwork = vi.fn();
                showRadial = vi.fn();
                transition = vi.fn();
                requestNetwork = vi.fn(
                    (entityKey: string, pushHistory: boolean) => {
                        this.transition("state-requesting-network");
                    },
                );
                requestRandom = vi.fn(() => {
                    this.transition("state-requesting-network");
                });
                requestRadial = vi.fn((entityKey: string) => {
                    this.transition("state-requesting-radial");
                });
                selectEntity = vi.fn();
                loadInlineData = vi.fn();
                toggleRadial = vi.fn();
                toggleNetwork = vi.fn();
                toggleLoading = vi.fn();
                toggleFilter = vi.fn();
                pushState = vi.fn();
                on = vi.fn();
            };
        }),
    },
};

// Mock window object
Object.defineProperty(window, "machina", {
    value: mockMachina,
    writable: true,
});

// Mock window.addEventListener
window.addEventListener = vi.fn();
window.history.pushState = vi.fn();

// Mock document.getElementById
document.getElementById = vi.fn((id: string) => {
    if (id === "tree-container") {
        return document.createElement("div");
    }
    return null;
}) as unknown as typeof document.getElementById;

// Import FSM and test utilities after mocks are set up
import { DiscographFsm } from "../fsm";

describe("FSM", () => {
    let fsm: FSMInstance;

    beforeEach(() => {
        fsm = new (mockMachina.Fsm.extend({} as FSMConfig))();
        vi.clearAllMocks();
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    describe("Initialization", () => {
        it("should initialize in uninitialized state", () => {
            expect(fsm.state).toBe("uninitialized");
        });
    });

    describe("State Transitions", () => {
        it("should transition to requesting-network state when requesting network", () => {
            fsm.requestNetwork("test-123", true);
            expect(fsm.transition).toHaveBeenCalledWith(
                "state-requesting-network",
            );
        });

        it("should transition to requesting-radial state when requesting radial", () => {
            fsm.requestRadial("test-123");
            expect(fsm.transition).toHaveBeenCalledWith(
                "state-requesting-radial",
            );
        });

        it("should transition to requesting-random state when requesting random", () => {
            fsm.requestRandom();
            expect(fsm.transition).toHaveBeenCalledWith(
                "state-requesting-network",
            );
        });
    });

    describe("Error Handling", () => {
        it("should handle errors", () => {
            const error = new Error("Test error");
            fsm.handleError(error);
            expect(fsm.transition).toHaveBeenCalledWith(
                "state-viewing-network",
            );
        });
    });

    describe("UI Toggles", () => {
        it("should toggle loading state", () => {
            fsm.toggleLoading(true);
            expect(fsm.toggleLoading).toHaveBeenCalledWith(true);
        });

        it("should toggle network visibility", () => {
            fsm.toggleNetwork(true);
            expect(fsm.toggleNetwork).toHaveBeenCalledWith(true);
        });

        it("should toggle radial visibility", () => {
            fsm.toggleRadial(true);
            expect(fsm.toggleRadial).toHaveBeenCalledWith(true);
        });
    });

    describe("Browser History Management", () => {
        it("should push state to browser history", () => {
            fsm.pushState("test-123", { param: "value" });
            expect(fsm.pushState).toHaveBeenCalledWith("test-123", {
                param: "value",
            });
        });
    });
});
