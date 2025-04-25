import { describe, it, expect, vi, afterEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import "@testing-library/jest-dom";
import SearchInput from "../SearchInput";
import * as useSearchApiModule from "../hooks/useSearchApi";

// Mock the custom hook
vi.mock("../hooks/useSearchApi", () => ({
    default: vi.fn(),
}));

// Mock the RequestNetworkEvent
const mockDispatchEvent = vi.fn();
window.dispatchEvent = mockDispatchEvent;

// Reset mocks after each test
afterEach(() => {
    vi.clearAllMocks();
});

describe("SearchInput", () => {
    it("renders correctly with default props", () => {
        // Mock the hook to return empty results
        vi.mocked(useSearchApiModule.default).mockReturnValue({
            results: [],
            loading: false,
            error: null,
        });

        render(<SearchInput />);

        // Check if search input is rendered with default placeholder
        expect(screen.getByPlaceholderText("Search")).toBeInTheDocument();
    });

    it("shows loading state", () => {
        // Mock the hook to return loading state
        vi.mocked(useSearchApiModule.default).mockReturnValue({
            results: [],
            loading: true,
            error: null,
        });

        render(<SearchInput />);

        // Check if spinner is visible - using querySelector since the spinner has aria-hidden="true"
        const spinner = document.querySelector(".spinner-border");
        expect(spinner).toBeInTheDocument();
    });

    it("shows results when typing", async () => {
        // Mock search results
        const mockResults = [
            { name: "Artist 1", key: "a-1234", type: "artist" },
            { name: "Artist 2", key: "a-5678", type: "artist" },
        ];

        // Mock the hook to return results
        vi.mocked(useSearchApiModule.default).mockReturnValue({
            results: mockResults,
            loading: false,
            error: null,
        });

        render(<SearchInput />);

        // Type in the search box
        const searchInput = screen.getByPlaceholderText("Search");
        await userEvent.type(searchInput, "artist");

        // Wait for results to be visible (using waitFor instead of act)
        await waitFor(() => {
            expect(screen.getByText("Artist 1")).toBeInTheDocument();
            expect(screen.getByText("Artist 2")).toBeInTheDocument();
        });
    });

    it("dispatches network event when selecting a result", async () => {
        // Mock search results
        const mockResults = [
            { name: "Artist 1", key: "a-1234", type: "artist" },
        ];

        // Mock the hook to return results
        vi.mocked(useSearchApiModule.default).mockReturnValue({
            results: mockResults,
            loading: false,
            error: null,
        });

        render(<SearchInput />);

        // Type in the search box to trigger results
        const searchInput = screen.getByPlaceholderText("Search");
        await userEvent.type(searchInput, "artist");

        // Wait for the results to be visible
        await waitFor(() => {
            expect(screen.getByText("Artist 1")).toBeInTheDocument();
        });

        // Click on a result
        await userEvent.click(screen.getByText("Artist 1"));

        // Verify the event was dispatched
        expect(mockDispatchEvent).toHaveBeenCalled();

        // Verify input value was updated
        expect(searchInput).toHaveValue("Artist 1");
    });

    it("allows clearing the input", async () => {
        // Mock the hook
        vi.mocked(useSearchApiModule.default).mockReturnValue({
            results: [],
            loading: false,
            error: null,
        });

        render(<SearchInput />);

        // Type in the search box
        const searchInput = screen.getByPlaceholderText("Search");
        await userEvent.type(searchInput, "artist");

        // Wait for input to have the value
        await waitFor(() => {
            expect(searchInput).toHaveValue("artist");
        });

        // Click the clear button
        const clearButton = screen.getByRole("button", {
            name: /clear search/i,
        });
        await userEvent.click(clearButton);

        // Verify input was cleared
        await waitFor(() => {
            expect(searchInput).toHaveValue("");
        });
    });
});
