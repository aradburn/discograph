import { defineConfig } from "vitest/config";
import path from "path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
    test: {
        environment: "jsdom",
        setupFiles: [],
        globals: true,
        include: [
            // Unit tests in __tests__ directories
            "source/**/__tests__/**/*.{test,spec}.{js,jsx,ts,tsx}",
            // Integration and e2e tests
            "tests/**/*.{test,spec}.{js,jsx,ts,tsx}",
        ],
    },
    resolve: {
        alias: {
            "@": path.resolve(__dirname, "./source"),
            "~bootstrap": path.resolve(__dirname, "./node_modules/bootstrap"),
        },
    },
});
