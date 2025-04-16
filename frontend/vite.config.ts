import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
    root: path.join(__dirname, "./source/"),
    base: "/assets/",
    resolve: {
        alias: {
            "@": path.resolve(__dirname, "./source"),
            "~bootstrap": path.resolve(__dirname, "./node_modules/bootstrap"),
            //      "bootstrap-icons/": path.resolve(__dirname, "./node_modules/bootstrap-icons/font"),
        },
    },
    build: {
        target: "es2020",
        assetsDir: "assets",
        sourcemap: true,
        outDir: path.join(__dirname, "./dist/"),
        manifest: "manifest.json",
        rollupOptions: {
            input: "source/index.ts",
        },
        emptyOutDir: true,
        copyPublicDir: false,
    },
    test: {
        environment: "jsdom",
        setupFiles: [],
        globals: true,
        root: __dirname,
        include: [
            // Unit tests in __tests__ directories
            "source/**/__tests__/**/*.{test,spec}.{js,jsx,ts,tsx}",
            // Integration and e2e tests
            "tests/**/*.{test,spec}.{js,jsx,ts,tsx}",
        ],
        coverage: {
            provider: "istanbul",
            exclude: ["**/js/vendor/**", "**/frontend/node_modules/**"],
        },
    },
    css: {
        preprocessorOptions: {
            scss: {
                api: "modern-compiler", // or "modern"
                silenceDeprecations: [
                    "mixed-decls",
                    "color-functions",
                    "global-builtin",
                    "import",
                ],
            },
        },
    },
    optimizeDeps: {
        include: ["jquery", "corejs-typeahead"],
    },
});
