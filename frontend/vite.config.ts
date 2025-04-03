import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import { resolve } from "path";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
    root: path.join(__dirname, "./source/"),
    base: "/assets/",
    resolve: {
        alias: {
            "@": resolve(__dirname, "./source"),
            "~bootstrap": path.resolve(__dirname, "./node_modules/bootstrap"),
        },
    },
    //  resolve: {
    //    alias: {
    //      "bootstrap-icons/": path.resolve(__dirname, "./node_modules/bootstrap-icons/font"),
    //    },
    //  },
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
