import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  root: path.join(__dirname, "./source/"),
  base: "/assets/",
  //  resolve: {
  //    alias: {
  //      "bootstrap-icons/": path.resolve(__dirname, "./node_modules/bootstrap-icons/font"),
  //    },
  //  },
  build: {
    outDir: path.join(__dirname, "./dist/"),
    manifest: "manifest.json",
    assetsDir: "bundled",
    rollupOptions: {
      input: [
        "source/color.ts",
        "source/loading.ts",
        "source/network/events.ts",
        "source/network/forceLayout.ts",
        "source/network/halo.ts",
        "source/network/hull.ts",
        "source/network/init.ts",
        "source/network/link.ts",
        "source/network/node.ts",
        "source/network/text.ts",
        "source/network/tick.ts",
        "source/network/tooltips.ts",
        "source/svg.ts",
        "source/relations.ts",
        "source/roles.ts",
        "source/typeahead.ts",
        "source/fsm.ts",
        "source/init.ts",
        "source/index.ts",
      ],
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
