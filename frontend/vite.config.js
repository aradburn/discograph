import path from "node:path";
import { defineConfig } from "vite";

export default defineConfig({
  root: path.join(__dirname, "./source/"),
  base: "/assets/",
//  resolve: {
//    alias: {
//      "bootstrap-icons/": path.resolve(__dirname, "./node_modules/bootstrap-icons/font"),
//    },
//  },
  build: {
    outDir: path.join(__dirname, "./source_compiled/"),
    manifest: "manifest.json",
    assetsDir: "bundled",
    rollupOptions: {
      input: [
        "source/js/color.js",
        "source/js/loading.js",
        "source/js/network/events.js",
        "source/js/network/forceLayout.js",
        "source/js/network/halo.js",
        "source/js/network/hull.js",
        "source/js/network/init.js",
        "source/js/network/link.js",
        "source/js/network/node.js",
        "source/js/network/text.js",
        "source/js/network/tick.js",
        "source/js/svg.js",
        "source/js/relations.js",
        "source/js/roles.js",
        "source/js/typeahead.js",
        "source/js/fsm.js",
        "source/js/init.js",
        "source/js/index.js",
      ],
    },
    emptyOutDir: true,
    copyPublicDir: false,
  },
  css: {
      preprocessorOptions: {
          scss: {
              api: 'modern-compiler', // or "modern"
              silenceDeprecations: ['mixed-decls', 'color-functions', 'global-builtin', 'import']
          }
      }
  },
  optimizeDeps: {
    include: ["jquery", "corejs-typeahead"],
  },

});